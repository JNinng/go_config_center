#!/usr/bin/env bash
# 票 12 验收脚本：10 万条规模化验证（DESIGN §1.1、§8.3、ADR-0007 量级承诺）
# 10 万条 Config Entry（含 History）下：快照生成时长/体积记录、冷重启 ≤10s 断言、
# 空节点经 InstallSnapshot 追平且读一致。退出码 0=全部通过。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d postgres -tAc"
LOGDIR=/tmp/gcc-e2e12
mkdir -p "$LOGDIR"
TOTAL=100000
WORKERS=20
PER=$((TOTAL / WORKERS))

fail=0
check() {
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}

gen_config() {
  local id=$1 http=$2 raft=$3 db=$4
  shift 4
  {
    echo "http_addr: \"127.0.0.1:$http\""
    echo "raft_addr: \"127.0.0.1:$raft\""
    echo "data_dir: \"./data-$id\""
    echo "admin_token: \"$TOKEN\""
    echo "pg_dsn: \"host=127.0.0.1 port=5432 user=postgresql dbname=$db sslmode=disable\""
    echo "http_advertise: \"http://127.0.0.1:$http\""
    echo "seeds:"
    for s in "$@"; do echo "  - \"$s\""; done
  } > "$LOGDIR/config-$id.yaml"
}

now_ms() { date +%s%N | cut -c1-13; }

echo "=== 构建与准备 ==="
go build -o "$BIN" ./cmd/config-center
check $? "go build"
rm -rf data-1 data-2
$PG "DROP DATABASE IF EXISTS config_center_1 WITH (FORCE)" >/dev/null
$PG "DROP DATABASE IF EXISTS config_center_2 WITH (FORCE)" >/dev/null
$PG "CREATE DATABASE config_center_1" >/dev/null
$PG "CREATE DATABASE config_center_2" >/dev/null
gen_config 1 8080 7946 config_center_1
gen_config 2 8081 7947 config_center_2 "127.0.0.1:8080"

echo "=== 灌入 $TOTAL 条（$WORKERS 并发 worker × $PER 条） ==="
"$BIN" --config "$LOGDIR/config-1.yaml" --bootstrap >"$LOGDIR/n1.log" 2>&1 &
N1=$!
for _ in $(seq 1 200); do
  curl -sf "http://127.0.0.1:8080/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
for _ in $(seq 1 300); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:8080/v1/namespaces/dev/configs/warmup" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"content":"w","data_type":"TEXT"}')
  [ "$code" = "200" ] && break
  sleep 0.2
done
[ "$code" = "200" ]; check $? "服务就绪"

worker() { # $1=起始序号 $2=条数
  local i code
  for i in $(seq "$1" $(( $1 + $2 - 1 ))); do
    code=$(curl -s -o /dev/null --max-time 10 -w '%{http_code}' -X PUT "http://127.0.0.1:8080/v1/namespaces/scale/configs/load-$i" \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "{\"content\":\"payload-$i\",\"data_type\":\"TEXT\"}")
    if [ "$code" != "200" ]; then echo "load-$i $code" >> "$LOGDIR/loadfail.txt"; fi
  done
}
START=$(now_ms)
w=0
WPIDS=""
while [ $w -lt $WORKERS ]; do
  worker $((w * PER + 1)) $PER &
  WPIDS="$WPIDS $!"
  w=$((w + 1))
done
for wp in $WPIDS; do wait "$wp"; done
END=$(now_ms)
echo "（灌数耗时 $((END - START))ms）"
fails=0
if [ -f "$LOGDIR/loadfail.txt" ]; then
  fails=$(wc -l < "$LOGDIR/loadfail.txt")
fi
[ "$fails" = "0" ]; check $? "全部写入成功（失败 $fails 条）"

# warmup 键占 1 条
expect=$((TOTAL + 1))
P1="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
count=$($P1 "SELECT count(*) FROM biz_configs")
hist=$($P1 "SELECT count(*) FROM biz_config_history")
[ "$count" = "$expect" ]; check $? "biz_configs = $expect 条（实际 $count）"
[ "$hist" = "$expect" ]; check $? "biz_config_history = $expect 条（实际 $hist）"

echo "=== 验收 1：快照生成成功，时长与体积记录 ==="
START=$(now_ms)
snap=$(curl -s -X POST "http://127.0.0.1:8080/v1/cluster/snapshot" -H "Authorization: Bearer $TOKEN")
END=$(now_ms)
snap_ms=$((END - START))
echo "$snap" | grep -q '"status":"ok"'; check $? "手动快照成功（${snap_ms}ms，$snap）"
snapfile=$(find data-1/raft/snapshots -name '*.state' -o -name '*.bin' 2>/dev/null | xargs -I{} stat -c '%s %n' {} 2>/dev/null | sort -rn | head -1)
snapsize=$(echo "$snapfile" | cut -d' ' -f1)
echo "SNAPSHOT_RECORD: duration_ms=$snap_ms size_bytes=$snapsize size_mb=$((snapsize / 1024 / 1024))MB entries=$TOTAL"

echo "=== 验收 2：冷重启 ≤10s（10 万条数据） ==="
kill -9 $N1 2>/dev/null
sleep 0.5
START=$(now_ms)
"$BIN" --config "$LOGDIR/config-1.yaml" >>"$LOGDIR/n1.log" 2>&1 &
N1=$!
cold_ok=0
for _ in $(seq 1 300); do
  if curl -sf --max-time 1 "http://127.0.0.1:8080/healthz" >/dev/null 2>&1; then
    body=$(curl -s --max-time 2 "http://127.0.0.1:8080/v1/namespaces/scale/configs/load-50000")
    echo "$body" | grep -q 'payload-50000' && { cold_ok=1; break; }
  fi
  sleep 0.1
done
END=$(now_ms)
cold_ms=$((END - START))
if [ "$cold_ok" = "1" ] && [ "$cold_ms" -le 10000 ]; then
  check 0 "冷重启到可读 $cold_ms ms ≤ 10s"
else
  check 1 "冷重启 $cold_ms ms，ok=$cold_ok（阈值 10s）"
fi

echo "=== 验收 3：空节点规模下经 InstallSnapshot 追平，读一致 ==="
"$BIN" --config "$LOGDIR/config-2.yaml" >"$LOGDIR/n2.log" 2>&1 &
N2=$!
for _ in $(seq 1 200); do
  curl -sf "http://127.0.0.1:8081/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
joined=0
for _ in $(seq 1 600); do
  c2=$(psql -h 127.0.0.1 -U postgresql -d config_center_2 -tAc "SELECT count(*) FROM biz_configs" 2>/dev/null)
  [ "$c2" = "$expect" ] && { joined=1; break; }
  sleep 0.5
done
if [ "$joined" = "1" ]; then check 0 "新节点 PG 物化 $expect 条"; else check 1 "新节点物化 $c2/$expect"; fi
install_evidence=$(grep -ci "install" "$LOGDIR/n2.log" 2>/dev/null)
restore_full=$(grep -c "全量导入完成" "$LOGDIR/n2.log" 2>/dev/null)
echo "（新节点日志：install=$install_evidence 全量导入=$restore_full）"
mismatch=0
for i in 1 25000 50000 75000 100000; do
  lb=$(curl -s "http://127.0.0.1:8080/v1/namespaces/scale/configs/load-$i" | grep -o '"content":"[^"]*"')
  fb=$(curl -s "http://127.0.0.1:8081/v1/namespaces/scale/configs/load-$i" | grep -o '"content":"[^"]*"')
  [ "$lb" = "$fb" ] || mismatch=$((mismatch + 1))
done
[ "$mismatch" = "0" ]; check $? "抽样 5 条两节点读一致（不一致 $mismatch 条）"

kill -9 $N1 $N2 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
