#!/usr/bin/env bash
# 票 02 验收脚本：快照生成与跳过式恢复
# 覆盖 .scratch/config-center-mvp/issues/02-snapshot-skip-restore.md 全部验收项。
# 注：raft 在 bootstrap 与每次选举时各追加一条 noop 日志，快照 index 与业务日志数
# 不相等属正常；断言采用相对值（响应 index == 头 raft_index）。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e02.log

fail=0
check() {
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}

put() { # $1=data_id $2=content(默认 v1)；等到 200（选举完成前会 503）
  content="${2:-v1}"
  for _ in $(seq 1 100); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
      "$HTTP/v1/namespaces/dev/configs/$1" \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
      -d "{\"content\":\"$content\",\"data_type\":\"TEXT\"}")
    [ "$code" = "200" ] && return 0
    sleep 0.1
  done
  return 1
}

wait_healthy() {
  for _ in $(seq 1 120); do
    curl -sf "$HTTP/healthz" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  return 1
}

# 等待快照点之后的日志经选举完成后的正常提交流程重放（DESIGN §4.4"重放快照点之后的少量日志"）
wait_entry() { # $1=data_id
  for _ in $(seq 1 100); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$HTTP/v1/namespaces/dev/configs/$1")
    [ "$code" = "200" ] && return 0
    sleep 0.1
  done
  return 1
}

echo "=== 准备：清空 data 目录与 PG 表，bootstrap 单节点 ==="
rm -rf data
$PG "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE" >/dev/null
"$BIN" --config config.yaml --bootstrap >"$LOG" 2>&1 &
SRV=$!
wait_healthy; check $? "单节点 bootstrap 启动"

echo "=== 写入 4 条日志：a×2 版本、b、c ==="
put a v1 && put a v2 && put b v1 && put c v1
check $? "4 次 Publish 成功"

echo "=== 验收 1：手动触发快照，文件含合法头与三表数据 ==="
snap_resp=$(curl -s -X POST "$HTTP/v1/cluster/snapshot" -H "Authorization: Bearer $TOKEN")
SNAP_IDX=$(echo "$snap_resp" | grep -oE '"snapshot_index":[0-9]+' | grep -oE '[0-9]+')
[ -n "$SNAP_IDX" ]; check $? "触发快照返回 200 且 snapshot_index=$SNAP_IDX"

SNAP_FILE=$(ls -t data/raft/snapshots/*/state.bin 2>/dev/null | head -1)
[ -n "$SNAP_FILE" ]; check $? "快照文件已落盘（$SNAP_FILE）"

header=$(tail -c +9 "$SNAP_FILE" | head -1)
echo "$header" | grep -q '"format_version":1';  check $? "头含 format_version=1"
echo "$header" | grep -q "\"raft_index\":$SNAP_IDX"; check $? "头 raft_index=$SNAP_IDX 与响应一致"
echo "$header" | grep -qE '"biz_configs":\{"rows":3';  check $? "头含 biz_configs 行数 3"
echo "$header" | grep -qE '"biz_config_history":\{"rows":4'; check $? "头含 history 行数 4"
echo "$header" | grep -q '"checksum":"[0-9a-f]\{64\}"'
check $? "头含 64 位 hex checksum"

# 手动校验 checksum 与数据体
hdr_len=$(echo "$header" | wc -c)
body_sum=$(tail -c +"$((9 + hdr_len))" "$SNAP_FILE" | sha256sum | cut -d' ' -f1)
hdr_sum=$(echo "$header" | grep -oE '"checksum":"[0-9a-f]+"' | cut -d'"' -f4)
[ "$body_sum" = "$hdr_sum" ]; check $? "数据体 sha256 与头 checksum 一致"

echo "=== 快照点之后再写 1 条，用于重启后重放验证 ==="
put d; check $? "快照点后 Publish d 成功"

echo "=== 验收 2：带数据重启命中跳过路径，HTTP 就绪秒级 ==="
kill -9 $SRV 2>/dev/null; sleep 0.5
start=$(date +%s%N)
"$BIN" --config config.yaml >>"$LOG" 2>&1 &
SRV=$!
wait_healthy
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
[ "$ms" -le 3000 ]; check $? "重启到 HTTP 可服务 ${ms}ms ≤3s"
grep -q "跳过数据导入" "$LOG"; check $? "日志命中跳过路径"
curl -s "$HTTP/metrics" | grep -q 'restore_skipped_total [1-9]'
check $? "指标 restore_skipped_total ≥ 1"
code=$(curl -s -o /tmp/gcc02-get.json -w '%{http_code}' "$HTTP/v1/namespaces/dev/configs/a")
[ "$code" = "200" ] && grep -q '"version":2' /tmp/gcc02-get.json
check $? "重启后 GET a 返回 v2（跳过导入未丢数据）"
hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='dev'")
[ "$hist" = "5" ]; check $? "重启后 history = 5 条（含 d）"

echo "=== 验收 3：清空 PG 后重启走全量导入，数据完整恢复 ==="
kill -9 $SRV 2>/dev/null; sleep 0.5
$PG "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE" >/dev/null
"$BIN" --config config.yaml >>"$LOG" 2>&1 &
SRV=$!
wait_healthy; check $? "清空 PG 后重启成功（Migrate 重建 + Restore 全量导入）"
grep -q "全量导入完成" "$LOG"; check $? "日志命中全量导入路径"
wait_entry d; check $? "全量恢复后快照点之后的 d 也经重放回归（≤10s）"
ok=1
for id in a b c; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$HTTP/v1/namespaces/dev/configs/$id")
  [ "$code" = "200" ] || ok=0
done
[ "$ok" = "1" ]; check $? "全量恢复后 a/b/c/d 全部可读"
body=$(curl -s "$HTTP/v1/namespaces/dev/configs/a")
echo "$body" | grep -q '"version":2'; check $? "恢复后 a 仍为 v2"
meta_idx=$($PG "SELECT last_applied_index FROM raft_fsm_meta")
[ "$meta_idx" -ge "$SNAP_IDX" ] 2>/dev/null; check $? "恢复+重放后 last_applied_index=$meta_idx ≥ 快照点 $SNAP_IDX"
put e; check $? "恢复后继续 Publish e（导入序列已修正）"

echo "=== 验收 4：checksum 损坏时启动显式失败，PG 无半写 ==="
kill -9 $SRV 2>/dev/null; sleep 0.5
cp "$SNAP_FILE" "$SNAP_FILE.bak"
printf 'X' | dd of="$SNAP_FILE" bs=1 seek=$(( $(stat -c %s "$SNAP_FILE") - 1 )) conv=notrunc 2>/dev/null
"$BIN" --config config.yaml >/tmp/gcc02-corrupt.log 2>&1
rc=$?
[ "$rc" -ne 0 ]; check $? "快照损坏时进程非零退出（rc=$rc）"
grep -qE "checksum 不符|failed to restore|failed to load any existing snapshots|CRC mismatch" /tmp/gcc02-corrupt.log
check $? "错误信息指出恢复失败（$(grep -oE 'checksum 不符|failed to restore.*|failed to load any existing snapshots|CRC mismatch' /tmp/gcc02-corrupt.log | head -1 | head -c 100)）"
hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='dev'")
[ "$hist" = "6" ]; check $? "失败后 PG 数据原样（history=$hist，无半写/清空）"
mv "$SNAP_FILE.bak" "$SNAP_FILE"

echo "=== 验收 5：还原快照后服务正常可写可读 ==="
"$BIN" --config config.yaml >>"$LOG" 2>&1 &
SRV=$!
wait_healthy; check $? "还原快照后重启成功"
wait_entry e; check $? "重放完成后 e 可读"
put f; check $? "重启后继续可写"
curl -sf "$HTTP/readyz" | grep -q '"status":"ready"'; check $? "/readyz ready"
curl -s "$HTTP/metrics" | grep -q 'snapshot_duration_seconds'; check $? "指标 snapshot_duration_seconds 暴露"

kill -9 $SRV 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
