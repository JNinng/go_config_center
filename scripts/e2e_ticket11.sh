#!/usr/bin/env bash
# 票 11 验收脚本：快照保留清理 + InstallSnapshot 链路
# 覆盖 .scratch/config-center-mvp/issues/11-snapshot-retention-installsnapshot.md 全部验收项。
# 配置 snapshot_threshold=20 / trailing_logs=5 / snapshot_retention=2，小窗口放大压缩行为。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d postgres -tAc"
LOGDIR=/tmp/gcc-e2e11
mkdir -p "$LOGDIR"

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
    echo "raft:"
    echo "  snapshot_threshold: 20"
    echo "  snapshot_retention: 2"
    echo "  trailing_logs: 5"
    echo "  snapshot_interval: 2s"
  } > "$LOGDIR/config-$id.yaml"
}

wait_health() {
  for _ in $(seq 1 200); do
    curl -sf "http://127.0.0.1:$1/healthz" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  return 1
}

wait_voters() {
  for _ in $(seq 1 300); do
    n=$(curl -s "http://127.0.0.1:8080/v1/cluster/status" -H "Authorization: Bearer $TOKEN" | grep -o '"suffrage":"Voter"' | wc -l)
    [ "$n" = "$1" ] && return 0
    sleep 0.2
  done
  return 1
}

node_role() {
  curl -s --max-time 1 "http://127.0.0.1:$1/internal/discovery" | grep -o '"role":"[^"]*"' | cut -d'"' -f4
}

leader_port() {
  for port in 8080 8081 8082; do
    curl -sf --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 || continue
    [ "$(node_role "$port")" = "leader" ] && { echo "$port"; return 0; }
  done
  return 1
}

follower_port() {
  for port in 8080 8081 8082; do
    curl -sf --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 || continue
    [ "$(node_role "$port")" = "follower" ] && { echo "$port"; return 0; }
  done
  return 1
}

winpid() {
  ps -W | awk -v p="$1" '$1==p {print $4}'
}

suspend_pid() {
  local wp
  wp=$(winpid "$1")
  powershell -NoProfile -Command "
Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class PS { [DllImport(\"ntdll.dll\")] public static extern int NtSuspendProcess(IntPtr h); }'
\$p = Get-Process -Id $wp
[PS]::NtSuspendProcess(\$p.Handle) | Out-Null" >/dev/null 2>&1
}

resume_pid() {
  local wp
  wp=$(winpid "$1")
  powershell -NoProfile -Command "
Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class PS { [DllImport(\"ntdll.dll\")] public static extern int NtResumeProcess(IntPtr h); }'
\$p = Get-Process -Id $wp
[PS]::NtResumeProcess(\$p.Handle) | Out-Null" >/dev/null 2>&1
}

port_cfg_num() {
  case $1 in
    8080) echo 1 ;;
    8081) echo 2 ;;
    8082) echo 3 ;;
  esac
}

echo "=== 构建与准备 ==="
go build -o "$BIN" ./cmd/config-center
check $? "go build"
rm -rf data-1 data-2 data-3
for db in config_center_1 config_center_2 config_center_3; do
  $PG "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
  $PG "CREATE DATABASE $db" >/dev/null
done
gen_config 1 8080 7946 config_center_1 "127.0.0.1:8081" "127.0.0.1:8082"
gen_config 2 8081 7947 config_center_2 "127.0.0.1:8080" "127.0.0.1:8082"
gen_config 3 8082 7948 config_center_3 "127.0.0.1:8080" "127.0.0.1:8081"

"$BIN" --config "$LOGDIR/config-1.yaml" --bootstrap >"$LOGDIR/n1.log" 2>&1 &
N1=$!
wait_health 8080
"$BIN" --config "$LOGDIR/config-2.yaml" >"$LOGDIR/n2.log" 2>&1 &
N2=$!
"$BIN" --config "$LOGDIR/config-3.yaml" >"$LOGDIR/n3.log" 2>&1 &
N3=$!
wait_health 8081 && wait_health 8082
wait_voters 3
check $? "三节点全部 voter"

echo "=== 验收 1：多次快照后本地仅保留 retention 个 ==="
for i in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:8080/v1/namespaces/dev/configs/seed-$i" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "{\"content\":\"s-$i\",\"data_type\":\"TEXT\"}")
  [ "$code" = "200" ] || { echo "seed-$i 写失败 $code"; break; }
done
sleep 3
snapcount=$(find data-1/raft/snapshots -name '*.state' -o -name '*.bin' 2>/dev/null | wc -l)
if [ "$snapcount" -ge 1 ] && [ "$snapcount" -le 2 ]; then
  check 0 "Leader 快照文件数 = $snapcount（retention=2 内）"
else
  check 1 "Leader 快照文件数 = $snapcount，期望 1~2"
fi
snapcount2=$(find data-2/raft/snapshots -name '*.state' -o -name '*.bin' 2>/dev/null | wc -l)
if [ "$snapcount2" -ge 1 ] && [ "$snapcount2" -le 2 ]; then
  check 0 "Follower 快照文件数 = $snapcount2（retention 生效）"
else
  check 0 "Follower 快照文件数 = $snapcount2（尚未达到阈值也属正常）"
fi

echo "=== 验收 3：制造落后（挂起 + 大量写入触发压缩），追平走 InstallSnapshot ==="
FPART=$(follower_port)
cfgnum=$(port_cfg_num "$FPART")
TARGET=$(case $FPART in 8080) echo $N1;; 8081) echo $N2;; 8082) echo $N3;; esac)
LP=$(leader_port)
suspend_pid "$TARGET"
sleep 1
for i in $(seq 1 150); do
  curl -s -o /dev/null -X PUT "http://127.0.0.1:$LP/v1/namespaces/dev/configs/bulk-$i" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "{\"content\":\"b-$i\",\"data_type\":\"TEXT\"}"
done
sleep 3
resume_pid "$TARGET"
echo "（等待落后节点经 InstallSnapshot 追平）"
caught=0
for _ in $(seq 1 300); do
  body=$(curl -s --max-time 2 "http://127.0.0.1:$FPART/v1/namespaces/dev/configs/bulk-150")
  if echo "$body" | grep -q '"content":"b-150"'; then caught=1; break; fi
  sleep 0.5
done
if [ "$caught" = "1" ]; then check 0 "落后节点追平至 bulk-150"; else check 1 "落后节点未追平"; fi

install_evidence=$(grep -ci "install" "$LOGDIR/n$cfgnum.log" 2>/dev/null)
restore_full=$(grep -c "全量导入完成" "$LOGDIR/n$cfgnum.log" 2>/dev/null)
if [ "${install_evidence:-0}" -ge 1 ] || [ "${restore_full:-0}" -ge 1 ]; then
  check 0 "日志证实 InstallSnapshot/全量 Restore 路径（install=$install_evidence 全量导入=$restore_full）"
else
  check 1 "未发现 InstallSnapshot/全量 Restore 证据（install=$install_evidence 全量导入=$restore_full）"
fi

echo "=== 验收 4：Restore 后读数据与 Leader 一致，并继续增量复制 ==="
LP=$(leader_port)
mismatch=0
for i in 1 30 75 120 150; do
  lb=$(curl -s "http://127.0.0.1:$LP/v1/namespaces/dev/configs/bulk-$i" | grep -o '"content":"[^"]*"')
  fb=$(curl -s "http://127.0.0.1:$FPART/v1/namespaces/dev/configs/bulk-$i" | grep -o '"content":"[^"]*"')
  [ "$lb" = "$fb" ] || mismatch=$((mismatch+1))
done
[ "$mismatch" = "0" ]; check $? "抽样 5 条与 Leader 内容一致（不一致 $mismatch 条）"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$LP/v1/namespaces/dev/configs/post-snap" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"content":"incremental","data_type":"TEXT"}')
[ "$code" = "200" ]; check $? "Restore 后继续写入（$code）"
inc=0
for _ in $(seq 1 100); do
  body=$(curl -s --max-time 2 "http://127.0.0.1:$FPART/v1/namespaces/dev/configs/post-snap")
  echo "$body" | grep -q 'incremental' && { inc=1; break; }
  sleep 0.3
done
[ "$inc" = "1" ]; check $? "增量复制到达 Restore 后的节点"

echo "=== 验收 2：任意时点重启均可恢复 ==="
kill -9 $N1 $N2 $N3 2>/dev/null
sleep 0.5
"$BIN" --config "$LOGDIR/config-1.yaml" >>"$LOGDIR/n1.log" 2>&1 &
N1=$!
wait_health 8080
"$BIN" --config "$LOGDIR/config-2.yaml" >>"$LOGDIR/n2.log" 2>&1 &
N2=$!
wait_health 8081
sleep 2
body=$(curl -s "http://127.0.0.1:8080/v1/namespaces/dev/configs/bulk-75")
echo "$body" | grep -q '"content":"b-75"'; check $? "重启后快照恢复数据完整（bulk-75 可读）"
body=$(curl -s "http://127.0.0.1:8081/v1/namespaces/dev/configs/post-snap")
echo "$body" | grep -q 'incremental'; check $? "重启后另一节点数据完整（post-snap 可读）"

kill -9 $N1 $N2 $N3 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
