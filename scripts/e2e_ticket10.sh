#!/usr/bin/env bash
# 票 10 验收脚本：混沌场景套件（DESIGN §8.2）
# 场景 A：写压测中杀 Leader——新 Leader 秒级产生、已提交写不丢、未提交写明确失败、
#         客户端仅凭 307/重试语义恢复。
# 场景 B：Follower 分区（进程挂起模拟）——期间其余节点读写正常，恢复后自动追平。
# 一键可重复执行；退出码 0=全部通过。
# 注：单机无网络分区手段，分区以进程挂起等价模拟（节点不响应任何请求）；
#     期间读旧数据不可观测，以恢复后追平断言替代（见票 10 验收记录）。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d postgres -tAc"
LOGDIR=/tmp/gcc-e2e10
mkdir -p "$LOGDIR"
RESULTS="$LOGDIR/writes.log"
FAILCODES="$LOGDIR/failcodes.txt"
: > "$RESULTS"
: > "$FAILCODES"

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

alive_port() {
  for port in 8080 8081 8082; do
    curl -sf --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && { echo "$port"; return 0; }
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

echo "=== 场景 A：写压测中杀 Leader ==="
FPORT=$(follower_port)
writer() {
  local port=$1 start=$2 count=$3
  local i code
  for i in $(seq "$start" $((start + count - 1))); do
    code=$(curl -s -L --max-time 3 -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$port/v1/namespaces/dev/configs/chaos-$i" \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
      -d "{\"content\":\"c-$i\",\"data_type\":\"TEXT\"}")
    echo "chaos-$i $code" >> "$RESULTS"
    if [ "$code" != "200" ]; then echo "$code" >> "$FAILCODES"; fi
    sleep 0.05
  done
}
writer "$FPORT" 1 100 & W1=$!
writer "$FPORT" 101 100 & W2=$!
sleep 3

LP=$(leader_port)
case $LP in
  8080) kill -9 $N1 ;;
  8081) kill -9 $N2 ;;
  8082) kill -9 $N3 ;;
esac
KILL_AT=$(date +%s)
check $? "压测进行中杀掉 Leader（端口 $LP）"

NEWLP=""
for _ in $(seq 1 40); do
  NEWLP=$(leader_port)
  if [ -n "$NEWLP" ] && [ "$NEWLP" != "$LP" ]; then break; fi
  sleep 0.2
done
NOW=$(date +%s)
echo "（选举耗时约 $((NOW - KILL_AT))s，新 Leader 端口 $NEWLP）"
if [ -n "$NEWLP" ] && [ "$NEWLP" != "$LP" ]; then check 0 "新 Leader 秒级产生"; else check 1 "新 Leader 秒级产生"; fi
wait $W1 $W2 2>/dev/null

echo "=== 验收 1：已提交写全部可读；未提交写明确失败 ==="
committed=0
lost=0
while read -r id code; do
  if [ "$code" = "200" ]; then
    committed=$((committed + 1))
    body=$(curl -s --max-time 2 "http://127.0.0.1:$NEWLP/v1/namespaces/dev/configs/$id")
    echo "$body" | grep -q '"content":"' || lost=$((lost + 1))
  fi
done < "$RESULTS"
if [ "$committed" -gt 50 ]; then check 0 "已提交写 $committed 条"; else check 1 "已提交写仅 $committed 条"; fi
if [ "$lost" = "0" ]; then check 0 "已提交写在新 Leader 上零丢失"; else check 1 "已提交写丢失 $lost 条"; fi
if [ -s "$FAILCODES" ]; then
  badcodes=$(sort -u "$FAILCODES" | grep -vc -e '^503$' -e '^000$' -e '^307$')
  if [ "$badcodes" = "0" ]; then
    check 0 "失败写均为 503/超时/307 未送达（$(sort -u "$FAILCODES" | tr '\n' ' ')）"
  else
    check 1 "存在未定义失败码（$(sort -u "$FAILCODES" | tr '\n' ' ')）"
  fi
else
  check 1 "压测期间应存在失败写以验证拒绝语义"
fi

echo "=== 验收 2：选举期 503，选举后仅凭重试/307 恢复 ==="
codes=$(grep -c '^503$' "$FAILCODES" 2>/dev/null || echo 0)
if [ "$codes" -gt 0 ]; then check 0 "选举窗口观察到 503（$codes 次）"; else check 1 "未观察到 503"; fi
recovered=1
for i in 901 902 903 904 905; do
  code=$(curl -s -L --max-time 3 -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$FPORT/v1/namespaces/dev/configs/chaos-$i" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "{\"content\":\"c-$i\",\"data_type\":\"TEXT\"}")
  if [ "$code" != "200" ]; then recovered=0; fi
done
if [ "$recovered" = "1" ]; then check 0 "恢复后全新写入 5/5 成功（仅凭重试与 307）"; else check 1 "恢复后写入未恢复"; fi

echo "=== 场景 B：Follower 分区（进程挂起），恢复后自动追平 ==="
cfgnum=$(port_cfg_num "$LP")
"$BIN" --config "$LOGDIR/config-$cfgnum.yaml" >>"$LOGDIR/n$cfgnum.log" 2>&1 &
NP=$!
wait_voters 3
check $? "原 Leader 重启归队，恢复 3 voter"

pid_for_port() {
  case $1 in
    8080) if kill -0 $N1 2>/dev/null; then echo $N1; else echo $NP; fi ;;
    8081) echo $N2 ;;
    8082) echo $N3 ;;
  esac
}
FPART=$(follower_port)
TARGET=$(pid_for_port "$FPART")
suspend_pid "$TARGET"
sleep 1
if curl -sf --max-time 1 "http://127.0.0.1:$FPART/healthz" >/dev/null 2>&1; then
  check 1 "分区生效：Follower(端口 $FPART) 仍响应"
else
  check 0 "分区生效：Follower(端口 $FPART) 不响应"
fi
ALIVE=$(alive_port)
part_ok=0
for i in 1 2 3 4 5; do
  code=$(curl -s -L --max-time 3 -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$ALIVE/v1/namespaces/dev/configs/partition-$i" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "{\"content\":\"during-partition-$i\",\"data_type\":\"TEXT\"}")
  if [ "$code" = "200" ]; then part_ok=$((part_ok + 1)); fi
  sleep 1
done
if [ "$part_ok" = "5" ]; then check 0 "分区期间其余节点写入 5/5（quorum 2/3 存活）"; else check 1 "分区期间写入 $part_ok/5"; fi
sleep 25
resume_pid "$TARGET"
caught=0
for _ in $(seq 1 150); do
  ok=1
  for i in 1 2 3 4 5; do
    body=$(curl -s --max-time 2 "http://127.0.0.1:$FPART/v1/namespaces/dev/configs/partition-$i")
    echo "$body" | grep -q "during-partition-$i" || { ok=0; break; }
  done
  if [ "$ok" = "1" ]; then caught=1; break; fi
  sleep 0.5
done
if [ "$caught" = "1" ]; then check 0 "恢复后 Follower 自动追平分区期间全部写入"; else check 1 "恢复后未追平"; fi

kill -9 $N1 $N2 $N3 $NP 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
