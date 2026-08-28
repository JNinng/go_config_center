#!/usr/bin/env bash
# 票 09 验收脚本：成员移除 + quorum 护栏 + 自退出 + cluster/status
# 覆盖 .scratch/config-center-mvp/issues/09-membership-remove-guard.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d postgres -tAc"
LOGDIR=/tmp/gcc-e2e09
mkdir -p "$LOGDIR"

fail=0
check() {
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}

gen_config() { # $1=编号 $2=http端口 $3=raft端口 $4=库名 $5...=seeds
  local id=$1 http=$2 raft=$3 db=$4; shift 4
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

wait_voters() { # $1=期望 voter 数
  for _ in $(seq 1 300); do
    n=$(curl -s "http://127.0.0.1:8080/v1/cluster/status" -H "Authorization: Bearer $TOKEN" \
      | grep -o '"suffrage":"Voter"' | wc -l)
    [ "$n" = "$1" ] && return 0
    sleep 0.2
  done
  return 1
}

leader_port() {
  for port in 8080 8081 8082; do
    curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 || continue
    role=$(curl -s "http://127.0.0.1:$port/internal/discovery" | grep -o '"role":"[^"]*"' | cut -d'"' -f4)
    [ "$role" = "leader" ] && { echo "$port"; return 0; }
  done
  return 1
}

node_id() { # $1=http端口
  curl -s "http://127.0.0.1:$1/internal/discovery" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4
}

echo "=== 构建与准备 ==="
go build -o "$BIN" ./cmd/config-center; check $? "go build"
rm -rf data-1 data-2 data-3
for db in config_center_1 config_center_2 config_center_3; do
  $PG "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
  $PG "CREATE DATABASE $db" >/dev/null
done
gen_config 1 8080 7946 config_center_1 "127.0.0.1:8081" "127.0.0.1:8082"
gen_config 2 8081 7947 config_center_2 "127.0.0.1:8080" "127.0.0.1:8082"
gen_config 3 8082 7948 config_center_3 "127.0.0.1:8080" "127.0.0.1:8081"

"$BIN" --config "$LOGDIR/config-1.yaml" --bootstrap >"$LOGDIR/n1.log" 2>&1 & N1=$!
wait_health 8080
"$BIN" --config "$LOGDIR/config-2.yaml" >"$LOGDIR/n2.log" 2>&1 & N2=$!
"$BIN" --config "$LOGDIR/config-3.yaml" >"$LOGDIR/n3.log" 2>&1 & N3=$!
wait_health 8081 && wait_health 8082
wait_voters 3; check $? "三节点全部 voter"

echo "=== 验收 4：cluster/status 含成员 node_id/addr/suffrage 与 last_applied_index ==="
status=$(curl -s "http://127.0.0.1:8080/v1/cluster/status" -H "Authorization: Bearer $TOKEN")
echo "$status" | grep -q '"last_applied_index"' && echo "$status" | grep -q '"suffrage":"Voter"' \
  && echo "$status" | grep -q '"addr":"127.0.0.1:794' && echo "$status" | grep -q '"node_id":"'
check $? "status 字段完整"
members=$(echo "$status" | grep -o '"suffrage":"' | wc -l)
[ "$members" = "3" ]; check $? "成员表 3 条（实际 $members）"

echo "=== 验收 5：对已是成员的 Node ID 重复 Join → 幂等 200 ==="
nid2=$(node_id 8081)
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X POST "http://127.0.0.1:8080/v1/cluster/join" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"node_id\":\"$nid2\",\"raft_addr\":\"127.0.0.1:7947\",\"http_addr\":\"http://127.0.0.1:8081\"}")
[ "$code" = "200" ] && grep -q '"status":"exists"' /tmp/gcc-resp.json
check $? "重复 Join → 200 exists（$code $(cat /tmp/gcc-resp.json)）"
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://127.0.0.1:8080/v1/cluster/peers/00000000-0000-0000-0000-000000000000" \
  -H "Authorization: Bearer $TOKEN")
[ "$code" = "404" ]; check $? "移除非成员 → 404（实际 $code）"

echo "=== 验收 1 + 3：三节点在线时移除 1 个 → 200，被移除节点以 42 退出，集群继续读写 ==="
nid3=$(node_id 8082)
LEADER=$(leader_port)
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X DELETE "http://127.0.0.1:$LEADER/v1/cluster/peers/$nid3" \
  -H "Authorization: Bearer $TOKEN")
[ "$code" = "200" ]; check $? "移除节点 3 → 200（$code）"
gone=0
for _ in $(seq 1 40); do
  kill -0 $N3 2>/dev/null || { gone=1; break; }
  sleep 0.25
done
[ "$gone" = "1" ]; check $? "被移除节点进程已退出"
if [ "$gone" = "1" ]; then
  wait $N3 2>/dev/null; rc=$?
  [ "$rc" = "42" ]; check $? "退出码 = 42（实际 $rc）"
else
  kill -9 $N3 2>/dev/null
  check 1 "退出码 = 42（进程未自行退出，已强杀）"
fi
wait_voters 2; check $? "成员表剩 2 voter"
for _ in $(seq 1 100); do
  code=$(curl -s -L -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT "http://127.0.0.1:8081/v1/namespaces/dev/configs/after.remove" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"content":"still-working","data_type":"TEXT"}')
  [ "$code" = "200" ] && break
  sleep 0.2
done
[ "$code" = "200" ]; check $? "二节点集群写入成功（$code）"
body=$(curl -s "http://127.0.0.1:8080/v1/namespaces/dev/configs/after.remove")
echo "$body" | grep -q 'still-working'; check $? "读路径正常"

echo "=== 重拉起节点 3（清空 data_dir 重新 Join，DESIGN §4.5 换 IP 流程） ==="
rm -rf data-3
$PG "DROP DATABASE IF EXISTS config_center_3 WITH (FORCE)" >/dev/null
$PG "CREATE DATABASE config_center_3" >/dev/null
"$BIN" --config "$LOGDIR/config-3.yaml" >"$LOGDIR/n3b.log" 2>&1 & N3=$!
wait_health 8082
wait_voters 3; check $? "节点 3 重新加入，恢复 3 voter"

echo "=== 验收 2：1 节点宕机时再移除 1 个在线节点 → 409 ==="
kill -9 $N3 2>/dev/null   # 新节点 3 宕机（仍是 voter）
sleep 0.5
nid2=$(node_id 8081)
LEADER=$(leader_port)
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X DELETE "http://127.0.0.1:$LEADER/v1/cluster/peers/$nid2" \
  -H "Authorization: Bearer $TOKEN")
[ "$code" = "409" ] && grep -q '"error":"quorum_guard"' /tmp/gcc-resp.json
check $? "宕机 1 节点后移除在线节点 → 409 quorum_guard（$code $(cat /tmp/gcc-resp.json)）"
# 集群仍可写（3 voter 中 2 个存活）
for _ in $(seq 1 100); do
  code=$(curl -s -L -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:8081/v1/namespaces/dev/configs/guard.check" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"content":"ok","data_type":"TEXT"}')
  [ "$code" = "200" ] && break
  sleep 0.2
done
[ "$code" = "200" ]; check $? "护栏拒绝后集群写入不受影响（$code）"

kill -9 $N1 $N2 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
