#!/usr/bin/env bash
# 票 08 验收脚本：发现代理 + Join + 写重定向（本机 1 PG + 3 库 + 3 进程拓扑）
# 覆盖 .scratch/config-center-mvp/issues/08-discovery-join-redirect.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d postgres -tAc"
LOGDIR=/tmp/gcc-e2e08
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

wait_health() { # $1=http端口
  for _ in $(seq 1 200); do
    curl -sf "http://127.0.0.1:$1/healthz" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  return 1
}

wait_voters() { # $1=http端口 $2=期望 voter 数
  for _ in $(seq 1 300); do
    n=$(curl -s "http://127.0.0.1:$1/v1/cluster/status" -H "Authorization: Bearer $TOKEN" \
      | grep -o '"suffrage":"Voter"' | wc -l)
    [ "$n" = "$2" ] && return 0
    sleep 0.2
  done
  return 1
}

leader_http() { # $1=http端口 → stdout=Leader HTTP 基地址（可能为空）
  curl -s "http://127.0.0.1:$1/internal/discovery" | grep -o '"leader_http_addr":"[^"]*"' | cut -d'"' -f4
}

echo "=== 构建与准备 ==="
go build -o "$BIN" ./cmd/config-center; check $? "go build"
rm -rf data-1 data-2 data-3 data-4
for db in config_center_1 config_center_2 config_center_3 config_center_4; do
  $PG "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
  $PG "CREATE DATABASE $db" >/dev/null
done

gen_config 1 8080 7946 config_center_1 "127.0.0.1:8081" "127.0.0.1:8082"
gen_config 2 8081 7947 config_center_2 "127.0.0.1:8080" "127.0.0.1:8082"
gen_config 3 8082 7948 config_center_3 "127.0.0.1:8080" "127.0.0.1:8081"

echo "=== 验收 1：节点 1 bootstrap，节点 2/3 自动加入并成为 voter ==="
"$BIN" --config "$LOGDIR/config-1.yaml" --bootstrap >"$LOGDIR/n1.log" 2>&1 & N1=$!
wait_health 8080; check $? "节点 1 就绪"
"$BIN" --config "$LOGDIR/config-2.yaml" >"$LOGDIR/n2.log" 2>&1 & N2=$!
"$BIN" --config "$LOGDIR/config-3.yaml" >"$LOGDIR/n3.log" 2>&1 & N3=$!
wait_health 8081 && wait_health 8082; check $? "节点 2/3 HTTP 就绪"
wait_voters 8080 3; check $? "成员表 3 个 voter（/v1/cluster/status）"

echo "=== 验收 5：/internal/discovery 返回 node_id、role、leader_addr ==="
for port in 8080 8081 8082; do
  body=$(curl -s "http://127.0.0.1:$port/internal/discovery")
  echo "$body" | grep -q '"node_id":' && echo "$body" | grep -q '"role":"' && echo "$body" | grep -q '"leader_addr":"127.0.0.1:'
  check $? "节点($port) discovery 响应完整（$body）"
done
LEADER=$(leader_http 8080)
[ -n "$LEADER" ]; check $? "Leader HTTP 地址已知（$LEADER）"

echo "=== 验收 2：向 Follower 发 PUT → 307 → 跟随重定向写入成功 ==="
FOLLOWER=""
for port in 8080 8081 8082; do
  role=$(curl -s "http://127.0.0.1:$port/internal/discovery" | grep -o '"role":"[^"]*"' | cut -d'"' -f4)
  if [ "$role" = "follower" ]; then FOLLOWER=$port; break; fi
done
[ -n "$FOLLOWER" ]; check $? "找到 Follower（端口 $FOLLOWER）"
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/app.yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"content":"cluster-v1","data_type":"YAML"}')
[ "$code" = "307" ]; check $? "Follower PUT → 307（实际 $code）"
code=$(curl -s -L -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/app.yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"content":"cluster-v1","data_type":"YAML"}')
[ "$code" = "200" ] && grep -q '"version":1' /tmp/gcc-resp.json
check $? "跟随 307 重定向 → 写入成功（$code $(cat /tmp/gcc-resp.json)）"
sleep 0.5
for port in 8080 8081 8082; do
  body=$(curl -s "http://127.0.0.1:$port/v1/namespaces/dev/configs/app.yaml")
  echo "$body" | grep -q '"version":1'
  check $? "节点($port) 读到复制数据"
done
# 307 保留查询串：DELETE expected_version 经 Follower
code=$(curl -s -L -o /tmp/gcc-resp.json -w '%{http_code}' -X DELETE "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/app.yaml?expected_version=1" \
  -H "Authorization: Bearer $TOKEN")
[ "$code" = "200" ] && grep -q '"status":"deleted"' /tmp/gcc-resp.json
check $? "Follower DELETE 带 expected_version 经 307 → 删除成功（$code）"

echo "=== 验收 3：选举窗口内写请求 503 + Retry-After ==="
LEADER=$(leader_http 8080)
LEADER_PORT=${LEADER##*:}
kill -9 $(case $LEADER_PORT in 8080) echo $N1;; 8081) echo $N2;; 8082) echo $N3;; esac) 2>/dev/null
saw503=0
for _ in $(seq 1 40); do
  code=$(curl -s -o /dev/null -D /tmp/gcc-headers.txt -w '%{http_code}' -X PUT "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/elec.test" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"content":"x","data_type":"TEXT"}' 2>/dev/null)
  if [ "$code" = "503" ]; then saw503=1; break; fi
  sleep 0.05
done
[ "$saw503" = "1" ]; check $? "选举窗口内捕获 503（实际 $code）"
if [ "$saw503" = "1" ]; then
  grep -qi "retry-after: 1" /tmp/gcc-headers.txt; check $? "503 响应带 Retry-After: 1"
else
  check 1 "503 响应带 Retry-After: 1（未捕获 503，无法校验）"
fi

echo "=== 等待重选举，集群恢复可用 ==="
sleep 4
LEADER2=""
for port in 8080 8081 8082; do
  curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 || continue
  role=$(curl -s "http://127.0.0.1:$port/internal/discovery" | grep -o '"role":"[^"]*"' | cut -d'"' -f4)
  [ "$role" = "leader" ] && LEADER2=$port && break
done
[ -n "$LEADER2" ]; check $? "重选举完成，新 Leader 在端口 $LEADER2"
for _ in $(seq 1 100); do
  code=$(curl -s -L -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/elec.test" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"content":"after-election","data_type":"TEXT"}')
  [ "$code" = "200" ] && break
  sleep 0.2
done
[ "$code" = "200" ]; check $? "新 Leader 下写入成功（$code）"
body=$(curl -s "http://127.0.0.1:$FOLLOWER/v1/namespaces/dev/configs/app.yaml")
[ -z "$body" ] || echo "$body" | grep -q '"error":"not_found"' || true
# 幸存节点的旧数据仍在
survivor=$LEADER2
body=$(curl -s "http://127.0.0.1:$survivor/v1/namespaces/dev/configs/elec.test")
echo "$body" | grep -q 'after-election'; check $? "重选举后数据可读"

kill -9 $N1 $N2 $N3 2>/dev/null

echo "=== 验收 4：Seed 全不可达 → 持续退避重试，不自举、不退出 ==="
gen_config 4 8083 7949 config_center_4 "127.0.0.1:59991" "127.0.0.1:59992"
"$BIN" --config "$LOGDIR/config-4.yaml" >"$LOGDIR/n4.log" 2>&1 & N4=$!
wait_health 8083; check $? "节点 4 进程存活且 HTTP 可服务"
sleep 3
kill -0 $N4 2>/dev/null; check $? "3 秒后节点 4 仍在运行（未退出）"
grep -q "退避重试" "$LOGDIR/n4.log"; check $? "日志显示退避重试（不自举）"
if grep -q "BootstrapCluster\|已当选 Leader" "$LOGDIR/n4.log"; then
  check 1 "节点 4 不得成为 Leader"
else
  check 0 "节点 4 未自举（无 Bootstrap/当选记录）"
fi
kill -9 $N4 2>/dev/null

echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
