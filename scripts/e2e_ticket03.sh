#!/usr/bin/env bash
# 票 03 验收脚本：CAS + Delete/Tombstone + History 查询
# 覆盖 .scratch/config-center-mvp/issues/03-cas-delete-tombstone-history.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e03.log
NS=dev
ID=db.password

fail=0
check() { # $1=退出码 $2=描述
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}
code_of() { # $1=method $2=url $3=body → stdout=HTTP 状态码，响应体落 /tmp/gcc-resp.json
  curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X "$1" "$HTTP$2" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' ${3:+-d "$3"}
}

put() { # $1=body → stdout=状态码（等待选举完成，期间 503 重试）
  local code
  for _ in $(seq 1 100); do
    code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" "$1")
    [ "$code" != "503" ] && break
    sleep 0.1
  done
  echo "$code"
}

echo "=== 构建 ==="
go build -o "$BIN" ./cmd/config-center; check $? "go build"

echo "=== 准备：清空 data 目录与 PG 表 ==="
rm -rf data
$PG "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE" >/dev/null

echo "=== 启动单节点 ==="
"$BIN" --config config.yaml --bootstrap >"$LOG" 2>&1 &
SRV=$!
for _ in $(seq 1 120); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -sf "$HTTP/healthz" >/dev/null; check $? "服务就绪"

echo "=== 验收 1：expected_version 省略 = LWW，版本序列无空洞 ==="
code=$(put '{"content":"v1","data_type":"TEXT"}')
[ "$code" = "200" ] && grep -q '"version":1' /tmp/gcc-resp.json
check $? "首次 PUT → v1（$code $(cat /tmp/gcc-resp.json)）"
code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"v2","data_type":"TEXT"}')
[ "$code" = "200" ] && grep -q '"version":2' /tmp/gcc-resp.json
check $? "第二次 PUT → v2（$code）"
code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"v3","data_type":"TEXT"}')
[ "$code" = "200" ] && grep -q '"version":3' /tmp/gcc-resp.json
check $? "第三次 PUT → v3（$code）"
hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID' AND op='publish'")
[ "$hist" = "3" ]; check $? "History 3 条 publish，序列 1/2/3 无空洞（实际 $hist）"

echo "=== 验收 2：expected_version=0 = create-if-absent ==="
code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"x","data_type":"TEXT","expected_version":0}')
[ "$code" = "409" ] && grep -q '"current_version":3' /tmp/gcc-resp.json
check $? "对存活条目 expected=0 → 409 + current_version=3（$code）"
code=$(code_of PUT "/v1/namespaces/$NS/configs/fresh.id" '{"content":"new","data_type":"TEXT","expected_version":0}')
[ "$code" = "200" ] && grep -q '"created":true' /tmp/gcc-resp.json
check $? "对不存在条目 expected=0 → 200 created（$code）"

echo "=== 验收 3：expected_version=N 匹配通过 / 不匹配 409 ==="
code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"nope","data_type":"TEXT","expected_version":1}')
[ "$code" = "409" ] && grep -q '"current_version":3' /tmp/gcc-resp.json
check $? "expected=1 ≠ 当前 3 → 409 + current_version=3（$code）"
code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"v4","data_type":"TEXT","expected_version":3}')
[ "$code" = "200" ] && grep -q '"version":4' /tmp/gcc-resp.json
check $? "expected=3 匹配 → 200 v4（$code）"

echo "=== 验收 4：Delete 写 Tombstone，版本号照常消耗 ==="
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$HTTP/v1/namespaces/$NS/configs/$ID")
[ "$code" = "401" ]; check $? "无 token 的 DELETE → 401（实际 $code）"
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X DELETE \
  "$HTTP/v1/namespaces/$NS/configs/$ID?expected_version=4" -H "Authorization: Bearer $TOKEN")
[ "$code" = "200" ] && grep -q '"version":5' /tmp/gcc-resp.json && grep -q '"status":"deleted"' /tmp/gcc-resp.json
check $? "expected=4 匹配 → 200 {version:5, deleted}（$code $(cat /tmp/gcc-resp.json)）"
code=$(curl -s -o /dev/null -w '%{http_code}' "$HTTP/v1/namespaces/$NS/configs/$ID")
[ "$code" = "404" ]; check $? "删除后 GET → 404（实际 $code）"
tomb=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID' AND version=5 AND op='delete' AND content IS NULL")
[ "$tomb" = "1" ]; check $? "History 有 op=delete、content NULL 的 Tombstone 记录 v5"
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X DELETE \
  "$HTTP/v1/namespaces/$NS/configs/$ID" -H "Authorization: Bearer $TOKEN")
[ "$code" = "404" ]; check $? "重复 DELETE 已删除条目 → 404（实际 $code）"
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT \
  "$HTTP/v1/namespaces/$NS/configs/$ID" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"content":"reborn","data_type":"TEXT","expected_version":0}')
[ "$code" = "200" ] && grep -q '"version":6' /tmp/gcc-resp.json
check $? "墓碑后 expected=0 重建 → v6（版本序列延续，$code）"

echo "=== 验收 5：versions 接口倒序分页 ==="
body=$(curl -s "$HTTP/v1/namespaces/$NS/configs/$ID/versions")
echo "$body" | grep -q '"op":"delete"' && echo "$body" | grep -q '"content":null'
check $? "倒序列表含 Tombstone（content null）"
order=$(echo "$body" | grep -o '"version":[0-9]*' | cut -d: -f2 | tr '\n' ',')
[ "$order" = "6,5,4,3,2,1," ]; check $? "全量倒序 [6 5 4 3 2 1]（实际 $order）"
order=$(curl -s "$HTTP/v1/namespaces/$NS/configs/$ID/versions?limit=2" | grep -o '"version":[0-9]*' | cut -d: -f2 | tr '\n' ',')
[ "$order" = "6,5," ]; check $? "limit=2 → [6 5]（实际 $order）"
order=$(curl -s "$HTTP/v1/namespaces/$NS/configs/$ID/versions?before_version=3&limit=50" | grep -o '"version":[0-9]*' | cut -d: -f2 | tr '\n' ',')
[ "$order" = "2,1," ]; check $? "before_version=3 → [2 1]（实际 $order）"

echo "=== 进度一致性：无效操作的已提交日志也推进 raft_fsm_meta ==="
consistent=$($PG "SELECT (SELECT version FROM biz_configs WHERE namespace='$NS' AND data_id='$ID') = (SELECT max(version) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID')")
[ "$consistent" = "t" ]; check $? "biz_configs.version == max(History.version)"
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X PUT \
  "$HTTP/v1/namespaces/$NS/configs/$ID" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"content":"x","data_type":"TEXT","expected_version":1}')
[ "$code" = "409" ]; check $? "冲突写入 → 409（$code）"
consistent=$($PG "SELECT (SELECT version FROM biz_configs WHERE namespace='$NS' AND data_id='$ID') = (SELECT max(version) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID')")
[ "$consistent" = "t" ]; check $? "冲突的已提交日志不改变业务终态"

kill -9 $SRV 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
