package raftlog

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEncodeDecodePublishRoundtrip(t *testing.T) {
	expected := int64(3)
	cmd, err := NewPublish(PublishData{
		Namespace:       "dev",
		DataId:          "db.password",
		DataType:        "JSON",
		Content:         `{"k":"v"}`,
		Tags:            map[string]string{"team": "core"},
		ExpectedVersion: &expected,
		Actor:           "admin",
	})
	if err != nil {
		t.Fatalf("NewPublish: %v", err)
	}
	if cmd.Op != OpPublish || cmd.V != FormatVersion {
		t.Fatalf("envelope = {v:%d, op:%q}, want {v:%d, op:%q}", cmd.V, cmd.Op, FormatVersion, OpPublish)
	}

	b, err := Encode(cmd)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	data, err := got.Publish()
	if err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	if data.Namespace != "dev" || data.DataId != "db.password" || data.DataType != "JSON" ||
		data.Content != `{"k":"v"}` || data.Tags["team"] != "core" || data.Actor != "admin" {
		t.Fatalf("roundtrip lost fields: %+v", data)
	}
	if data.ExpectedVersion == nil || *data.ExpectedVersion != 3 {
		t.Fatalf("expected_version lost in roundtrip: %v", data.ExpectedVersion)
	}
}

func TestDecodeAcceptsWireFormatFromDesign(t *testing.T) {
	// DESIGN §3.2 的信封原文（delete/rollback 本票不实现，但解码必须认识）。
	for _, tc := range []struct {
		wire string
		op   Op
	}{
		{`{"v":1,"op":"delete","data":{"namespace":"dev","data_id":"a","expected_version":null,"actor":"u"}}`, OpDelete},
		{`{"v":1,"op":"rollback","data":{"namespace":"dev","data_id":"a","to_version":3,"actor":"u"}}`, OpRollback},
	} {
		cmd, err := Decode([]byte(tc.wire))
		if err != nil {
			t.Fatalf("Decode(%s): %v", tc.wire, err)
		}
		if cmd.Op != tc.op {
			t.Fatalf("op = %q, want %q", cmd.Op, tc.op)
		}
		var raw map[string]any
		if err := json.Unmarshal(cmd.Data, &raw); err != nil {
			t.Fatalf("data 不是 JSON 对象: %v", err)
		}
	}
}

func TestDecodeRejectsUnknownVersionAndOp(t *testing.T) {
	if _, err := Decode([]byte(`{"v":2,"op":"publish","data":{}}`)); err == nil {
		t.Fatal("未来格式版本 v=2 应被拒绝")
	}
	if _, err := Decode([]byte(`{"v":1,"op":"explode","data":{}}`)); err == nil {
		t.Fatal("未知 op 应被拒绝")
	}
	if _, err := Decode([]byte(`not json`)); err == nil {
		t.Fatal("非 JSON 应被拒绝")
	}
}

func TestNewPublishValidation(t *testing.T) {
	negative := int64(-1)
	cases := []struct {
		name string
		data PublishData
	}{
		{"空 Namespace", PublishData{DataId: "a", DataType: "TEXT", Content: "x"}},
		{"空 DataId", PublishData{Namespace: "dev", DataType: "TEXT", Content: "x"}},
		{"Namespace 超长", PublishData{Namespace: strings.Repeat("n", MaxNamespaceLen+1), DataId: "a", DataType: "TEXT", Content: "x"}},
		{"DataId 超长", PublishData{Namespace: "dev", DataId: strings.Repeat("d", MaxDataIdLen+1), DataType: "TEXT", Content: "x"}},
		{"DataType 非法", PublishData{Namespace: "dev", DataId: "a", DataType: "TOML", Content: "x"}},
		{"expected_version 为负", PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: "x", ExpectedVersion: &negative}},
		{"content 超 1MB", PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: strings.Repeat("c", MaxContentBytes+1)}},
	}
	for _, tc := range cases {
		if _, err := NewPublish(tc.data); err == nil {
			t.Errorf("%s: 应返回错误", tc.name)
		}
	}
	if _, err := NewPublish(PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: "x"}); err != nil {
		t.Errorf("合法最小 publish 不应报错: %v", err)
	}
}
