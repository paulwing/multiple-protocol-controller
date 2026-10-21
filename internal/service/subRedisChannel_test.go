package service

import (
	"encoding/json"
	"testing"
)

func TestDecodeCommandPayloadKeepsIntegerPrecision(t *testing.T) {
	payload := []byte(`{"uid":"u-1","data":[{"device_id":"d-1","level":9007199254740993}]}`)
	command, err := decodeCommandPayload(payload)
	if err != nil {
		t.Fatalf("decodeCommandPayload error = %v", err)
	}
	if command.Uid != "u-1" || len(command.Data) != 1 {
		t.Fatalf("decoded command = %+v", command)
	}
	number, ok := command.Data[0]["level"].(json.Number)
	if !ok {
		t.Fatalf("value type = %T, want json.Number", command.Data[0]["level"])
	}
	if number.String() != "9007199254740993" {
		t.Fatalf("value = %s, want the exact integer", number)
	}
}

func TestDecodeCommandPayloadRejectsInvalidJSON(t *testing.T) {
	if _, err := decodeCommandPayload([]byte(`{"uid":`)); err == nil {
		t.Fatal("invalid JSON must be rejected")
	}
	if _, err := decodeCommandPayload([]byte(`{"uid":"u-1"} trailing`)); err == nil {
		t.Fatal("trailing content must be rejected, as json.Unmarshal did")
	}
}
