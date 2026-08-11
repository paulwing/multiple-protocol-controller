package collector

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestNormalizeJudgeSourceConfig(t *testing.T) {
	disabled := normalizedJudgeSourceConfig(config.JudgeSourceCfg{})
	if disabled.enabled || disabled.stream != "judge:source" || disabled.writeTimeout != 200*time.Millisecond ||
		disabled.retryCount != 1 || disabled.retryInterval != 20*time.Millisecond || disabled.maximumEventBytes != 64<<10 {
		t.Fatalf("disabled defaults = %+v", disabled)
	}
	bounded := normalizedJudgeSourceConfig(config.JudgeSourceCfg{Enabled: true, Stream: " custom:source ", WriteTimeoutMS: 60_000, RetryCount: 100, RetryIntervalMS: 60_000, MaxEventBytes: 1 << 20})
	if !bounded.enabled || bounded.stream != "custom:source" || bounded.writeTimeout != 2*time.Second ||
		bounded.retryCount != 3 || bounded.retryInterval != time.Second || bounded.maximumEventBytes != 64<<10 {
		t.Fatalf("bounded config = %+v", bounded)
	}
}

func TestNewJudgeEventIDProducesCanonicalUUIDv4(t *testing.T) {
	seen := map[string]struct{}{}
	for range 128 {
		id, err := newJudgeEventID()
		if err != nil || !validJudgeEventID(id) {
			t.Fatalf("newJudgeEventID() = %q, %v", id, err)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate UUIDv4 %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestBuildJudgeSourceEventProducesExactFiveFields(t *testing.T) {
	eventID := "550e8400-e29b-41d4-a716-446655440000"
	event, err := buildJudgeSourceEvent(
		eventID, "device-1", "temperature",
		time.Date(2026, 8, 11, 1, 2, 3, 456000000, time.UTC),
		map[string]any{"temperature": 21.5, "online": true}, 64<<10,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"event_id": eventID, "device_id": "device-1", "updated_point": "temperature",
		"collected_at": "2026-08-11T01:02:03.456Z", "values": `{"online":true,"temperature":21.5}`,
	}
	if !reflect.DeepEqual(event.redisValues(), want) {
		t.Fatalf("fields = %#v, want %#v", event.redisValues(), want)
	}
}

func TestBuildJudgeSourceEventRejectsInvalidContract(t *testing.T) {
	validID := "550e8400-e29b-41d4-a716-446655440000"
	tooManyValues := make(map[string]any, 65)
	for i := 0; i < 65; i++ {
		tooManyValues[string(rune('a'+i))] = i
	}
	tests := []struct {
		name, eventID, deviceID, updatedPoint string
		values                                map[string]any
		maximumBytes                          int
	}{
		{name: "empty event ID", deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "not UUID", eventID: "event-1", deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "UUIDv1", eventID: "550e8400-e29b-11d4-a716-446655440000", deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "wrong variant", eventID: "550e8400-e29b-41d4-0716-446655440000", deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "uppercase", eventID: "550E8400-E29B-41D4-A716-446655440000", deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "empty device", eventID: validID, updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "long device", eventID: validID, deviceID: strings.Repeat("d", 65), updatedPoint: "p", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "empty point", eventID: validID, deviceID: "d1", values: map[string]any{"p": 1}, maximumBytes: 64 << 10},
		{name: "long point", eventID: validID, deviceID: "d1", updatedPoint: strings.Repeat("p", 129), values: map[string]any{strings.Repeat("p", 129): 1}, maximumBytes: 64 << 10},
		{name: "missing point", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"q": 1}, maximumBytes: 64 << 10},
		{name: "null updated point", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": nil}, maximumBytes: 64 << 10},
		{name: "nested value", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": map[string]any{"nested": true}}, maximumBytes: 64 << 10},
		{name: "nan", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": math.NaN()}, maximumBytes: 64 << 10},
		{name: "too many values", eventID: validID, deviceID: "d1", updatedPoint: "a", values: tooManyValues, maximumBytes: 64 << 10},
		{name: "long string", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": strings.Repeat("x", (8<<10)+1)}, maximumBytes: 64 << 10},
		{name: "small envelope budget", eventID: validID, deviceID: "d1", updatedPoint: "p", values: map[string]any{"p": strings.Repeat("x", 100)}, maximumBytes: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildJudgeSourceEvent(test.eventID, test.deviceID, test.updatedPoint, time.Now(), test.values, test.maximumBytes)
			if err == nil {
				t.Fatal("buildJudgeSourceEvent() error = nil, want contract error")
			}
		})
	}
}
