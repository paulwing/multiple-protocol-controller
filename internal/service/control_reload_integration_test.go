package service

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/internal/control"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// Exercise the real Redis subscriber, configuration loader, command processor
// and Paho client together. Only the MQTT endpoint is simulated. No collectors
// are started; their shared-connection handover is covered in collector tests.
func TestRedisConfigNotificationWaitsForInFlightControl(t *testing.T) {
	// Production connection managers are process-wide. Isolate this integration
	// so shutting them down cannot leave a closed singleton for another test.
	if os.Getenv("MPC_LOCAL_CONTROL_TEST_CHILD") != "1" {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(binary, "-test.run=^TestRedisConfigNotificationWaitsForInFlightControl$", "-test.v", "-test.timeout=30s")
		child.Env = append(os.Environ(), "MPC_LOCAL_CONTROL_TEST_CHILD=1")
		output, err := child.CombinedOutput()
		if err != nil {
			t.Fatalf("local control integration: %v\n%s", err, output)
		}
		if strings.Contains(string(output), "--- SKIP:") {
			t.Skip(string(output))
		}
		return
	}
	core, logs := observer.New(zap.DebugLevel)
	previousLogger, previousConfig := logger.Log, config.IotCfgStore
	logger.Log = zap.New(core)
	config.IotCfgStore = atomic.Value{}
	t.Cleanup(func() { logger.Log, config.IotCfgStore = previousLogger, previousConfig })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rdb := startControlTestRedis(t, ctx)
	broker := startControlTestMQTT(t)
	settings := &config.Config{Redis: config.RedisCfg{Address: rdb.Options().Addr}}
	writeConfig := func(topic string) {
		t.Helper()
		protocol, err := json.Marshal(map[string]any{"type": "MQTT", "topics": map[string]any{"publish": []map[string]any{{"topic": topic, "path": "value", "qos": 1, "dataType": "int16"}}}})
		if err != nil {
			t.Fatal(err)
		}
		device := config.DeviceConfig{ID: "local-device", SerialNumber: "local-serial", Protocol: "MQTT", Username: "local-user", Password: "private-local-password",
			Properties: []config.Property{{Key: "setting", Type: "int", Access: "01", Protocol: protocol}},
			Params:     []config.DeviceParam{{Key: "clientId", Type: "string", Value: json.RawMessage(`"local-control-client"`)}}}
		host, port, err := net.SplitHostPort(broker.listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		device.GatewayInfo.IP = host
		device.GatewayInfo.Port, err = strconv.Atoi(port)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal([]config.DeviceConfig{device})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(config.RedisWrapper{Version: "local-test", Key: config.DeviceCfg, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		if err := rdb.Set(ctx, config.DeviceCfg, payload, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig("control/before")
	if err := GetIoTCfg(ctx, &config.IotCfgStore, settings); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.ShutdownDefault() })
	// MQTT writes use a connection already owned by acquisition. Supply that
	// owner explicitly; this test does not launch the global collector manager.
	runtime := config.IotCfgStore.Load().(config.IotCfgType).DeviceBySerial["local-serial"]
	mqttManager := conn.DefaultMqttManager()
	t.Cleanup(func() { _ = mqttManager.Close() })
	release, err := mqttManager.Watch(ctx, conn.MqttConfig{
		Broker: runtime.MqttBroker, ClientID: runtime.MqttClientID, Username: runtime.MqttUsername,
		Password: runtime.MqttPassword, KeepAlive: runtime.MqttKeepAlive, CleanSession: runtime.MqttCleanSession,
	}, "telemetry/local", 0, func(string, []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	select {
	case <-broker.subscribed:
	case <-ctx.Done():
		t.Fatal("acquisition connection did not subscribe")
	}

	client, err := store.NewRedisClient(ctx, settings.Redis.Address, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	loopCtx, stopLoop := context.WithCancel(ctx)
	reloaded := make(chan error, 1)
	// Record at the actual callback boundaries, not by sampling configuration
	// from the test goroutine. A reload racing the first command must remain
	// visible even if it finishes between two observations below.
	events := make(chan string, 16)
	finished := client.SubscribeWithRetry(loopCtx, []string{config.SetDeviceProperty, config.CfgChangeCh}, map[string]func(*redis.Message) error{
		config.SetDeviceProperty: newCommandHandler(func(command config.Command4MPC) error {
			events <- command.Uid + ":started"
			err := control.ProcessCommand(client, command)
			events <- command.Uid + ":finished"
			return err
		}),
		config.CfgChangeCh: newCfgChangeHandler(func() error {
			events <- "reload:started"
			err := GetIoTCfg(ctx, &config.IotCfgStore, settings)
			events <- "reload:finished"
			reloaded <- err
			return err
		}),
	})
	defer func() {
		stopLoop()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("subscriber stopped with error: %v", err)
			}
		case <-time.After(6 * time.Second):
			t.Error("subscriber did not finish")
		}
	}()
	awaitControlTest(t, ctx, func() bool {
		counts, err := rdb.PubSubNumSub(ctx, config.SetDeviceProperty, config.CfgChangeCh).Result()
		return err == nil && counts[config.SetDeviceProperty] == 1 && counts[config.CfgChangeCh] == 1
	})
	receipts := rdb.Subscribe(ctx, config.SendCmdResCh)
	defer receipts.Close()
	if _, err := receipts.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	commands := []string{
		`{"uid":"local-inflight","data":[{"device_id":"local-serial","setting":17}]}`,
		`{"uid":"local-next","data":[{"device_id":"local-serial","setting":23}]}`,
	}
	if err := rdb.Publish(ctx, config.SetDeviceProperty, commands[0]).Err(); err != nil {
		t.Fatal(err)
	}
	first := receiveControlWrite(t, ctx, broker.writes)
	ackFirst := sync.OnceFunc(func() { first.ack(t) })
	defer ackFirst() // Also release the blocked command if an assertion fails.
	if first.password != "private-local-password" || first.packet.TopicName != "control/before" || string(first.packet.Payload) != `{"value":17}` || first.packet.Qos != 1 {
		t.Fatal("initial control did not use the original configuration and payload")
	}
	observed := []string{receiveControlEvent(t, ctx, events)}
	if observed[0] != "local-inflight:started" {
		t.Fatalf("first control did not start first: %v", observed)
	}
	// Hold its real MQTT PUBACK while publishing a changed configuration and a
	// second command on the same Redis subscription. Both must wait their turn.
	writeConfig("control/after")
	if err := rdb.Publish(ctx, config.CfgChangeCh, config.DeviceCfg).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Publish(ctx, config.SetDeviceProperty, commands[1]).Err(); err != nil {
		t.Fatal(err)
	}
	// Give the real subscriber time to expose an incorrectly asynchronous
	// dispatch while PUBACK is held. The complete event-order assertion below
	// also catches overtaking after this observation window, before dispatch
	// actually finishes; a timer alone is not evidence of correct ordering.
	hold := time.NewTimer(200 * time.Millisecond)
	defer hold.Stop()
	for waiting := true; waiting; {
		select {
		case event := <-events:
			observed = append(observed, event)
			t.Errorf("operation advanced while the first control awaited PUBACK: %s", event)
		case <-first.closed:
			t.Fatal("configuration notification interrupted the MQTT connection")
		case <-hold.C:
			waiting = false
		case <-ctx.Done():
			t.Fatal("timed out while holding the first control")
		}
	}
	if got := config.IotCfgStore.Load().(config.IotCfgType).DeviceMqttCmdBySerial["local-serial"][0].PublishTopic; got != "control/before" {
		t.Error("configuration overtook an in-flight command")
	}
	if count := broker.count.Load(); count != 1 {
		t.Errorf("MQTT writes while first command was held=%d, want 1", count)
	}
	ackFirst()
	assertControlReceipt(t, ctx, receipts, "local-inflight")
	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("configuration did not reload after control completion")
	}
	second := receiveControlWrite(t, ctx, broker.writes)
	if second.packet.TopicName != "control/after" || string(second.packet.Payload) != `{"value":23}` {
		t.Fatal("next control did not use the newly loaded configuration")
	}
	second.ack(t)
	assertControlReceipt(t, ctx, receipts, "local-next")
	wantEvents := []string{
		"local-inflight:started", "local-inflight:finished",
		"reload:started", "reload:finished",
		"local-next:started", "local-next:finished",
	}
	for len(observed) < len(wantEvents) {
		observed = append(observed, receiveControlEvent(t, ctx, events))
	}
	if !reflect.DeepEqual(observed, wantEvents) {
		t.Errorf("control/reload order=%v, want %v", observed, wantEvents)
	}
	awaitControlTest(t, ctx, func() bool {
		return logs.FilterField(zap.String("stage", "receipt_publish")).Len() == 2
	})
	if broker.count.Load() != 2 {
		t.Fatalf("MQTT writes=%d, want one per command", broker.count.Load())
	}
	for _, id := range []string{"local-inflight", "local-next"} {
		entries := logs.FilterField(zap.String("command_id", id))
		for _, stage := range []string{"receive", "execution_finished", "receipt_publish"} {
			if entries.FilterField(zap.String("stage", stage)).Len() != 1 {
				t.Errorf("missing or repeated %s for %s", stage, id)
			}
		}
	}
	for _, entry := range logs.All() {
		for _, field := range entry.Context {
			if strings.Contains(field.String, "local-password") {
				t.Fatal("connection password appeared in logs")
			}
		}
	}
}

func receiveControlEvent(t *testing.T, ctx context.Context, events <-chan string) string {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal("control/reload event was not received")
		return ""
	}
}

func assertControlReceipt(t *testing.T, ctx context.Context, subscription *redis.PubSub, uid string) {
	t.Helper()
	message, err := subscription.ReceiveMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Result struct {
			UID  string `json:"uid"`
			Data []struct {
				Status string `json:"verification_status"`
			} `json:"data"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(message.Payload), &result); err != nil {
		t.Fatal(err)
	}
	if result.Result.UID != uid || len(result.Result.Data) != 1 || result.Result.Data[0].Status != "unverified" {
		t.Fatalf("receipt identity/status changed: %s", message.Payload)
	}
}

func awaitControlTest(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatal("local integration condition timed out")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func startControlTestRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server unavailable; real local Redis integration not run")
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	_ = reservation.Close()
	dir := t.TempDir()
	process := exec.Command(binary, "--bind", "127.0.0.1", "--port", port, "--save", "", "--appendonly", "no", "--dir", dir, "--logfile", filepath.Join(dir, "redis.log"))
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	awaitControlTest(t, ctx, func() bool { return client.Ping(ctx).Err() == nil })
	return client
}

type controlTestMQTT struct {
	listener   net.Listener
	writes     chan controlTestWrite
	subscribed chan struct{}
	count      atomic.Int32
	mu         sync.Mutex
	clients    []net.Conn
	wg         sync.WaitGroup
}

type controlTestWrite struct {
	connection net.Conn
	closed     <-chan struct{}
	password   string
	packet     *packets.PublishPacket
}

func (w controlTestWrite) ack(t *testing.T) {
	t.Helper()
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = w.packet.MessageID
	_ = w.connection.SetWriteDeadline(time.Now().Add(time.Second))
	if err := ack.Write(w.connection); err != nil {
		t.Fatal(err)
	}
}

func receiveControlWrite(t *testing.T, ctx context.Context, writes <-chan controlTestWrite) controlTestWrite {
	t.Helper()
	select {
	case write := <-writes:
		return write
	case <-ctx.Done():
		t.Fatal("MQTT control was not received")
		return controlTestWrite{}
	}
}

func startControlTestMQTT(t *testing.T) *controlTestMQTT {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	broker := &controlTestMQTT{listener: listener, writes: make(chan controlTestWrite, 8), subscribed: make(chan struct{}, 8)}
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			broker.mu.Lock()
			broker.clients = append(broker.clients, client)
			broker.mu.Unlock()
			broker.wg.Add(1)
			go func() {
				defer broker.wg.Done()
				defer client.Close()
				closed := make(chan struct{})
				defer close(closed)
				password := ""
				for {
					packet, err := packets.ReadPacket(client)
					if err != nil {
						return
					}
					switch value := packet.(type) {
					case *packets.ConnectPacket:
						password = string(value.Password)
						if packets.NewControlPacket(packets.Connack).Write(client) != nil {
							return
						}
					case *packets.PublishPacket:
						broker.count.Add(1)
						broker.writes <- controlTestWrite{connection: client, closed: closed, password: password, packet: value}
					case *packets.SubscribePacket:
						ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
						ack.MessageID, ack.ReturnCodes = value.MessageID, append([]byte(nil), value.Qoss...)
						if ack.Write(client) != nil {
							return
						}
						broker.subscribed <- struct{}{}
					case *packets.UnsubscribePacket:
						ack := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
						ack.MessageID = value.MessageID
						if ack.Write(client) != nil {
							return
						}
					case *packets.DisconnectPacket:
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		broker.mu.Lock()
		for _, client := range broker.clients {
			_ = client.Close()
		}
		broker.mu.Unlock()
		broker.wg.Wait()
	})
	return broker
}
