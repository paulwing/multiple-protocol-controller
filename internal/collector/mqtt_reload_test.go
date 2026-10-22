package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type mqttReloadConnection struct {
	net.Conn
	writeMu sync.Mutex
	options *packets.ConnectPacket
	closed  chan struct{}
}

func (c *mqttReloadConnection) write(packet packets.ControlPacket) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	return packet.Write(c.Conn)
}

type mqttReloadPublication struct {
	connection *mqttReloadConnection
	packet     *packets.PublishPacket
}

// This broker uses Paho's actual TCP/packet/route path. Only PUBACK is held by
// the test, so a real in-flight control command pins the old shared connection.
type mqttReloadBroker struct {
	listener         net.Listener
	mu               sync.Mutex
	closed           bool
	clients          map[*mqttReloadConnection]bool
	wg               sync.WaitGroup
	acceptDone       chan struct{}
	connects         chan *mqttReloadConnection
	subs             chan string
	publishes        chan mqttReloadPublication
	publicationCount atomic.Int32
}

func newMqttReloadBroker(t *testing.T) *mqttReloadBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &mqttReloadBroker{listener: listener, clients: make(map[*mqttReloadConnection]bool), acceptDone: make(chan struct{}),
		connects: make(chan *mqttReloadConnection, 8), subs: make(chan string, 16), publishes: make(chan mqttReloadPublication, 8)}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(b.acceptDone)
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			client := &mqttReloadConnection{Conn: raw, closed: make(chan struct{})}
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				_ = raw.Close()
				return
			}
			b.clients[client] = true
			b.mu.Unlock()
			b.wg.Add(1)
			go b.serve(client)
		}
	}()
	t.Cleanup(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		_ = listener.Close()
		<-b.acceptDone
		b.mu.Lock()
		for client := range b.clients {
			_ = client.Close()
		}
		b.mu.Unlock()
		b.wg.Wait()
	})
	return b
}

func (b *mqttReloadBroker) serve(client *mqttReloadConnection) {
	defer b.wg.Done()
	defer func() {
		_ = client.Close()
		close(client.closed)
		b.mu.Lock()
		delete(b.clients, client)
		b.mu.Unlock()
	}()
	for {
		packet, err := packets.ReadPacket(client)
		if err != nil {
			return
		}
		switch p := packet.(type) {
		case *packets.ConnectPacket:
			client.options = p
			if client.write(packets.NewControlPacket(packets.Connack)) != nil {
				return
			}
			b.connects <- client
		case *packets.SubscribePacket:
			ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			ack.MessageID, ack.ReturnCodes = p.MessageID, append([]byte(nil), p.Qoss...)
			if client.write(ack) != nil {
				return
			}
			for _, topic := range p.Topics {
				b.subs <- topic
			}
		case *packets.UnsubscribePacket:
			ack := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
			ack.MessageID = p.MessageID
			if client.write(ack) != nil {
				return
			}
		case *packets.PublishPacket:
			b.publicationCount.Add(1)
			b.publishes <- mqttReloadPublication{connection: client, packet: p}
		case *packets.PingreqPacket:
			if client.write(packets.NewControlPacket(packets.Pingresp)) != nil {
				return
			}
		case *packets.DisconnectPacket:
			return
		}
	}
}

func receiveMqttReload[T any](t *testing.T, channel <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func assertMqttReloadSnapshots(t *testing.T, client *mqttReloadConnection, sink *mqttSnapshotSink, value int) {
	t.Helper()
	for _, id := range []string{"a", "b"} {
		message := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		message.TopicName, message.Payload = "device/"+id, []byte(fmt.Sprintf(`{"value":%d}`, value))
		if err := client.write(message); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	for range 2 {
		write := receiveMqttReload(t, sink.writes, "both devices' realtime snapshot updates")
		var result struct {
			DeviceID string         `json:"device_id"`
			Values   map[string]int `json:"current_value"`
		}
		if err := json.Unmarshal([]byte(write.payload), &result); err != nil {
			t.Fatal(err)
		}
		if result.DeviceID != "a" && result.DeviceID != "b" || write.key != "device:data:"+result.DeviceID || result.Values["value"] != value || seen[result.DeviceID] {
			t.Fatalf("unexpected realtime snapshot: key=%q payload=%s", write.key, write.payload)
		}
		seen[result.DeviceID] = true
	}
}

func TestMqttSharedConnectionConfigReloadRestoresBothDevicesWithoutReplayingControl(t *testing.T) {
	for _, changedOption := range []string{"password", "keepAlive"} {
		t.Run(changedOption, func(t *testing.T) {
			broker := newMqttReloadBroker(t)
			transport := conn.NewMqttManager(10)
			manager := newManager(context.Background())
			sink := &mqttSnapshotSink{writes: make(chan mqttSnapshotWrite, 16)}
			resultWriterMu.Lock()
			previous := resultWriter
			resultWriter = &deviceResultWriter{ctx: context.Background(), redis: sink, snapshots: make(map[string]*deviceSnapshot)}
			resultWriterMu.Unlock()
			t.Cleanup(func() {
				manager.cancel()
				manager.update(config.IotCfgType{})
				_ = transport.Close()
				resultWriterMu.Lock()
				resultWriter = previous
				resultWriterMu.Unlock()
			})
			attempts := make(chan string, 16)
			manager.mqttWorkerFactory = func(ctx context.Context, spec mqttSpec) *mqttWorker {
				worker := newMqttWorker(ctx, spec)
				worker.subscribe = func(ctx context.Context, cfg conn.MqttConfig, topic string, qos byte, handler conn.MessageHandler) (func(), error) {
					release, err := transport.Watch(ctx, cfg, topic, qos, handler)
					if errors.Is(err, conn.ErrMqttConnectionConfigConflict) {
						select {
						case attempts <- spec.Device.Config.ID:
						default:
						}
					}
					return release, err
				}
				return worker
			}
			oldCfg := conn.MqttConfig{Broker: "tcp://" + broker.listener.Addr().String(), ClientID: "shared", Username: "reader", Password: "old", KeepAlive: 30, CleanSession: true}
			devices := make([]config.DeviceRuntime, 0, 2)
			for _, id := range []string{"a", "b"} {
				devices = append(devices, config.DeviceRuntime{
					Config:     config.DeviceConfig{ID: id, SerialNumber: "serial-" + id, Protocol: "MQTT"},
					MqttBroker: oldCfg.Broker, MqttClientID: oldCfg.ClientID, MqttUsername: oldCfg.Username,
					MqttPassword: oldCfg.Password, MqttKeepAlive: oldCfg.KeepAlive, MqttCleanSession: oldCfg.CleanSession,
					MqttReadPoints: []config.MqttParam{{Identify: "value", SubscribeTopic: "device/" + id, Path: "value", DataType: "int16", DataTypeExplicit: true}},
				})
			}
			manager.update(config.IotCfgType{Devices: devices})
			oldConnection := receiveMqttReload(t, broker.connects, "initial MQTT connection")
			receiveMqttReload(t, broker.subs, "initial first device subscription")
			receiveMqttReload(t, broker.subs, "initial second device subscription")
			assertMqttReloadSnapshots(t, oldConnection, sink, 1)

			published := make(chan error, 1)
			go func() { published <- transport.Publish(oldCfg, "control", []byte("command"), 1) }()
			command := receiveMqttReload(t, broker.publishes, "in-flight control PUBLISH")
			if command.packet.Qos != 1 || string(command.packet.Payload) != "command" {
				t.Fatalf("unexpected control packet: %#v", command.packet)
			}
			for i := range devices {
				if changedOption == "password" {
					devices[i].MqttPassword = "new"
				} else {
					devices[i].MqttKeepAlive = 60
				}
			}
			manager.update(config.IotCfgType{Devices: devices})
			failedAttempts := make(map[string]int)
			attemptDeadline := time.NewTimer(4 * time.Second)
			defer attemptDeadline.Stop()
			for failedAttempts["a"] < 2 || failedAttempts["b"] < 2 {
				select {
				case device := <-attempts:
					failedAttempts[device]++
				case <-attemptDeadline.C:
					t.Fatalf("both refreshed devices must retry while control pins the old connection; attempts=%v", failedAttempts)
				}
			}
			attemptDeadline.Stop()
			select {
			case err := <-published:
				t.Fatalf("configuration reload interrupted control before PUBACK: %v", err)
			case <-oldConnection.closed:
				t.Fatal("configuration reload disconnected the in-flight control")
			case <-broker.connects:
				t.Fatal("configuration reload replaced the connection before control finished")
			default:
			}
			ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
			ack.MessageID = command.packet.MessageID
			if err := command.connection.write(ack); err != nil {
				t.Fatal(err)
			}
			if err := receiveMqttReload(t, published, "original control completion"); err != nil {
				t.Fatal(err)
			}
			newConnection := receiveMqttReload(t, broker.connects, "new shared MQTT connection after control finishes")
			if newConnection.options.ClientIdentifier != oldCfg.ClientID || string(newConnection.options.Password) != devices[0].MqttPassword || int(newConnection.options.Keepalive) != devices[0].MqttKeepAlive {
				t.Fatal("replacement connection did not use the updated shared options")
			}
			topics := make(map[string]bool)
			for range 2 {
				topics[receiveMqttReload(t, broker.subs, "both refreshed device subscriptions")] = true
			}
			if !topics["device/a"] || !topics["device/b"] {
				t.Fatalf("restored subscriptions = %v", topics)
			}
			assertMqttReloadSnapshots(t, newConnection, sink, 2)
			if count := broker.publicationCount.Load(); count != 1 {
				t.Fatalf("control PUBLISH count = %d; want exactly one", count)
			}
		})
	}
}
