package conn

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

// pipeBroker exercises Paho's actual packet reader and topic router without
// opening a socket or requiring an MQTT server.
type pipeBroker struct {
	mu                  sync.Mutex
	writeMu             sync.Mutex
	conn                net.Conn
	subs                chan *packets.SubscribePacket
	unsubs              chan *packets.UnsubscribePacket
	pubs                chan *packets.PublishPacket
	connections         chan net.Conn
	failConnect         int
	rejectSubscriptions int
	holdPublish         bool
	holdSubscription    bool
	dropSubacks         int
	holdUnsubscribe     bool
	session             map[string]byte
	liveConnections     int
	maxLiveConnections  int
}

func newPipeBroker(t *testing.T) *pipeBroker {
	t.Helper()
	b := &pipeBroker{subs: make(chan *packets.SubscribePacket, 100), unsubs: make(chan *packets.UnsubscribePacket, 100), pubs: make(chan *packets.PublishPacket, 100), connections: make(chan net.Conn, 100), session: make(map[string]byte)}
	t.Cleanup(b.close)
	return b
}

func (b *pipeBroker) open(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
	b.mu.Lock()
	if b.failConnect > 0 {
		b.failConnect--
		b.mu.Unlock()
		return nil, fmt.Errorf("test broker unavailable")
	}
	client, server := net.Pipe()
	b.conn = server
	b.liveConnections++
	if b.liveConnections > b.maxLiveConnections {
		b.maxLiveConnections = b.liveConnections
	}
	b.mu.Unlock()
	b.connections <- server
	go b.serve(server)
	return client, nil
}

func (b *pipeBroker) serve(c net.Conn) {
	defer func() { c.Close(); b.mu.Lock(); b.liveConnections--; b.mu.Unlock() }()
	for {
		packet, err := packets.ReadPacket(c)
		if err != nil {
			return
		}
		switch p := packet.(type) {
		case *packets.ConnectPacket:
			b.mu.Lock()
			if p.CleanSession {
				b.session = make(map[string]byte)
			}
			b.mu.Unlock()
			ack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			if b.write(c, ack) != nil {
				return
			}
		case *packets.SubscribePacket:
			b.subs <- p
			ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			ack.MessageID = p.MessageID
			ack.ReturnCodes = append([]byte(nil), p.Qoss...)
			b.mu.Lock()
			if b.rejectSubscriptions > 0 {
				b.rejectSubscriptions--
				ack.ReturnCodes[0] = 0x80
			}
			hold := b.holdSubscription
			if b.dropSubacks > 0 {
				b.dropSubacks--
				hold = true
			}
			for i, topic := range p.Topics {
				if ack.ReturnCodes[i] != 0x80 {
					b.session[topic] = p.Qoss[i]
				}
			}
			b.mu.Unlock()
			if hold {
				continue
			}
			if b.write(c, ack) != nil {
				return
			}
		case *packets.UnsubscribePacket:
			b.unsubs <- p
			b.mu.Lock()
			for _, topic := range p.Topics {
				delete(b.session, topic)
			}
			hold := b.holdUnsubscribe
			b.mu.Unlock()
			if hold {
				continue
			}
			ack := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
			ack.MessageID = p.MessageID
			if b.write(c, ack) != nil {
				return
			}
		case *packets.PublishPacket:
			b.pubs <- p
			b.mu.Lock()
			hold := b.holdPublish
			b.mu.Unlock()
			if p.Qos == 1 && !hold {
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = p.MessageID
				if b.write(c, ack) != nil {
					return
				}
			}
		case *packets.PingreqPacket:
			if b.write(c, packets.NewControlPacket(packets.Pingresp)) != nil {
				return
			}
		case *packets.DisconnectPacket:
			return
		}
	}
}

func (b *pipeBroker) write(c net.Conn, p packets.ControlPacket) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return p.Write(c)
}

func (b *pipeBroker) publish(t *testing.T, topic, payload string) {
	t.Helper()
	b.mu.Lock()
	c := b.conn
	b.mu.Unlock()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = topic, []byte(payload)
	if err := b.write(c, p); err != nil {
		t.Fatal(err)
	}
}

func (b *pipeBroker) close() {
	b.mu.Lock()
	c := b.conn
	b.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func receiveMqtt[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MQTT activity")
		var zero T
		return zero
	}
}

func testMqttConfig() MqttConfig {
	return MqttConfig{Broker: "tcp://pipe.invalid:1883", ClientID: "collector", KeepAlive: 30, CleanSession: true}
}

func pipeMqttManager(t *testing.T, broker *pipeBroker) *MqttManager {
	t.Helper()
	mgr := NewMqttManager(10)
	mgr.retryInterval = 10 * time.Millisecond
	mgr.operationTimeout = 80 * time.Millisecond
	mgr.cleanupTimeout = 80 * time.Millisecond
	mgr.newClient = func(opts *mqtt.ClientOptions) mqtt.Client {
		return mqtt.NewClient(opts.SetCustomOpenConnectionFn(broker.open).SetMaxReconnectInterval(20 * time.Millisecond))
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

func watchMqtt(t *testing.T, mgr *MqttManager, cfg MqttConfig, topic string, handler MessageHandler) func() {
	t.Helper()
	release, err := mgr.Watch(context.Background(), cfg, topic, 1, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return release
}

func mqttEventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("MQTT state did not settle")
}

func mqttCounts(mgr *MqttManager) (int, int) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	owners := 0
	for _, entry := range mgr.clients {
		for _, subscriptions := range entry.filters {
			owners += len(subscriptions)
		}
	}
	return len(mgr.clients), owners
}

func TestMqttManagerDeliversPahoMessageToRegisteredHandler(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	got := make(chan string, 4)
	watchMqtt(t, mgr, testMqttConfig(), "device/+/data", func(topic string, payload []byte) { got <- topic + ":" + string(payload) })
	receiveMqtt(t, b.subs)
	b.publish(t, "other/one/data", `{"value":10}`)
	b.publish(t, "device/one/data", `{"value":42}`)
	if value := receiveMqtt(t, got); value != `device/one/data:{"value":42}` {
		t.Fatalf("message = %q", value)
	}
	select {
	case extra := <-got:
		t.Fatalf("unexpected message: %s", extra)
	default:
	}
}

func TestMqttSharedFilterKeepsOtherOwnerAndReleasesLastConnection(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	first, second := make(chan string, 4), make(chan string, 4)
	releaseFirst := watchMqtt(t, mgr, testMqttConfig(), "device/#", func(_ string, payload []byte) { first <- string(payload) })
	releaseSecond := watchMqtt(t, mgr, testMqttConfig(), "device/#", func(_ string, payload []byte) { second <- string(payload) })
	receiveMqtt(t, b.subs)
	b.publish(t, "device", "one") // # also matches zero additional levels.
	if receiveMqtt(t, first) != "one" || receiveMqtt(t, second) != "one" {
		t.Fatal("shared route lost an owner")
	}
	releaseFirst()
	releaseFirst()
	b.publish(t, "device/deep/value", "two")
	if receiveMqtt(t, second) != "two" {
		t.Fatal("remaining owner did not receive")
	}
	select {
	case <-first:
		t.Fatal("released owner received a message")
	case <-b.unsubs:
		t.Fatal("shared topic unsubscribed")
	default:
	}
	releaseSecond()
	mqttEventually(t, func() bool { clients, owners := mqttCounts(mgr); return clients == 0 && owners == 0 })
}

func TestMqttConnectionsWithSameTopicStayIsolated(t *testing.T) {
	for _, differentBroker := range []bool{false, true} {
		t.Run(fmt.Sprint(differentBroker), func(t *testing.T) {
			firstBroker, secondBroker := newPipeBroker(t), newPipeBroker(t)
			mgr := pipeMqttManager(t, firstBroker)
			mgr.newClient = func(opts *mqtt.ClientOptions) mqtt.Client {
				b := firstBroker
				if opts.ClientID == "second" || opts.Servers[0].Host == "second.invalid:1883" {
					b = secondBroker
				}
				return mqtt.NewClient(opts.SetCustomOpenConnectionFn(b.open))
			}
			firstCfg, secondCfg := testMqttConfig(), testMqttConfig()
			if differentBroker {
				secondCfg.Broker = "tcp://second.invalid:1883"
			} else {
				secondCfg.ClientID = "second"
			}
			first, second := make(chan string, 2), make(chan string, 2)
			watchMqtt(t, mgr, firstCfg, "same/topic", func(_ string, payload []byte) { first <- string(payload) })
			watchMqtt(t, mgr, secondCfg, "same/topic", func(_ string, payload []byte) { second <- string(payload) })
			receiveMqtt(t, firstBroker.subs)
			receiveMqtt(t, secondBroker.subs)
			firstBroker.publish(t, "same/topic", "first")
			secondBroker.publish(t, "same/topic", "second")
			if receiveMqtt(t, first) != "first" || receiveMqtt(t, second) != "second" {
				t.Fatal("cross-connection delivery")
			}
		})
	}
}

func TestMqttRetriesInitialConnectionAndRejectedSubscription(t *testing.T) {
	b := newPipeBroker(t)
	b.failConnect, b.rejectSubscriptions = 2, 1
	mgr := pipeMqttManager(t, b)
	received := make(chan string, 8)
	watchMqtt(t, mgr, testMqttConfig(), "device/value", func(_ string, payload []byte) { received <- string(payload) })
	receiveMqtt(t, b.subs) // SUBACK 0x80 is not exposed by token.Error().
	receiveMqtt(t, b.subs)
	b.publish(t, "device/value", "recovered")
	if receiveMqtt(t, received) != "recovered" {
		t.Fatal("recovery did not deliver")
	}
	time.Sleep(40 * time.Millisecond)
	select {
	case <-b.subs:
		t.Fatal("successful subscription kept accumulating")
	default:
	}
}

func TestMqttReconnectRestoresOnlyActiveFiltersWithoutDuplicateCallbacks(t *testing.T) {
	for _, clean := range []bool{true, false} {
		t.Run(fmt.Sprint(clean), func(t *testing.T) {
			b := newPipeBroker(t)
			mgr := pipeMqttManager(t, b)
			cfg := testMqttConfig()
			cfg.CleanSession = clean
			got := make(chan string, 20)
			watchMqtt(t, mgr, cfg, "device/#", func(_ string, payload []byte) { got <- string(payload) })
			receiveMqtt(t, b.connections)
			receiveMqtt(t, b.subs)
			for i := 0; i < 3; i++ {
				b.close()
				receiveMqtt(t, b.connections)
				receiveMqtt(t, b.subs)
				want := fmt.Sprintf("generation-%d", i)
				b.publish(t, "device/value", want)
				if receiveMqtt(t, got) != want {
					t.Fatal("stale callback was replayed")
				}
				if clients, owners := mqttCounts(mgr); clients != 1 || owners != 1 {
					t.Fatalf("counts = %d/%d", clients, owners)
				}
			}
			select {
			case <-got:
				t.Fatal("duplicate callback after reconnect")
			default:
			}
		})
	}
}

func TestMqttRepeatedReloadRemovesOldFiltersAndOwners(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	base := watchMqtt(t, mgr, testMqttConfig(), "base", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	for i := 0; i < 12; i++ {
		got := make(chan string, 2)
		topic := fmt.Sprintf("reload/%d", i)
		release := watchMqtt(t, mgr, testMqttConfig(), topic, func(_ string, payload []byte) { got <- string(payload) })
		receiveMqtt(t, b.subs)
		b.publish(t, topic, "one")
		if receiveMqtt(t, got) != "one" {
			t.Fatal("missing reload delivery")
		}
		release()
		unsub := receiveMqtt(t, b.unsubs)
		if len(unsub.Topics) != 1 || unsub.Topics[0] != topic {
			t.Fatalf("unsubscribed %v", unsub.Topics)
		}
		mqttEventually(t, func() bool {
			mgr.mu.Lock()
			defer mgr.mu.Unlock()
			entry := mgr.clients[mqttClientKey(testMqttConfig())]
			return entry != nil && len(entry.filters) == 1
		})
		b.publish(t, topic, "late")
		select {
		case <-got:
			t.Fatal("removed filter still delivers")
		default:
		}
	}
	base()
	mqttEventually(t, func() bool { clients, owners := mqttCounts(mgr); return clients == 0 && owners == 0 })
	// A new worker can acquire the same identity after the last owner stopped.
	got := make(chan string, 1)
	watchMqtt(t, mgr, testMqttConfig(), "base", func(_ string, payload []byte) { got <- string(payload) })
	receiveMqtt(t, b.subs)
	b.publish(t, "base", "new")
	if receiveMqtt(t, got) != "new" {
		t.Fatal("new connection lost")
	}
}

func TestMqttStopCancelsPendingSubscriptionAndInitialConnect(t *testing.T) {
	for _, pendingConnect := range []bool{false, true} {
		t.Run(fmt.Sprint(pendingConnect), func(t *testing.T) {
			b := newPipeBroker(t)
			if pendingConnect {
				b.failConnect = 1000
			} else {
				b.holdSubscription = true
			}
			mgr := pipeMqttManager(t, b)
			ctx, cancel := context.WithCancel(context.Background())
			release, err := mgr.Watch(ctx, testMqttConfig(), "pending", 1, func(string, []byte) { t.Error("unexpected delivery") })
			if err != nil {
				t.Fatal(err)
			}
			if !pendingConnect {
				receiveMqtt(t, b.subs)
			}
			cancel()
			release()
			release()
			mqttEventually(t, func() bool { clients, owners := mqttCounts(mgr); return clients == 0 && owners == 0 })
			if err := mgr.Close(); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMqttLastOwnerDoesNotInterruptActivePublish(t *testing.T) {
	b := newPipeBroker(t)
	b.holdPublish = true
	mgr := pipeMqttManager(t, b)
	cfg := testMqttConfig()
	release := watchMqtt(t, mgr, cfg, "read", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	result := make(chan error, 1)
	go func() { result <- mgr.Publish(cfg, "write", []byte("command"), 1) }()
	publication := receiveMqtt(t, b.pubs)
	release()
	mgr.mu.Lock()
	entry := mgr.clients[mqttClientKey(cfg)]
	active := entry != nil && entry.publishes == 1 && entry.ctx.Err() == nil
	mgr.mu.Unlock()
	if !active {
		t.Fatal("last collector interrupted active publish")
	}
	select {
	case err := <-result:
		t.Fatalf("publish returned before PUBACK: %v", err)
	default:
	}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = publication.MessageID
	b.mu.Lock()
	c := b.conn
	b.mu.Unlock()
	if err := b.write(c, ack); err != nil {
		t.Fatal(err)
	}
	if err := receiveMqtt(t, result); err != nil {
		t.Fatal(err)
	}
	mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
}

func TestMqttSubscribeAndWaitDoesNotReplacePermanentOwner(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	got := make(chan string, 4)
	watchMqtt(t, mgr, testMqttConfig(), "read", func(_ string, payload []byte) { got <- string(payload) })
	receiveMqtt(t, b.subs)
	result := make(chan string, 1)
	go func() {
		payload, err := mgr.SubscribeAndWait(testMqttConfig(), "read", 1, time.Second)
		if err != nil {
			result <- err.Error()
		} else {
			result <- string(payload)
		}
	}()
	mqttEventually(t, func() bool { _, owners := mqttCounts(mgr); return owners == 2 })
	b.publish(t, "read", "both")
	if receiveMqtt(t, got) != "both" || receiveMqtt(t, result) != "both" {
		t.Fatal("temporary owner replaced permanent route")
	}
	mqttEventually(t, func() bool { _, owners := mqttCounts(mgr); return owners == 1 })
	b.publish(t, "read", "permanent")
	if receiveMqtt(t, got) != "permanent" {
		t.Fatal("temporary cancellation removed permanent route")
	}
}

func TestMqttNeverConnectedPublishIsRejectedWithoutLateSend(t *testing.T) {
	b := newPipeBroker(t)
	b.failConnect = 1000
	mgr := pipeMqttManager(t, b)
	release := watchMqtt(t, mgr, testMqttConfig(), "read", func(string, []byte) {})
	// Paho reports IsConnected during initial ConnectRetry. It must not make
	// control commands eligible before the connection has ever succeeded.
	mqttEventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return mgr.clients[mqttClientKey(testMqttConfig())].client.IsConnected()
	})
	result := make(chan error, 1)
	go func() { result <- mgr.Publish(testMqttConfig(), "write", []byte("must-not-queue"), 1) }()
	if err := receiveMqtt(t, result); err == nil {
		t.Fatal("publish before initial connection succeeded")
	}
	b.mu.Lock()
	b.failConnect = 0
	b.mu.Unlock()
	receiveMqtt(t, b.subs)
	select {
	case <-b.pubs:
		t.Fatal("rejected command was sent after connection recovered")
	default:
	}
	release()
	mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
}

func TestMqttMissingSubackReconnectsAndRestoresEveryActiveFilter(t *testing.T) {
	b := newPipeBroker(t)
	b.dropSubacks = 1
	mgr := pipeMqttManager(t, b)
	first, second := make(chan string, 2), make(chan string, 2)
	watchMqtt(t, mgr, testMqttConfig(), "first", func(_ string, payload []byte) { first <- string(payload) })
	watchMqtt(t, mgr, testMqttConfig(), "second", func(_ string, payload []byte) { second <- string(payload) })
	receiveMqtt(t, b.connections)
	receiveMqtt(t, b.subs)
	receiveMqtt(t, b.connections)
	receiveMqtt(t, b.subs)
	receiveMqtt(t, b.subs)
	b.publish(t, "first", "a")
	b.publish(t, "second", "b")
	if receiveMqtt(t, first) != "a" || receiveMqtt(t, second) != "b" {
		t.Fatal("active mappings were not restored")
	}
}

func TestMqttMissingSubackDoesNotRestartAnActivePublish(t *testing.T) {
	b := newPipeBroker(t)
	b.holdSubscription, b.holdPublish = true, true
	mgr := pipeMqttManager(t, b)
	release := watchMqtt(t, mgr, testMqttConfig(), "read", func(string, []byte) {})
	receiveMqtt(t, b.connections)
	receiveMqtt(t, b.subs)
	result := make(chan error, 1)
	go func() { result <- mgr.Publish(testMqttConfig(), "write", []byte("command"), 1) }()
	publication := receiveMqtt(t, b.pubs)
	time.Sleep(2 * mgr.operationTimeout)
	select {
	case <-b.connections:
		t.Fatal("subscription timeout interrupted publish")
	case <-b.subs:
		t.Fatal("pending subscription token accumulated")
	case <-result:
		t.Fatal("publish stopped waiting for PUBACK")
	default:
	}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = publication.MessageID
	b.mu.Lock()
	c := b.conn
	b.holdSubscription = false
	b.mu.Unlock()
	if err := b.write(c, ack); err != nil {
		t.Fatal(err)
	}
	if err := receiveMqtt(t, result); err != nil {
		t.Fatal(err)
	}
	receiveMqtt(t, b.connections)
	receiveMqtt(t, b.subs)
	release()
	mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
}

func TestMqttContinuousMissingAcksStillCloseAndReclaimConnections(t *testing.T) {
	b := newPipeBroker(t)
	b.holdSubscription, b.holdUnsubscribe = true, true
	mgr := pipeMqttManager(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release, err := mgr.Watch(ctx, testMqttConfig(), "read", 1, func(string, []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		receiveMqtt(t, b.connections)
		receiveMqtt(t, b.subs)
	}
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}
	release()
	cancel()
	mqttEventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.liveConnections == 0 })
	if clients, owners := mqttCounts(mgr); clients != 0 || owners != 0 {
		t.Fatalf("leaked manager state: %d/%d", clients, owners)
	}
	b.mu.Lock()
	maxLive := b.maxLiveConnections
	b.mu.Unlock()
	if maxLive > 2 {
		t.Fatalf("connections accumulated: peak %d", maxLive)
	}
}

func TestMqttReaddedOwnerSurvivesPendingUnsubscribe(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	watchMqtt(t, mgr, testMqttConfig(), "base", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	old := watchMqtt(t, mgr, testMqttConfig(), "replace", func(string, []byte) { t.Error("old owner called") })
	receiveMqtt(t, b.subs)
	b.mu.Lock()
	b.holdUnsubscribe = true
	b.mu.Unlock()
	old()
	unsubscription := receiveMqtt(t, b.unsubs)
	got := make(chan string, 1)
	watchMqtt(t, mgr, testMqttConfig(), "replace", func(_ string, payload []byte) { got <- string(payload) })
	ack := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	ack.MessageID = unsubscription.MessageID
	b.mu.Lock()
	c := b.conn
	b.holdUnsubscribe = false
	b.mu.Unlock()
	if err := b.write(c, ack); err != nil {
		t.Fatal(err)
	}
	receiveMqtt(t, b.subs)
	b.publish(t, "replace", "new-owner")
	if receiveMqtt(t, got) != "new-owner" {
		t.Fatal("pending unsubscribe removed new owner")
	}
}

func TestMqttPersistentLastOwnerReloadAndCloseRemoveBrokerFilters(t *testing.T) {
	b := newPipeBroker(t)
	mgr := pipeMqttManager(t, b)
	cfg := testMqttConfig()
	cfg.CleanSession = false
	release := watchMqtt(t, mgr, cfg, "old", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	release()
	receiveMqtt(t, b.unsubs)
	mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
	watchMqtt(t, mgr, cfg, "new", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	b.mu.Lock()
	_, oldRemains := b.session["old"]
	_, newExists := b.session["new"]
	b.mu.Unlock()
	if oldRemains || !newExists {
		t.Fatal("persistent session kept the previous worker's filter")
	}
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	remaining := len(b.session)
	b.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("Close left %d broker filters", remaining)
	}
}

// Wrap callbacks as well as methods so Paho exposes the same client identity
// to the manager when a test pauses a public client method.
type mqttMethodGate struct {
	mqtt.Client
	beforeDisconnect func()
	beforeSubscribe  func()
}

func (c *mqttMethodGate) Disconnect(quiesce uint) {
	if c.beforeDisconnect != nil {
		c.beforeDisconnect()
	}
	c.Client.Disconnect(quiesce)
}

func (c *mqttMethodGate) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) mqtt.Token {
	if c.beforeSubscribe != nil {
		c.beforeSubscribe()
	}
	return c.Client.Subscribe(topic, qos, func(_ mqtt.Client, msg mqtt.Message) { handler(c, msg) })
}

func (c *mqttMethodGate) AddRoute(topic string, handler mqtt.MessageHandler) {
	c.Client.AddRoute(topic, func(_ mqtt.Client, msg mqtt.Message) { handler(c, msg) })
}

func newGatedMqttClient(opts *mqtt.ClientOptions, broker *pipeBroker, wrapper *mqttMethodGate) mqtt.Client {
	onConnect := opts.OnConnect
	opts.SetOnConnectHandler(func(mqtt.Client) { onConnect(wrapper) })
	wrapper.Client = mqtt.NewClient(opts.SetCustomOpenConnectionFn(broker.open))
	return wrapper
}

func TestMqttLateOldOnConnectCannotAdmitPublishDuringReplacement(t *testing.T) {
	b := newPipeBroker(t)
	b.dropSubacks = 1
	mgr := pipeMqttManager(t, b)
	disconnecting, allowDisconnect := make(chan struct{}), make(chan struct{})
	var allowOnce, disconnectOnce sync.Once
	unblock := func() { allowOnce.Do(func() { close(allowDisconnect) }) }
	t.Cleanup(unblock)
	oldClient := &mqttMethodGate{beforeDisconnect: func() {
		disconnectOnce.Do(func() { close(disconnecting); <-allowDisconnect })
	}}
	var oldOnConnect mqtt.OnConnectHandler
	created := 0
	mgr.newClient = func(opts *mqtt.ClientOptions) mqtt.Client {
		created++
		if created == 1 {
			oldOnConnect = opts.OnConnect
			return newGatedMqttClient(opts, b, oldClient)
		}
		return mqtt.NewClient(opts.SetCustomOpenConnectionFn(b.open))
	}
	watchMqtt(t, mgr, testMqttConfig(), "read", func(string, []byte) {})
	receiveMqtt(t, b.subs)
	// The old client is still connected: the original replacement ordering
	// had not changed entry.client at this exact pause point.
	receiveMqtt(t, disconnecting)
	mgr.mu.Lock()
	entry := mgr.clients[mqttClientKey(testMqttConfig())]
	generation := entry.generation
	mgr.mu.Unlock()
	oldOnConnect(oldClient)
	mgr.mu.Lock()
	unchanged := entry.restarting && entry.generation == generation && entry.client != oldClient
	mgr.mu.Unlock()
	if !unchanged {
		t.Fatal("old OnConnect changed replacement state")
	}
	result := make(chan error, 1)
	go func() { result <- mgr.Publish(testMqttConfig(), "write", []byte("must-not-queue"), 1) }()
	if err := receiveMqtt(t, result); err == nil {
		t.Fatal("publish entered the replacement window")
	}
	unblock()
	receiveMqtt(t, b.subs)
	select {
	case <-b.pubs:
		t.Fatal("late callback caused a delayed control publish")
	default:
	}
}

func TestMqttFinalUnsubscribeWaitsForEarlierSubscribeMethod(t *testing.T) {
	for _, completeMethod := range []bool{true, false} {
		t.Run(fmt.Sprint(completeMethod), func(t *testing.T) {
			b := newPipeBroker(t)
			mgr := pipeMqttManager(t, b)
			mgr.cleanupTimeout = 100 * time.Millisecond
			entering, allowSubscribe, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var gateOnce, allowOnce sync.Once
			unblock := func() { allowOnce.Do(func() { close(allowSubscribe) }) }
			t.Cleanup(unblock)
			wrapper := &mqttMethodGate{beforeSubscribe: func() {
				gateOnce.Do(func() { close(entering); <-allowSubscribe; close(returned) })
			}}
			mgr.newClient = func(opts *mqtt.ClientOptions) mqtt.Client { return newGatedMqttClient(opts, b, wrapper) }
			cfg := testMqttConfig()
			cfg.CleanSession = false
			release := watchMqtt(t, mgr, cfg, "pending", func(string, []byte) {})
			receiveMqtt(t, entering)
			release()
			if completeMethod {
				// Cancellation must not enqueue the final UNSUBSCRIBE while
				// the earlier Subscribe method has not established send order.
				select {
				case <-b.unsubs:
					t.Fatal("unsubscribe overtook earlier subscribe")
				case <-time.After(20 * time.Millisecond):
				}
				unblock()
				receiveMqtt(t, b.subs)
				receiveMqtt(t, b.unsubs)
				mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
			} else {
				mqttEventually(t, func() bool { clients, _ := mqttCounts(mgr); return clients == 0 })
				select {
				case <-b.unsubs:
					t.Fatal("cleanup ignored unfinished Subscribe")
				default:
				}
				unblock()
			}
			receiveMqtt(t, returned)
			mqttEventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.liveConnections == 0 })
			b.mu.Lock()
			remaining := len(b.session)
			b.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("late subscribe left %d persistent filters", remaining)
			}
		})
	}
}
