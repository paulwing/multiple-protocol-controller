//go:build linux

package conn

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// 这组用例必须运行在 Linux 上：它们直接使用 TCP_INFO/POLLRDHUP 读取真实内核状态，
// 不能用注入的 inspectSocket 代替。主控以 linux/amd64 测试二进制在隔离容器中执行。

// 对端仍有未读数据但连接健康时，只能报告健康，且数据必须保持可读（未被探测消费）。
func TestInspectSocketPlatformEstablishedWithUnreadDataStaysHealthy(t *testing.T) {
	server, client := tcpPair(t)
	defer server.Close()
	defer client.Close()

	if _, err := server.Write([]byte("keep")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	waitForPollEvent(t, client, unix.POLLIN)

	verdict, err := inspectSocketPlatform(client)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if verdict != socketHealthy {
		t.Fatalf("verdict = %v, want healthy (unread data is not a disconnect)", verdict)
	}

	// 探测不得消费业务数据。
	buf := make([]byte, 4)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read after inspect: %v", err)
	}
	if string(buf) != "keep" {
		t.Fatalf("read after inspect = %q, want %q", buf, "keep")
	}
}

// 对端发送少量未读数据后 FIN（半关闭）：
// MSG_PEEK 式的“有数据即健康”会长期掩盖断线，内核状态必须直接判定断开，
// 同时未读数据仍留在接收队列中，证明探测没有消费业务响应。
func TestInspectSocketPlatformDetectsFINWithUnreadDataWithoutConsuming(t *testing.T) {
	server, client := tcpPair(t)
	defer client.Close()

	const payload = "tail"
	if _, err := server.Write([]byte(payload)); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if err := server.CloseWrite(); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	defer server.Close()

	verdict := waitForVerdict(t, client, socketDead)

	// 判定后数据仍在，说明 SO_ERROR/TCP_INFO/poll 探测没有读取接收队列。
	buf := make([]byte, len(payload))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := io.ReadFull(client, buf)
	if err != nil {
		t.Fatalf("read after half-close verdict %v: %v", verdict, err)
	}
	if string(buf[:n]) != payload {
		t.Fatalf("read after half-close = %q, want %q", buf[:n], payload)
	}
	if _, err := client.Read(buf[:1]); !errors.Is(err, io.EOF) {
		t.Fatalf("read after payload error = %v, want EOF", err)
	}
}

// 对端 RST 必须判定断开，不能因为当前 TCP_INFO 尚未更新就无限报在线。
func TestInspectSocketPlatformDetectsReset(t *testing.T) {
	server, client := tcpPair(t)
	defer client.Close()

	if err := server.SetLinger(0); err != nil {
		t.Fatalf("SetLinger: %v", err)
	}
	if _, err := server.Write([]byte("reset")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}

	waitForVerdict(t, client, socketDead)
}

// classifyTCPState 用 Linux UAPI 状态值验证分类，防止常量写错后真实用例偶然通过。
func TestClassifyTCPState(t *testing.T) {
	cases := []struct {
		state uint8
		want  socketVerdict
	}{
		{tcpStateEstablished, socketHealthy},
		{tcpStateCloseWait, socketDead},
		{tcpStateClosing, socketDead},
		{tcpStateLastAck, socketDead},
		{tcpStateTimeWait, socketDead},
		{tcpStateClose, socketDead},
		{tcpStateClosed, socketDead},
		{tcpStateSynSent, socketUnknown},
		{tcpStateSynRecv, socketUnknown},
	}
	for _, tc := range cases {
		if got := classifyTCPState(tc.state); got != tc.want {
			t.Fatalf("classifyTCPState(%d) = %v, want %v", tc.state, got, tc.want)
		}
	}
}

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan *net.TCPConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	dialed, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	select {
	case server := <-accepted:
		return server, dialed
	case err := <-acceptErr:
		_ = dialed.Close()
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		_ = dialed.Close()
		t.Fatalf("timed out waiting for accept")
	}
	return nil, nil
}

// waitForVerdict 在有界时间内等待期望的内核判定；用于吸收本机 TCP 状态传播的短暂延迟。
func waitForVerdict(t *testing.T, conn net.Conn, want socketVerdict) socketVerdict {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last socketVerdict
	for time.Now().Before(deadline) {
		verdict, err := inspectSocketPlatform(conn)
		if err == nil {
			last = verdict
			if verdict == want {
				return verdict
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for verdict %v, last = %v", want, last)
	return socketUnknown
}

func waitForPollEvent(t *testing.T, conn net.Conn, events int16) {
	t.Helper()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("conn is %T, want *net.TCPConn", conn)
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var ready bool
		var pollErr error
		if err := raw.Control(func(fd uintptr) {
			fds := []unix.PollFd{{Fd: int32(fd), Events: events}}
			if _, err := unix.Poll(fds, 0); err != nil {
				pollErr = err
				return
			}
			ready = fds[0].Revents&events != 0
		}); err != nil {
			t.Fatalf("Control: %v", err)
		}
		if pollErr != nil {
			t.Fatalf("poll: %v", pollErr)
		}
		if ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for poll events %#x", events)
}
