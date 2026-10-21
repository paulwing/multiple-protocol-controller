//go:build linux

package conn

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

// inspectSocketPlatform 只读取内核 socket 元数据，不读取、不消费接收队列中的数据。
//
// 判定依据：
//   - poll(POLLRDHUP/POLLERR/POLLHUP)：对端半关闭即使仍有未读数据也会被报告；
//   - TCP_INFO 状态：只有 ESTABLISHED 才算健康，CLOSE_WAIT 等状态判为断开。
//
// 这里刻意不读取 SO_ERROR：读取会清除 pending socket error，
// 可能改变并行业务 I/O 看到的错误。poll 与 TCP_INFO 已足够提供同一信息。
func inspectSocketPlatform(conn net.Conn) (socketVerdict, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return socketUnknown, nil
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return socketUnknown, err
	}

	verdict := socketUnknown
	var opErr error
	controlErr := raw.Control(func(fd uintptr) {
		fds := []unix.PollFd{{
			Fd:     int32(fd),
			Events: unix.POLLIN | unix.POLLRDHUP | unix.POLLERR | unix.POLLHUP,
		}}
		if _, err := unix.Poll(fds, 0); err != nil {
			if !errors.Is(err, unix.EINTR) {
				opErr = err
			}
			return
		}
		// POLLIN 只说明有未读数据，不能据此消费或判定健康；
		// 只有 ERR/HUP/RDHUP 才是连接层面的断开证据。
		if fds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLRDHUP) != 0 {
			verdict = socketDead
			return
		}

		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err != nil {
			opErr = err
			return
		}
		verdict = classifyTCPState(info.State)
	})
	if controlErr != nil {
		return socketUnknown, controlErr
	}
	if opErr != nil {
		return socketUnknown, opErr
	}
	return verdict, nil
}
