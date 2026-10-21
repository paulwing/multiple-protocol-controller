//go:build !linux

package conn

import "net"

// 非 Linux 平台没有可用的 TCP_INFO/POLLRDHUP 内核状态读取实现。
// 返回 unknown 时调用方不得续期在线状态；只有端点没有池内连接时的拨号结果可作依据。
// 生产部署以 Linux 容器为准（主控已确认为 linux/aarch64），
// macOS 开发环境显式 Unknown 是本轮约定的平台边界，不为测试扩展 Darwin 实现。
// 真实 socket 用例在 Linux 容器中执行，见 socket_health_linux_test.go。
func inspectSocketPlatform(net.Conn) (socketVerdict, error) {
	return socketUnknown, nil
}
