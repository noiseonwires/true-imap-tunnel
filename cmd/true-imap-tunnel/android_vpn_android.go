//go:build android

package main

import (
	"fmt"
	"net"
	"syscall"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/netprotect"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
)

const defaultAndroidProtectPath = "./protect_path"

func configureSIP003AndroidVPNProtection(rawOptions string) error {
	opts := parsePluginOptions(rawOptions)
	if _, ok := opts["__android_vpn"]; !ok {
		return nil
	}
	path := optionString(opts, "android_protect_path", defaultAndroidProtectPath)
	netprotect.Install(androidVPNProtectControl(path))
	tlog.Infof("android vpn socket protection enabled path=%s", path)
	return nil
}

func androidVPNProtectControl(path string) netprotect.ControlFunc {
	return func(_, _ string, c syscall.RawConn) error {
		var controlErr error
		if err := c.Control(func(fd uintptr) {
			controlErr = protectAndroidSocket(path, int(fd))
		}); err != nil {
			return err
		}
		return controlErr
	}
}

func protectAndroidSocket(path string, fd int) error {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return fmt.Errorf("connect android protect socket %s: %w", path, err)
	}
	defer conn.Close()

	oob := syscall.UnixRights(fd)
	if _, _, err := conn.WriteMsgUnix([]byte{0}, oob, nil); err != nil {
		return fmt.Errorf("send fd to android protect socket: %w", err)
	}

	var resp [1]byte
	n, err := conn.Read(resp[:])
	if err != nil {
		return fmt.Errorf("read android protect response: %w", err)
	}
	if n != 1 || resp[0] != 0 {
		return fmt.Errorf("android protect rejected fd")
	}
	return nil
}
