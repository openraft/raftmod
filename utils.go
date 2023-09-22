/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"github.com/pkg/errors"
	"net"
	"os"
	"strconv"
	"strings"
)

type EmptyAddr struct {
}

func (t EmptyAddr) Network() string {
	return ""
}

func (t EmptyAddr) String() string {
	return ""
}

func panicToError(err *error) {
	if r := recover(); r != nil {
		switch v := r.(type) {
		case error:
			*err = v
		case string:
			*err = errors.New(v)
		default:
			*err = errors.Errorf("%v", v)
		}
	}
}

func getPortNumber(addr string) (int, error) {
	hostAndPort := strings.Split(addr, ":")
	end := hostAndPort[len(hostAndPort)-1]
	return strconv.Atoi(end)
}

func getHostAndPortNumber(addr string) (string, int, error) {
	hostAndPort := strings.Split(addr, ":")
	if len(hostAndPort) != 2 {
		return "", 0, errors.Errorf("invalid address '%s'", addr)
	}
	sport := hostAndPort[len(hostAndPort)-1]
	port, err := strconv.Atoi(sport)
	return hostAndPort[0], port, err
}

func createDirIfNeeded(dir string, perm os.FileMode) error {
	if _, err := os.Stat(dir); err != nil {
		if err = os.Mkdir(dir, perm); err != nil {
			return errors.Errorf("unable to create dir '%s' with permissions %x, %v", dir, perm ,err)
		}
		if err = os.Chmod(dir, perm); err != nil {
			return errors.Errorf("unable to chmod dir '%s' with permissions %x, %v", dir, perm ,err)
		}
	}
	return nil
}


// LocalIP get the host machine local IP address
func LocalIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if isPrivateIP(ip) {
				return ip, nil
			}
		}
	}

	return nil, errors.New("no IP")
}

func isPrivateIP(ip net.IP) bool {
	var privateIPBlocks []*net.IPNet
	for _, cidr := range []string{
		// don't check loopback ips
		//"127.0.0.0/8",    // IPv4 loopback
		//"::1/128",        // IPv6 loopback
		//"fe80::/10",      // IPv6 link-local
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
	} {
		_, block, _ := net.ParseCIDR(cidr)
		privateIPBlocks = append(privateIPBlocks, block)
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}

	return false
}

func GetIP(addr net.Addr) []byte {
	switch a := addr.(type) {
	case *net.UDPAddr:
		return []byte(a.IP.String())
	case *net.TCPAddr:
		return []byte(a.IP.String())
	}
	return []byte{}
}

func addLocalIP(addr string) string {
	parts := strings.Split(addr, ":")
	if parts[0] == "" {
		ipAddr, err := LocalIP()
		if err == nil {
			parts[0] = ipAddr.String()
			return strings.Join(parts, ":")
		}
	}
	return addr
}

func ReplaceToLanIP(addr string) string {
	parts := strings.Split(addr, ":")
	if parts[0] == "" || parts[0] == "0.0.0.0" || parts[0] == "127.0.0.1" {
		ipAddr, err := LocalIP()
		if err == nil {
			parts[0] = ipAddr.String()
			return strings.Join(parts, ":")
		}
	}
	return addr
}

func AdjustPortNumberInAddress(addr string, seq int) (result string, err error) {
	if seq == 0 {
		return addr, nil
	}
	parts := strings.Split(addr, ":")
	if len(parts) > 0 {
		lastIndex := len(parts)-1
		parts[lastIndex], err = AdjustPortNumber(parts[lastIndex], seq)
		if err != nil {
			return
		}
		return strings.Join(parts, ":"), nil
	}
	return addr, nil
}

func AdjustPortNumber(port string, seq int) (string, error) {
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return "", errors.Errorf("invalid port number string '%s', %v", port, err)
	}
	if portNum == 0 {
		// do not adjust zero port number, because it is the any one
		return port, nil
	}
	return strconv.Itoa(portNum + seq), nil
}

