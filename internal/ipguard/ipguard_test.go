package ipguard

import (
	"net"
	"testing"
)

func TestForbidden(t *testing.T) {
	// Given 各保留段与公网地址 When 判定 Then 保留段全拒、公网放行
	tests := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},
		{"0.0.0.0", true},
		{"0.1.2.3", true},
		{"10.0.0.1", true},
		{"172.16.0.1", true},
		{"192.168.1.1", true},
		{"169.254.1.1", true},
		{"100.64.0.1", true},
		{"100.127.255.255", true},
		{"100.128.0.1", false},
		{"198.18.0.1", true},
		{"198.19.255.255", true},
		{"198.20.0.1", false},
		{"192.0.0.1", true},
		{"240.0.0.1", true},
		{"255.255.255.255", true},
		{"224.0.0.1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"::1", true},
		{"fe80::1", true},
		{"fc00::1", true},
		{"2001:db8::1", true},
		{"2606:4700::1111", false},
	}
	for _, tt := range tests {
		if got := Forbidden(net.ParseIP(tt.ip)); got != tt.want {
			t.Errorf("Forbidden(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}
