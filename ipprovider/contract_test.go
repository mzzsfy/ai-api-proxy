package ipprovider

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

// mockProvider/mockLease 契约语义测试驱动;同时是契约行为的可执行规范

type mockProvider struct {
	mu       sync.Mutex
	caps     Capabilities
	leases   []*mockLease
	acquires int
	closed   bool
	noExits  bool
}

func (m *mockProvider) Capabilities() Capabilities { return m.caps }

func (m *mockProvider) Acquire(ctx context.Context, hint Hint) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquires++
	if m.closed {
		return nil, errors.New("provider closed")
	}
	if m.noExits {
		return nil, ErrNoExits
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	l := &mockLease{p: m, caps: m.caps}
	m.leases = append(m.leases, l)
	return l, nil
}

func (m *mockProvider) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Stats{Total: len(m.leases)}
	for _, l := range m.leases {
		if l.released {
			s.Draining++
		} else {
			s.Normal++
		}
		if l.egress != "" {
			s.EgressIPs = append(s.EgressIPs, l.egress)
		}
	}
	return s
}

func (m *mockProvider) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

type mockLease struct {
	p        *mockProvider
	caps     Capabilities
	egress   string
	released bool
	dials    int
}

func (l *mockLease) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if l.released {
		return nil, ErrLeaseReleased
	}
	l.dials++
	return nil, errors.New("mock dial: 由具体用例注入")
}

func (l *mockLease) EgressIP() string { return l.egress }

func (l *mockLease) Release() {
	if l.released {
		return // 幂等
	}
	l.released = true
}

func (l *mockLease) Capabilities() Capabilities { return l.caps }

// 场景:Acquire 错误路径 —— 无可用出口映射 ErrNoExits
func TestAcquire_无可用出口(t *testing.T) {
	p := &mockProvider{noExits: true}
	_, err := p.Acquire(context.Background(), Hint{})
	if !errors.Is(err, ErrNoExits) {
		t.Fatalf("want ErrNoExits, got %v", err)
	}
}

// 场景:Acquire 错误路径 —— ctx 取消透传
func TestAcquire_ctx取消(t *testing.T) {
	p := &mockProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Acquire(ctx, Hint{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// 场景:Release 幂等;Release 后 Dial 返回 ErrLeaseReleased
func TestLease_Release语义(t *testing.T) {
	p := &mockProvider{}
	l, err := p.Acquire(context.Background(), Hint{})
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	l.Release() // 幂等
	if _, err := l.Dial(context.Background(), "tcp", "example.com:80"); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("want ErrLeaseReleased, got %v", err)
	}
}

// 场景:能力声明构造后固定
func TestProvider_能力声明固定(t *testing.T) {
	p := &mockProvider{caps: Capabilities{CanRotateIP: true, SessionAffinity: true}}
	c1 := p.Capabilities()
	l, _ := p.Acquire(context.Background(), Hint{})
	c2 := l.Capabilities()
	if c1 != c2 {
		t.Fatalf("lease caps must inherit provider caps: %v vs %v", c1, c2)
	}
}

// 场景:registry 注册与 Create;重复注册 panic;未注册返回 ErrUnknownKind
func TestRegistry_Create与错误(t *testing.T) {
	t.Cleanup(func() { registryMu.Lock(); delete(registry, "mock-x"); registryMu.Unlock() })
	Register("mock-x", func(cfg ProviderCfg) (Provider, error) { return &mockProvider{}, nil })
	p, err := Create("mock-x", ProviderCfg{Name: "t"})
	if err != nil || p == nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := Create("mock-missing", ProviderCfg{}); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("want ErrUnknownKind, got %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate register must panic")
		}
	}()
	Register("mock-x", func(cfg ProviderCfg) (Provider, error) { return nil, nil })
}
