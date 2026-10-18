package plugin

import (
	"testing"
	"time"
)

func TestSetPoolConfig(t *testing.T) {
	// Given 注入池配置 When 读常驻上限与等待窗 Then 覆盖缺省;非正值忽略
	origResident := poolSize()
	origWait := poolWait()
	t.Cleanup(func() { SetPoolConfig(origResident, origWait) })

	SetPoolConfig(7, 25*time.Millisecond)
	if got := poolSize(); got != 7 {
		t.Fatalf("resident: %d", got)
	}
	if got := poolWait(); got != 25*time.Millisecond {
		t.Fatalf("wait: %v", got)
	}
	SetPoolConfig(0, 0)
	if got := poolSize(); got != 7 {
		t.Fatalf("non-positive must keep: %d", got)
	}
	if got := poolWait(); got != 25*time.Millisecond {
		t.Fatalf("non-positive must keep: %v", got)
	}
}

func TestPoolWaitConfiguresBorrowWindow(t *testing.T) {
	// Given 等待窗调大 When 窗内归还 Then 复用(窗覆盖生效于借用路径)
	origWait := poolWait()
	t.Cleanup(func() { SetPoolConfig(0, origWait) })
	SetPoolConfig(0, 200*time.Millisecond)

	var builds int
	p := newRuntimePool("t", 1, 50*time.Millisecond, func() (*hookInstance, error) {
		builds++
		return &hookInstance{hooks: &Hooks{}}, nil
	})
	t.Cleanup(p.Close)
	inst1, err := p.Borrow()
	if err != nil {
		t.Fatal(err)
	}
	if builds != 1 {
		t.Fatalf("first borrow builds: %d", builds)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		p.Return(inst1, false)
	}()
	inst2, err := p.Borrow()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Return(inst2, false)
	if inst2 != inst1 || builds != 1 {
		t.Fatalf("window reuse expected, builds=%d", builds)
	}
}
