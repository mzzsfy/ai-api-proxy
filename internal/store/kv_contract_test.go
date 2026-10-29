package store

import (
	"context"
	"testing"
)

func TestKV_OverwritePersistsAfterReopen(t *testing.T) {
	// Given 已存同名键且另一命名空间有值 When 覆盖并重开 Then 新值持久保存且不影响另一空间
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ ns, value string }{{"a", "old"}, {"b", "isolated"}, {"a", "new"}} {
		if err := s.KVSet(ctx, item.ns, "token", item.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	for _, item := range []struct{ ns, value string }{{"a", "new"}, {"b", "isolated"}} {
		value, ok, err := reopened.KVGet(ctx, item.ns, "token")
		if err != nil || !ok || value != item.value {
			t.Fatalf("%s = %q/%v/%v; want %q", item.ns, value, ok, err, item.value)
		}
	}
}
