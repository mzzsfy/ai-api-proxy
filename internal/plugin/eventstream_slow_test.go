//go:build slow

package plugin

import (
	"testing"

	"github.com/dop251/goja"
)

// Given a valid payload above the server policy threshold, when decoded, then its full length is returned.
func TestEventStreamDecodeAcceptsPayloadAboveServerPolicy(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	payload := make([]byte, 24*1024*1024+1)
	frame := testEventStreamFrameWithHeaders(nil, payload)
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){const e=util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next().value;return e.payload.byteLength;})()`)
	if err != nil {
		t.Fatal(err)
	}
	if got.ToInteger() != int64(len(payload)) {
		t.Fatalf("payload length=%d", got.ToInteger())
	}
}
