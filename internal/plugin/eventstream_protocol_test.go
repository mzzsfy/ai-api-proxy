package plugin

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/dop251/goja"
)

// 独立向量按 AWS EventStream wire layout 手工核验；CRC 使用 CRC-32/IEEE 多项式结果，非测试帧 helper 生成。
func TestEventStreamDecodeUsesIndependentFixedGoldenFrame(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := []byte{0, 0, 0, 42, 0, 0, 0, 24, 0xfd, 0x3f, 0x70, 0x9a, 5, ':', 'b', 'o', 'o', 'l', 1, 4, ':', 'n', 'u', 'm', 2, 0xff, 4, ':', 's', 't', 'r', 7, 0, 2, 'o', 'k', 0, 0xff, 0xd7, 0xdc, 0xd0, 0x54}
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){const i={next(){return this.sent?{done:true}:(this.sent=true,{done:false,value:frame});},[Symbol.iterator](){return this;}};const e=util.eventstream.decode(i).next().value;return [e.headers[":bool"],e.headers[":num"],e.headers[":str"],Array.from(new Uint8Array(e.payload))];})()`)
	if err != nil {
		t.Fatal(err)
	}
	v := got.Export().([]interface{})
	payload := v[3].([]interface{})
	if v[0] != true || v[1] != int64(-1) || v[2] != "ok" || len(payload) != 2 || payload[0] != int64(0) || payload[1] != int64(255) {
		t.Fatalf("decoded=%#v", v)
	}
}

// Given two frames in one input chunk, when iterated, then payload bytes retain frame order.
func TestEventStreamDecodeConcatenatedFrames(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	first := []byte{0, 0, 0, 18, 0, 0, 0, 0, 0x7f, 2, 0x1b, 0x8b, 'a', '1', 0xe0, 0xd5, 0x96, 0x9a}
	second := []byte{0, 0, 0, 18, 0, 0, 0, 0, 0x7f, 2, 0x1b, 0x8b, 'b', '2', 0x52, 0xf1, 0x94, 0xe3}
	combined := append(first, second...)
	_ = vm.Set("combined", vm.NewArrayBuffer(combined))
	got, err := vm.RunString(`(function(){const i={next(){return this.sent?{done:true}:(this.sent=true,{done:false,value:combined});},[Symbol.iterator](){return this;}};const out=[];for(const e of util.eventstream.decode(i))out.push(Array.from(new Uint8Array(e.payload)));return out;})()`)
	if err != nil {
		t.Fatal(err)
	}
	v := got.Export().([]interface{})
	if len(v) != 2 {
		t.Fatalf("frames=%#v", v)
	}
	for i, want := range [][]int64{{97, 49}, {98, 50}} {
		gotPayload := v[i].([]interface{})
		if len(gotPayload) != len(want) {
			t.Fatalf("payload %d=%#v", i, gotPayload)
		}
		for j, b := range want {
			if gotPayload[j] != b {
				t.Fatalf("payload[%d][%d]=%v want %d", i, j, gotPayload[j], b)
			}
		}
	}
}

// Given malformed UTF-8 in a string header, when decoded, then the frame is rejected.
func TestEventStreamDecodeRejectsInvalidUTF8(t *testing.T) {
	cases := [][]byte{{0xc0, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xe2, 0x82}}
	for i, bad := range cases {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			vm := goja.New()
			bindUtil(vm, HostDeps{})
			h := testEventStreamHeader("x", 7, append([]byte{0, byte(len(bad))}, bad...))
			f := testEventStreamFrameWithHeaders(h, nil)
			_ = vm.Set("frame", vm.NewArrayBuffer(f))
			_, err := vm.RunString(`util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next()`)
			if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
				t.Fatalf("bad=%x err=%v", bad, err)
			}
		})
	}
}

// Given each supported value encoding, when decoded, then values match the wire representation.
func TestEventStreamDecodeHeadersWireTypes(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	h := []byte{}
	h = append(h, testEventStreamHeader("b", 0, nil)...)
	h = append(h, testEventStreamHeader("B", 1, nil)...)
	h = append(h, testEventStreamHeader("i", 2, []byte{0x80})...)
	h = append(h, testEventStreamHeader("s", 3, []byte{0xff, 0xfe})...)
	h = append(h, testEventStreamHeader("l", 4, []byte{0xff, 0xff, 0xff, 0xff})...)
	h = append(h, testEventStreamHeader("q", 5, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})...)
	h = append(h, testEventStreamHeader("a", 6, []byte{0, 2, 0, 0xff})...)
	h = append(h, testEventStreamHeader("t", 7, []byte{0, 2, 'o', 'k'})...)
	h = append(h, testEventStreamHeader("m", 8, []byte{0x80, 0, 0, 0, 0, 0, 0, 0})...)
	h = append(h, testEventStreamHeader("u", 9, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})...)
	f := testEventStreamFrameWithHeaders(h, nil)
	_ = vm.Set("frame", vm.NewArrayBuffer(f))
	got, err := vm.RunString(`(function(){const e=util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next().value;return [e.headers.b,e.headers.B,e.headers.i,e.headers.s,e.headers.l,e.headers.q.toString(),Array.from(new Uint8Array(e.headers.a)),e.headers.t,e.headers.m.toString(),Array.from(new Uint8Array(e.headers.u))];})()`)
	if err != nil {
		t.Fatal(err)
	}
	v := got.Export().([]interface{})
	a, u := v[6].([]interface{}), v[9].([]interface{})
	if v[0] != false || v[1] != true || v[2] != int64(-128) || v[3] != int64(-2) || v[4] != int64(-1) || v[5] != "-1" || len(a) != 2 || a[0] != int64(0) || a[1] != int64(255) || v[7] != "ok" || v[8] != "9223372036854775808" || len(u) != 16 {
		t.Fatalf("headers=%#v", v)
	}
	for i, gotByte := range u {
		if gotByte != int64(i) {
			t.Fatalf("uuid[%d]=%v want %d", i, gotByte, i)
		}
	}
}

// Given a duplicated header name, when decoded, then an error identifies the duplicate.
func TestEventStreamDecodeRejectsDuplicateHeaders(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	h := append(testEventStreamHeader("x", 1, nil), testEventStreamHeader("x", 1, nil)...)
	f := testEventStreamFrameWithHeaders(h, nil)
	_ = vm.Set("frame", vm.NewArrayBuffer(f))
	_, err := vm.RunString(`util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next()`)
	if err == nil || !strings.Contains(err.Error(), "duplicate EventStream header") {
		t.Fatalf("error=%v", err)
	}
}

func TestEventStreamDecodeRejectsOutOfRangeHeaderLengths(t *testing.T) {
	for _, length := range []uint16{0, 32768} {
		t.Run(map[uint16]string{0: "empty", 32768: "over-limit"}[length], func(t *testing.T) {
			vm := goja.New()
			bindUtil(vm, HostDeps{})
			h := testEventStreamHeader("x", 7, []byte{byte(length >> 8), byte(length)})
			f := testEventStreamFrameWithHeaders(h, nil)
			_ = vm.Set("frame", vm.NewArrayBuffer(f))
			_, err := vm.RunString(`util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next()`)
			if err == nil {
				t.Fatalf("length %d accepted", length)
			}
		})
	}
}

func TestEventStreamDecodeAcceptsMaximumHeaderString(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	want := strings.Repeat("x", 32767)
	value := append([]byte{0x7f, 0xff}, []byte(want)...)
	header := testEventStreamHeader("x", 7, value)
	frame := testEventStreamFrameWithHeaders(header, nil)
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){return util.eventstream.decode({next(){return {done:false,value:frame};},[Symbol.iterator](){return this;}}).next().value.headers.x;})()`)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != want {
		t.Fatalf("decoded string differs from the %d-byte expected value", len(want))
	}
}

func testEventStreamHeader(name string, typ byte, value []byte) []byte {
	h := make([]byte, 1+len(name)+1+len(value))
	h[0] = byte(len(name))
	copy(h[1:], name)
	h[1+len(name)] = typ
	copy(h[2+len(name):], value)
	return h
}

func testEventStreamFrameWithHeaders(headers, payload []byte) []byte {
	f := make([]byte, 12+len(headers)+len(payload)+4)
	binary.BigEndian.PutUint32(f[:4], uint32(len(f)))
	binary.BigEndian.PutUint32(f[4:8], uint32(len(headers)))
	binary.BigEndian.PutUint32(f[8:12], crc32.ChecksumIEEE(f[:8]))
	copy(f[12:], headers)
	copy(f[12+len(headers):], payload)
	binary.BigEndian.PutUint32(f[len(f)-4:], crc32.ChecksumIEEE(f[:len(f)-4]))
	return f
}
