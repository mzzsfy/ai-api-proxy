package plugin

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

// 独立向量按 AWS EventStream wire layout 手工核验；CRC 使用 CRC-32/IEEE 多项式结果，非测试帧 helper 生成。
// chunk 通道 = JSON 数组文本 → Uint8Array(quickjs 桥无 ArrayBuffer;decode 兼容 view)
func mustUtilVM(t *testing.T) *jsengine.Instance {
	t.Helper()
	rt, err := newQJSRuntime(HostDeps{}, jsengine.Option{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt.inst
}

func setBytes(t *testing.T, inst *jsengine.Instance, name string, data []byte) {
	t.Helper()
	if err := inst.EvalString("globalThis." + name + " = " + jsBytesExpr(data)); err != nil {
		t.Fatal(err)
	}
}

// jsBytesExpr 字节字面量(数字数组文本 → Uint8Array;json.Marshal 对 []byte 走 base64,不可用)
func jsBytesExpr(data []byte) string {
	var sb strings.Builder
	sb.WriteString("(function(){ return Uint8Array.from([")
	for i, b := range data {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(int(b)))
	}
	sb.WriteString("]); })()")
	return sb.String()
}

// decodeIter 一次性迭代器 JS 表达式(单 chunk)
func decodeIter(exprVar string) string {
	return `(function(){var sent=false;const i={next(){if(sent)return{done:true};sent=true;return{done:false,value:` + exprVar + `}},[Symbol.iterator](){return this;}};return i;})()`
}

func TestEventStreamDecodeUsesIndependentFixedGoldenFrame(t *testing.T) {
	inst := mustUtilVM(t)
	frame := []byte{0, 0, 0, 42, 0, 0, 0, 24, 0xfd, 0x3f, 0x70, 0x9a, 5, ':', 'b', 'o', 'o', 'l', 1, 4, ':', 'n', 'u', 'm', 2, 0xff, 4, ':', 's', 't', 'r', 7, 0, 2, 'o', 'k', 0, 0xff, 0xd7, 0xdc, 0xd0, 0x54}
	setBytes(t, inst, "frame", frame)
	v, err := inst.EvalExpression(`(function(){const e=util.eventstream.decode(` + decodeIter("frame") + `).next().value;return [e.headers[":bool"],e.headers[":num"],e.headers[":str"],Array.from(new Uint8Array(e.payload))];})()`)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Free()
	out, err := v.JSON()
	if err != nil {
		t.Fatal(err)
	}
	// [":bool"=true,":num"=-1,":str"="ok",payload=[0,255]]
	want := `[true,-1,"ok",[0,255]]`
	if strings.TrimSpace(string(out)) != want {
		t.Fatalf("decoded=%s", out)
	}
}

// Given two frames in one input chunk, when iterated, then payload bytes retain frame order.
func TestEventStreamDecodeConcatenatedFrames(t *testing.T) {
	inst := mustUtilVM(t)
	first := []byte{0, 0, 0, 18, 0, 0, 0, 0, 0x7f, 2, 0x1b, 0x8b, 'a', '1', 0xe0, 0xd5, 0x96, 0x9a}
	second := []byte{0, 0, 0, 18, 0, 0, 0, 0, 0x7f, 2, 0x1b, 0x8b, 'b', '2', 0x52, 0xf1, 0x94, 0xe3}
	setBytes(t, inst, "combined", append(first, second...))
	v, err := inst.EvalExpression(`(function(){const out=[];for(const e of util.eventstream.decode(` + decodeIter("combined") + `))out.push(Array.from(new Uint8Array(e.payload)));return out;})()`)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Free()
	out, err := v.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(out)) != "[[97,49],[98,50]]" {
		t.Fatalf("frames=%s", out)
	}
}

// Given malformed UTF-8 in a string header, when decoded, then the frame is rejected.
func TestEventStreamDecodeRejectsInvalidUTF8(t *testing.T) {
	cases := [][]byte{{0xc0, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xe2, 0x82}}
	for i, bad := range cases {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			inst := mustUtilVM(t)
			h := testEventStreamHeader("x", 7, append([]byte{0, byte(len(bad))}, bad...))
			f := testEventStreamFrameWithHeaders(h, nil)
			setBytes(t, inst, "frame", f)
			_, err := inst.EvalExpression(`util.eventstream.decode(` + decodeIter("frame") + `).next()`)
			if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
				t.Fatalf("bad=%x err=%v", bad, err)
			}
		})
	}
}

// Given each supported value encoding, when decoded, then values match the wire representation.
func TestEventStreamDecodeHeadersWireTypes(t *testing.T) {
	inst := mustUtilVM(t)
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
	setBytes(t, inst, "frame", f)
	v, err := inst.EvalExpression(`(function(){const e=util.eventstream.decode(` + decodeIter("frame") + `).next().value;return [e.headers.b,e.headers.B,e.headers.i,e.headers.s,e.headers.l,String(e.headers.q),Array.from(new Uint8Array(e.headers.a)),e.headers.t,String(e.headers.m),Array.from(new Uint8Array(e.headers.u))];})()`)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Free()
	out, err := v.JSON()
	if err != nil {
		t.Fatal(err)
	}
	want := `[false,true,-128,-2,-1,"-1",[0,255],"ok","9223372036854775808",[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]]`
	if strings.TrimSpace(string(out)) != want {
		t.Fatalf("headers=%s", out)
	}
}

// Given a duplicated header name, when decoded, then an error identifies the duplicate.
func TestEventStreamDecodeRejectsDuplicateHeaders(t *testing.T) {
	inst := mustUtilVM(t)
	h := append(testEventStreamHeader("x", 1, nil), testEventStreamHeader("x", 1, nil)...)
	f := testEventStreamFrameWithHeaders(h, nil)
	setBytes(t, inst, "frame", f)
	_, err := inst.EvalExpression(`util.eventstream.decode(` + decodeIter("frame") + `).next()`)
	if err == nil || !strings.Contains(err.Error(), "duplicate EventStream header") {
		t.Fatalf("error=%v", err)
	}
}

func TestEventStreamDecodeRejectsOutOfRangeHeaderLengths(t *testing.T) {
	for _, length := range []uint16{0, 32768} {
		t.Run(map[uint16]string{0: "empty", 32768: "over-limit"}[length], func(t *testing.T) {
			inst := mustUtilVM(t)
			h := testEventStreamHeader("x", 7, []byte{byte(length >> 8), byte(length)})
			f := testEventStreamFrameWithHeaders(h, nil)
			setBytes(t, inst, "frame", f)
			if _, err := inst.EvalExpression(`util.eventstream.decode(` + decodeIter("frame") + `).next()`); err == nil {
				t.Fatalf("length %d accepted", length)
			}
		})
	}
}

func TestEventStreamDecodeAcceptsMaximumHeaderString(t *testing.T) {
	inst := mustUtilVM(t)
	want := strings.Repeat("x", 32767)
	value := append([]byte{0x7f, 0xff}, []byte(want)...)
	header := testEventStreamHeader("x", 7, value)
	frame := testEventStreamFrameWithHeaders(header, nil)
	setBytes(t, inst, "frame", frame)
	v, err := inst.EvalExpression(`util.eventstream.decode(` + decodeIter("frame") + `).next().value.headers.x`)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Free()
	if got := v.String(); got != want {
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
