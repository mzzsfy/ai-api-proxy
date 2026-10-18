package plugin

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin/jsengine"
)

func testEventStreamFrame(payload []byte) []byte {
	frame := make([]byte, 12+len(payload)+4)
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(frame)))
	binary.BigEndian.PutUint32(frame[4:8], 0)
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], payload)
	binary.BigEndian.PutUint32(frame[len(frame)-4:], crc32.ChecksumIEEE(frame[:len(frame)-4]))
	return frame
}

// runJSAssert 执行断言脚本,取全局 __r 字符串结果
func runJSAssert(t *testing.T, inst *jsengine.Instance, js string) string {
	t.Helper()
	if err := inst.EvalString(js); err != nil {
		t.Fatalf("run: %v", err)
	}
	v, g := inst.GetGlobal("__r")
	if g != nil {
		t.Fatalf("assert: %v", g)
	}
	defer v.Free()
	s, _ := v.Any()
	out, _ := s.(string)
	return out
}

func TestEventStreamDecodeRejectsBadPreludeCRCBeforeWaitingForFrame(t *testing.T) {
	inst := mustUtilVM(t)
	frame := testEventStreamFrame(nil)
	binary.BigEndian.PutUint32(frame[0:4], ^uint32(0))
	frame[8] ^= 0x01
	setBytes(t, inst, "prelude", frame[:12])
	runJSAssert(t, inst, `(function(){
	let returned = false;
	const input = { next(){ return {value:prelude,done:false}; }, return(){ returned=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (!String(e).includes("prelude CRC")) throw e; }
	if (!returned) throw new Error("input was not returned");
	globalThis.__r = "ok";
})()`)
}

func TestEventStreamDecodeKeepsParseErrorWhenReturnFails(t *testing.T) {
	inst := mustUtilVM(t)
	frame := testEventStreamFrame(nil)
	frame[len(frame)-1] ^= 0x01
	setBytes(t, inst, "frame", frame)
	got := runJSAssert(t, inst, `(function(){
	const input = { next(){ return {value:frame,done:false}; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { globalThis.__r = String(e); return; }
	globalThis.__r = "no error";
})()`)
	if !strings.Contains(got, "message CRC") || strings.Contains(got, "close failed") {
		t.Fatalf("error = %s", got)
	}
}

func TestEventStreamDecodePropagatesReturnErrorWithoutParseError(t *testing.T) {
	inst := mustUtilVM(t)
	frame := testEventStreamFrame([]byte("payload"))
	setBytes(t, inst, "frame", frame)
	if err := inst.EvalString(`(function(){
	const input = { next(){ return {value:frame,done:false}; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	const decoder = util.eventstream.decode(input);
	decoder.next();
	decoder.return();
})()`); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestEventStreamDecodePreservesThrownUndefined(t *testing.T) {
	inst := mustUtilVM(t)
	got := runJSAssert(t, inst, `(function(){
	const input = { next(){ throw undefined; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (e !== undefined) throw new Error("primary error replaced"); globalThis.__r = "true"; return; }
	globalThis.__r = "false";
})()`)
	if got != "true" {
		t.Fatalf("undefined primary error was lost: %s", got)
	}
}

func TestEventStreamDecodeReturnBeforeFirstNextClosesInput(t *testing.T) {
	inst := mustUtilVM(t)
	got := runJSAssert(t, inst, `(function(){
	let returns=0;
	const input={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	const decoder=util.eventstream.decode(input);
	decoder.return();
	globalThis.__r = String(returns);
})()`)
	if got != "1" {
		t.Fatalf("input return count=%s; want one return", got)
	}
}

func TestEventStreamDecodeThrowBeforeFirstNextClosesInputOnce(t *testing.T) {
	inst := mustUtilVM(t)
	got := runJSAssert(t, inst, `(function(){
	let returns=0;
	const input={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	const decoder=util.eventstream.decode(input);
	try { decoder.throw(new Error("primary")); } catch(e) { if (!String(e).includes("primary")) throw e; }
	globalThis.__r = String(returns);
})()`)
	if got != "1" {
		t.Fatalf("input return count=%s; want one return", got)
	}
}

func TestEventStreamDecodeThrowBeforeFirstNextClosesDistinctSourceOnce(t *testing.T) {
	inst := mustUtilVM(t)
	got := runJSAssert(t, inst, `(function(){
	let returns=0;
	const source={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};}};
	const input={[Symbol.iterator](){return source;}};
	const decoder=util.eventstream.decode(input);
	try { decoder.throw(new Error("primary")); } catch(e) { if (!String(e).includes("primary")) throw e; }
	globalThis.__r = String(returns);
})()`)
	if got != "1" {
		t.Fatalf("source return count=%s; want one return", got)
	}
}

func TestEventStreamDecodeParseErrorClosesInputOnce(t *testing.T) {
	inst := mustUtilVM(t)
	frame := testEventStreamFrame(nil)
	frame[len(frame)-1] ^= 0x01
	setBytes(t, inst, "frame", frame)
	got := runJSAssert(t, inst, `(function(){
	let returns=0;
	const input={next(){return {done:false,value:frame};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	try { util.eventstream.decode(input).next(); } catch(e) { if (!String(e).includes("message CRC")) throw e; }
	globalThis.__r = String(returns);
})()`)
	if got != "1" {
		t.Fatalf("input return count=%s; want one return", got)
	}
}

// Given a valid fixed frame, when decoded, then payload bytes match the checked literal vector.
func TestEventStreamDecodeGoldenFrame(t *testing.T) {
	inst := mustUtilVM(t)
	// 固定 AWS EventStream frame: empty headers, payload 00 80 FF。
	frame := []byte{
		0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00,
		0x42, 0x62, 0x32, 0x3b, 0x00, 0x80, 0xff, 0xd0,
		0xb7, 0x15, 0x6a,
	}
	setBytes(t, inst, "frame", frame)
	got := runJSAssert(t, inst, `(function(){
	const input = { next(){ return this.done ? {done:true} : (this.done=true,{value:frame,done:false}); }, return(){ this.closed=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	const decoder = util.eventstream.decode(input);
	const result = decoder.next();
	globalThis.__r = JSON.stringify([result.done, Array.from(new Uint8Array(result.value.payload)), decoder.next().done]);
})()`)
	if got != "[false,[0,128,255],true]" {
		t.Fatalf("decode states = %s", got)
	}
}

func TestEventStreamDecodeRejectsCorruptCRCAndClosesInput(t *testing.T) {
	inst := mustUtilVM(t)
	frame := testEventStreamFrame([]byte("payload"))
	frame[len(frame)-1] ^= 0x01
	setBytes(t, inst, "frame", frame)
	runJSAssert(t, inst, `(function(){
	let closed = false;
	const input = { next(){ return this.sent ? {done:true} : (this.sent=true,{value:frame,done:false}); }, return(){ closed=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (!String(e).includes("CRC")) throw e; }
	if (!closed) throw new Error("input was not returned");
	globalThis.__r = "ok";
})()`)
}

func TestEventStreamDecodeSplitsFrameAcrossChunks(t *testing.T) {
	inst := mustUtilVM(t)
	frame := []byte{
		0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00,
		0x7f, 0x02, 0x1b, 0x8b, 0x6f, 0x6b, 0xf5, 0xe8,
		0x03, 0xfe,
	}
	setBytes(t, inst, "first", frame[:7])
	setBytes(t, inst, "second", frame[7:])
	got := runJSAssert(t, inst, `(function(){
	let index = 0;
	const input = { next(){ const chunks=[first,second]; return index < chunks.length ? {value:chunks[index++],done:false} : {done:true}; }, [Symbol.iterator](){ return this; } };
	const decoder=util.eventstream.decode(input);
	globalThis.__r = Array.from(new Uint8Array(decoder.next().value.payload)).join(",");
})()`)
	if got != "111,107" {
		t.Fatalf("payload = %s", got)
	}
}
