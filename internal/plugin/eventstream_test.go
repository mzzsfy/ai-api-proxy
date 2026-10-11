package plugin

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/dop251/goja"
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

func TestEventStreamDecodeRejectsBadPreludeCRCBeforeWaitingForFrame(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := testEventStreamFrame(nil)
	binary.BigEndian.PutUint32(frame[0:4], ^uint32(0))
	frame[8] ^= 0x01
	_ = vm.Set("prelude", vm.NewArrayBuffer(frame[:12]))
	_, err := vm.RunString(`(function(){
	let returned = false;
	const input = { next(){ return {value:prelude,done:false}; }, return(){ returned=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (!String(e).includes("prelude CRC")) throw e; }
	if (!returned) throw new Error("input was not returned");
})()`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEventStreamDecodeKeepsParseErrorWhenReturnFails(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := testEventStreamFrame(nil)
	frame[len(frame)-1] ^= 0x01
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){
	const input = { next(){ return {value:frame,done:false}; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { return String(e); }
	return "no error";
})()`)
	if err != nil || !strings.Contains(got.String(), "message CRC") || strings.Contains(got.String(), "close failed") {
		t.Fatalf("error = %v, %v", got, err)
	}
}

func TestEventStreamDecodePropagatesReturnErrorWithoutParseError(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := testEventStreamFrame([]byte("payload"))
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	_, err := vm.RunString(`(function(){
	const input = { next(){ return {value:frame,done:false}; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	const decoder = util.eventstream.decode(input);
	decoder.next();
	decoder.return();
})()`)
	if err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestEventStreamDecodePreservesThrownUndefined(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	got, err := vm.RunString(`(function(){
	const input = { next(){ throw undefined; }, return(){ throw new Error("close failed"); }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (e !== undefined) throw new Error("primary error replaced"); return true; }
	return false;
})()`)
	if err != nil || !got.ToBoolean() {
		t.Fatalf("undefined primary error was lost: %v, %v", got, err)
	}
}

func TestEventStreamDecodeReturnBeforeFirstNextClosesInput(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	got, err := vm.RunString(`(function(){
	let returns=0;
	const input={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	const decoder=util.eventstream.decode(input);
	decoder.return();
	return returns;
})()`)
	if err != nil || got.ToInteger() != 1 {
		t.Fatalf("input return count=%v, err=%v; want one return", got, err)
	}
}

func TestEventStreamDecodeThrowBeforeFirstNextClosesInputOnce(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	got, err := vm.RunString(`(function(){
	let returns=0;
	const input={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	const decoder=util.eventstream.decode(input);
	try { decoder.throw(new Error("primary")); } catch(e) { if (!String(e).includes("primary")) throw e; }
	return returns;
})()`)
	if err != nil || got.ToInteger() != 1 {
		t.Fatalf("input return count=%v, err=%v; want one return", got, err)
	}
}

func TestEventStreamDecodeThrowBeforeFirstNextClosesDistinctSourceOnce(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	got, err := vm.RunString(`(function(){
	let returns=0;
	const source={next(){return {done:false,value:new ArrayBuffer(0)};},return(){returns++;return {done:true};}};
	const input={[Symbol.iterator](){return source;}};
	const decoder=util.eventstream.decode(input);
	try { decoder.throw(new Error("primary")); } catch(e) { if (!String(e).includes("primary")) throw e; }
	return returns;
})()`)
	if err != nil || got.ToInteger() != 1 {
		t.Fatalf("source return count=%v, err=%v; want one return", got, err)
	}
}

func TestEventStreamDecodeParseErrorClosesInputOnce(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := testEventStreamFrame(nil)
	frame[len(frame)-1] ^= 0x01
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){
	let returns=0;
	const input={next(){return {done:false,value:frame};},return(){returns++;return {done:true};},[Symbol.iterator](){return this;}};
	try { util.eventstream.decode(input).next(); } catch(e) { if (!String(e).includes("message CRC")) throw e; }
	return returns;
})()`)
	if err != nil || got.ToInteger() != 1 {
		t.Fatalf("input return count=%v, err=%v; want one return", got, err)
	}
}

// Given a valid fixed frame, when decoded, then payload bytes match the checked literal vector.
func TestEventStreamDecodeGoldenFrame(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	// 固定 AWS EventStream frame: empty headers, payload 00 80 FF。
	frame := []byte{
		0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00,
		0x42, 0x62, 0x32, 0x3b, 0x00, 0x80, 0xff, 0xd0,
		0xb7, 0x15, 0x6a,
	}
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	got, err := vm.RunString(`(function(){
	const input = { next(){ return this.done ? {done:true} : (this.done=true,{value:frame,done:false}); }, return(){ this.closed=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	const decoder = util.eventstream.decode(input);
	const result = decoder.next();
	return [result.done, Array.from(new Uint8Array(result.value.payload)), decoder.next().done];
})()`)
	if err != nil {
		t.Fatal(err)
	}
	values := got.Export().([]interface{})
	if values[0] != false || values[2] != true {
		t.Fatalf("decode states = %#v", values)
	}
	payload := values[1].([]interface{})
	want := []int{0, 128, 255}
	if len(payload) != len(want) {
		t.Fatalf("payload length=%d", len(payload))
	}
	for i, v := range payload {
		if int(v.(int64)) != want[i] {
			t.Fatalf("payload[%d]=%v want %d", i, v, want[i])
		}
	}
}

func TestEventStreamDecodeRejectsCorruptCRCAndClosesInput(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := testEventStreamFrame([]byte("payload"))
	frame[len(frame)-1] ^= 0x01
	_ = vm.Set("frame", vm.NewArrayBuffer(frame))
	_, err := vm.RunString(`(function(){
	let closed = false;
	const input = { next(){ return this.sent ? {done:true} : (this.sent=true,{value:frame,done:false}); }, return(){ closed=true; return {done:true}; }, [Symbol.iterator](){ return this; } };
	try { util.eventstream.decode(input).next(); } catch (e) { if (!String(e).includes("CRC")) throw e; }
	if (!closed) throw new Error("input was not returned");
})()`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEventStreamDecodeSplitsFrameAcrossChunks(t *testing.T) {
	vm := goja.New()
	bindUtil(vm, HostDeps{})
	frame := []byte{
		0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00,
		0x7f, 0x02, 0x1b, 0x8b, 0x6f, 0x6b, 0xf5, 0xe8,
		0x03, 0xfe,
	}
	_ = vm.Set("first", vm.NewArrayBuffer(frame[:7]))
	_ = vm.Set("second", vm.NewArrayBuffer(frame[7:]))
	got, err := vm.RunString(`(function(){
	let index = 0;
	const input = { next(){ const chunks=[first,second]; return index < chunks.length ? {value:chunks[index++],done:false} : {done:true}; }, [Symbol.iterator](){ return this; } };
	const decoder=util.eventstream.decode(input);
	return Array.from(new Uint8Array(decoder.next().value.payload)).join(",");
})()`)
	if err != nil || got.String() != "111,107" {
		t.Fatalf("payload = %v, err=%v", got, err)
	}
}
