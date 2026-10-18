//go:build slow

package plugin

import (
	"strconv"
	"testing"
)

// Given a valid payload above the server policy threshold, when decoded, then its full length is returned.
// 帧经分片喂入(桥无 ArrayBuffer;单段 JSON 文本超内存预算,分片在解码器容量语义内)
func TestEventStreamDecodeAcceptsPayloadAboveServerPolicy(t *testing.T) {
	inst := mustUtilVM(t)
	payload := make([]byte, 24*1024*1024+1)
	frame := testEventStreamFrameWithHeaders(nil, payload)
	const chunkSize = 256 * 1024
	chunks := (len(frame) + chunkSize - 1) / chunkSize
	for i := 0; i < chunks; i++ {
		lo := i * chunkSize
		hi := lo + chunkSize
		if hi > len(frame) {
			hi = len(frame)
		}
		setBytes(t, inst, "__ck"+strconv.Itoa(i), frame[lo:hi])
	}
	if err := inst.EvalString(`(function(){
	const n = ` + strconv.Itoa(chunks) + `;
	let i = 0;
	const input = { next(){ return i < n ? {value: globalThis["__ck"+(i++)], done:false} : {done:true}; }, [Symbol.iterator](){ return this; } };
	const e = util.eventstream.decode(input).next().value;
	if (e.payload.byteLength !== ` + strconv.Itoa(len(payload)) + `) throw new Error("payload length="+e.payload.byteLength);
})()`); err != nil {
		t.Fatal(err)
	}
}
