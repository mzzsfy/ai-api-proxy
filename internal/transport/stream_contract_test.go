package transport

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

type contractSSEBody struct {
	io.ReadCloser
	closes int
}

func (b *contractSSEBody) Close() error { b.closes++; return b.ReadCloser.Close() }

func TestFrameSSEResult_PreservesFramesBeforeReadError(t *testing.T) {
	// Given 完整帧后底层读取失败 When 排空结果 Then 按序得到完整帧及原错误且body关闭
	synctest.Test(t, func(t *testing.T) {
		readErr := errors.New("synthetic upstream read failure")
		body := &contractSSEBody{ReadCloser: &resultReader{results: []readResult{
			{data: []byte("event: delta\ndata: first\ndata: second\n\n")},
			{err: readErr},
		}}}
		var results []SSEFrameResult
		for result := range FrameSSEResult(body, make(chan struct{})) {
			results = append(results, result)
		}
		synctest.Wait()
		if len(results) != 2 || results[0].Frame.Event != "delta" || results[0].Frame.Data != "first\nsecond" || results[0].Err != nil {
			t.Fatalf("frame order/content = %+v", results)
		}
		if !errors.Is(results[1].Err, readErr) || results[1].Frame != (pipeline.SSEFrame{}) {
			t.Fatalf("terminal error = %+v", results[1])
		}
		if body.closes != 1 {
			t.Fatalf("body closes=%d; want 1", body.closes)
		}
	})
}

func TestFrameSSEResult_NaturalEOFFlushesTrailingFrame(t *testing.T) {
	// Given 最后帧无空行且上游自然EOF When 读取 Then 尾帧不丢失且不伪造错误
	synctest.Test(t, func(t *testing.T) {
		body := &contractSSEBody{ReadCloser: io.NopCloser(strings.NewReader(": keepalive\r\nevent: delta\r\ndata: first\r\ndata:  leading space\r\n"))}
		var frames []pipeline.SSEFrame
		for result := range FrameSSEResult(body, make(chan struct{})) {
			if result.Err != nil {
				t.Fatal(result.Err)
			}
			frames = append(frames, result.Frame)
		}
		synctest.Wait()
		want := []pipeline.SSEFrame{{Event: "delta", Data: "first\n leading space"}}
		if !reflect.DeepEqual(frames, want) {
			t.Fatalf("frames=%+v; want %+v", frames, want)
		}
		if body.closes != 1 {
			t.Fatalf("body closes=%d; want 1", body.closes)
		}
	})
}

func TestFrameSSEResult_CloseBodyAndStopReleasesBackpressure(t *testing.T) {
	// Given 帧队列已满且生产者阻塞发送 When 调用者关闭stop和body Then 通道排空后关闭
	synctest.Test(t, func(t *testing.T) {
		body, writer := io.Pipe()
		stop := make(chan struct{})
		stopClosed := false
		tracked := &contractSSEBody{ReadCloser: body}
		results := FrameSSEResult(tracked, stop)
		writeDone := make(chan error, 1)
		go func() {
			_, err := io.WriteString(writer, strings.Repeat("data: x\n\n", cap(results)+1))
			writeDone <- err
		}()
		defer func() {
			if !stopClosed {
				close(stop)
			}
			_ = body.Close()
			_ = writer.Close()
			for range results {
			}
			synctest.Wait()
		}()
		synctest.Wait()
		if got := len(results); got != cap(results) {
			t.Fatalf("queue not full: len=%d cap=%d", got, cap(results))
		}
		close(stop)
		stopClosed = true
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if tracked.closes != 1 {
			t.Errorf("producer did not close body before draining: closes=%d", tracked.closes)
		}
		for range results {
		}
		_ = writer.Close()
		<-writeDone
		synctest.Wait()
	})
}
