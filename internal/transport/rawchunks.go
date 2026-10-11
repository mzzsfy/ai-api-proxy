package transport

import "io"

const (
	rawChunkSize  = 32 * 1024
	rawChunkQueue = 16
)

// ChunkResult 是无损字节流中的一个数据块或终止读错误。
type ChunkResult struct {
	Data []byte
	Err  error
}

// RawChunks 按序读取原始响应字节；stop 取消等待发送，不打断阻塞 Read。
func RawChunks(body io.ReadCloser, stop <-chan struct{}) <-chan ChunkResult {
	out := make(chan ChunkResult, rawChunkQueue)
	go func() {
		defer close(out)
		buf := make([]byte, rawChunkSize)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				if !sendChunk(stop, out, ChunkResult{Data: chunk}) {
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					sendChunk(stop, out, ChunkResult{Err: err})
				}
				return
			}
		}
	}()
	return out
}

func sendChunk(stop <-chan struct{}, out chan<- ChunkResult, item ChunkResult) bool {
	select {
	case out <- item:
		return true
	case <-stop:
		return false
	}
}
