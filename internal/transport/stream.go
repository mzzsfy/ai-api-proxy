package transport

import (
	"bufio"
	"io"
	"net/http"
	"strings"

	"github.com/mzzsfy/ai-api-proxy/internal/pipeline"
)

// isEventStream Content-Type 是否 SSE
func isEventStream(ct string) bool {
	return strings.HasPrefix(strings.ToLower(ct), "text/event-stream")
}

// maxBodySize 非流式响应读取上限(防失控上游)
const maxBodySize = 32 * 1024 * 1024

// consumeResponse 上游响应 → TransportResponse(流式转帧通道/非流式限量读)
func consumeResponse(hresp *http.Response) pipeline.TransportResponse {
	headers := map[string]string{}
	for k := range hresp.Header {
		headers[k] = hresp.Header.Get(k)
	}
	out := pipeline.TransportResponse{Status: hresp.StatusCode, Headers: headers}
	if isEventStream(headers["Content-Type"]) {
		out.Events = frameSSE(hresp.Body)
	} else {
		out.Body = readAllLimited(hresp.Body)
		_ = hresp.Body.Close()
	}
	return out
}

// readAllLimited 限量读取
func readAllLimited(r io.Reader) []byte {
	b, _ := io.ReadAll(io.LimitReader(r, maxBodySize))
	return b
}

// frameSSE 上游响应体 → SSE 帧通道(host 分帧;data 为多行拼接语义)
func frameSSE(body io.ReadCloser) <-chan pipeline.SSEFrame {
	out := make(chan pipeline.SSEFrame, 16)
	go func() {
		defer close(out)
		defer func() { _ = body.Close() }()
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		var event string
		var data []string
		flush := func() {
			if len(data) > 0 || event != "" {
				out <- pipeline.SSEFrame{Event: event, Data: strings.Join(data, "\n")}
				event = ""
				data = data[:0]
			}
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			}
		}
		flush()
	}()
	return out
}
