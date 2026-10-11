package streamct

import (
	"mime"
	"strings"
)

// IsSSE 判断 Content-Type 是否为合法 SSE media type。
func IsSSE(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	return err == nil && strings.EqualFold(mediaType, "text/event-stream")
}

// IsEventStream 判断 Content-Type 是否为合法 AWS EventStream media type。
func IsEventStream(ct string) bool {
	mediaType, _, err := mime.ParseMediaType(ct)
	return err == nil && strings.EqualFold(mediaType, "application/vnd.amazon.eventstream")
}

// IsStream 判断 Content-Type 是否为支持的流式 media type。
func IsStream(ct string) bool {
	return IsSSE(ct) || IsEventStream(ct)
}
