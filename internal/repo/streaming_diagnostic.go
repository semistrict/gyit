package repo

import (
	"fmt"
	"os"
	"time"
)

var deferredTraceHook func(string, int64)

func streamingTrace(event string, count int64) {
	if deferredTraceHook != nil {
		deferredTraceHook(event, count)
	}
	if os.Getenv("GYIT_STREAM_NATIVE_TRACE") == "1" {
		fmt.Fprintf(os.Stderr, "streaming_phase=%s unix_nano=%d count=%d\n", event, time.Now().UnixNano(), count)
	}
}
