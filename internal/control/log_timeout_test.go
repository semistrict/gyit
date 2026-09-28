package control

import (
	pb "gyit/internal/gen/gyit/control/v1"
	"testing"
	"time"
)

func TestLogOperationsAllowColdHistoryAcquisition(t *testing.T) {
	for name, req := range map[string]*pb.Request{
		"log":     {Operation: &pb.Request_Log{Log: &pb.LogRequest{}}},
		"path":    {Operation: &pb.Request_PathLog{PathLog: &pb.LogRequest{}}},
		"history": {Operation: &pb.Request_HistoryLog{HistoryLog: &pb.LogRequest{}}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := operationTimeout(req); got < 5*time.Minute {
				t.Fatalf("cold history operation limited to %s", got)
			}
		})
	}
}
