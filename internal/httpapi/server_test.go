package httpapi

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestLoggerCapturesStatusAndAvoidsBody(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := requestLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), logger)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("secret-payload"))
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202 got %d", recorder.Code)
	}
	logged := output.String()
	if !strings.Contains(logged, "http_request_finished") || !strings.Contains(logged, "\"status\":202") {
		t.Fatalf("missing request log fields: %s", logged)
	}
	if strings.Contains(logged, "secret-payload") {
		t.Fatal("HTTP request body must not appear in structured logs")
	}
}
