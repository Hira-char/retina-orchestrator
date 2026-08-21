// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// Intentionally uncovered:
//
//   - NewOrch: newQueue/newRingBuffer errors are unreachable (hardcoded safe
//     values); newAPIServer only fails on nil fieHandler, never nil here.
//   - runScheduler: PD drop branch requires a consumer to vanish between
//     TryPush and send — not reproducible in unit tests.
//   - runAPIServer/runAgentServer: non-context error branches require server
//     failure unrelated to shutdown — not injectable without refactoring.
//   - fieStreamHandler: internal_error on Pop requires a non-context error
//     from RingBuffer, which has no injectable trigger.

// -- helpers ------------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *Metrics {
	return NewMetrics(prometheus.NewRegistry())
}

func writePDFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	pd := api.ProbingDirective{ProbingDirectiveId: 1}
	b, _ := json.Marshal(&pd)
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
	return f.Name()
}

func validConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		AgentAddress:      "127.0.0.1:0",
		AgentBufferLength: 8192,
		PDQueueSize:       100,
		RingBufferSize:    100,
		APIAddress:        "127.0.0.1:0",
		PDPath:            writePDFile(t),
		Seed:              0,
		IssuanceRate:      1.0,
		ImpactThreshold:   1.0,
		Secret:            "secret",
	}
}

// sendFIEs sends a sequence of FIEs to exercise agentHandler FIE receive paths:
// one with an unknown PD ID (UpdateFromFIE error log), one incomplete (continue
// branch), and one complete (ring buffer push).
func sendFIEs(t *testing.T, conn net.Conn) {
	t.Helper()

	// Unknown PD ID — exercises the UpdateFromFIE error log.
	writeMessage(t, conn, &api.ForwardingInfoElement{
		ProbingDirectiveId: 999,
	})

	// Incomplete FIE (nil FarInfo) — exercises the continue branch.
	writeMessage(t, conn, &api.ForwardingInfoElement{
		ProbingDirectiveId: 1,
		NearInfo:           &api.Info{},
	})

	// Complete FIE — exercises the ring buffer push.
	writeMessage(t, conn, &api.ForwardingInfoElement{
		ProbingDirectiveId: 1,
		NearInfo:           &api.Info{},
		FarInfo:            &api.Info{},
	})
}

// -- Config.Validate ----------------------------------------------------------

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	if err := validConfig(t).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_DefaultsAPIReadHeaderTimeout(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIReadHeaderTimeout = 0
	_ = c.Validate()
	if c.APIReadHeaderTimeout != 5*time.Second {
		t.Errorf("expected default 5s, got %v", c.APIReadHeaderTimeout)
	}
}

func TestConfig_Validate_DefaultsFIEFilterPolicy(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.FIEFilterPolicy = ""
	_ = c.Validate()
	if c.FIEFilterPolicy != "both" {
		t.Errorf("expected default 'both', got %q", c.FIEFilterPolicy)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()
	base := validConfig(t)
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty AgentAddress", func(c *Config) { c.AgentAddress = "" }},
		{"small AgentBufferLength", func(c *Config) { c.AgentBufferLength = 100 }},
		{"zero PDQueueSize", func(c *Config) { c.PDQueueSize = 0 }},
		{"zero RingBufferSize", func(c *Config) { c.RingBufferSize = 0 }},
		{"empty APIAddress", func(c *Config) { c.APIAddress = "" }},
		{"empty PDPath", func(c *Config) { c.PDPath = "" }},
		{"zero IssuanceRate", func(c *Config) { c.IssuanceRate = 0 }},
		{"negative IssuanceRate", func(c *Config) { c.IssuanceRate = -1 }},
		{"zero ImpactThreshold", func(c *Config) { c.ImpactThreshold = 0 }},
		{"negative ImpactThreshold", func(c *Config) { c.ImpactThreshold = -1 }},
		{"invalid FIEFilterPolicy", func(c *Config) { c.FIEFilterPolicy = "invalid" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := *base
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

// -- NewOrch ------------------------------------------------------------------

func TestNewOrch_Valid(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
}

func TestNewOrch_InvalidConfig(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.AgentAddress = ""
	if _, err := NewOrch(c, testLogger(), testMetrics()); err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestNewOrch_SchedulerError(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.PDPath = "/nonexistent/path.jsonl"
	if _, err := NewOrch(c, testLogger(), testMetrics()); err == nil {
		t.Fatal("expected error for bad PDPath, got nil")
	}
}

func TestNewOrch_NilLogger(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), nil, testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with nil logger: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
}

func TestNewOrch_NilMetrics(t *testing.T) {
	t.Parallel()
	if _, err := NewOrch(validConfig(t), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil metrics, got nil")
	}
}

// -- Run ----------------------------------------------------------------------

func TestRun_StartsAndStopsCleanly(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.Run(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("unexpected Run error: %v", err)
	}
}

// -- runScheduler -------------------------------------------------------------

func TestRunScheduler_ContextCancelled(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = o.runScheduler(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRunScheduler_SkipsNilPD(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Set all PD issuance probabilities to 0 so NextPD always returns nil.
	for _, pd := range o.scheduler.pdMap {
		pd.issuanceProb = 0.0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = o.runScheduler(ctx)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestRunScheduler_DropsWhenNoQueue(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = o.runScheduler(ctx)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestRunScheduler_PushesToExistingQueue(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Register a consumer for the agent ID used in the PD file (empty string
	// since writePDFile sets no AgentID).
	consumer, err := o.pdQueue.NewConsumer("")
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = o.runScheduler(ctx)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected context error, got %v", err)
	}
}

// -- runAPIServer -------------------------------------------------------------

func TestRunAPIServer_StartsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.runAPIServer(ctx)
	if err != nil {
		t.Fatalf("unexpected runAPIServer error: %v", err)
	}
}

// -- runAgentServer -----------------------------------------------------------

func TestRunAgentServer_StartsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.runAgentServer(ctx)
	if err != nil {
		t.Fatalf("unexpected runAgentServer error: %v", err)
	}
}

// -- fieStreamHandler ---------------------------------------------------------

func TestFieStreamHandler_SendsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var buf bytes.Buffer
	client := &fieClient{
		ctx:     ctx,
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&buf),
	}

	fie := &api.ForwardingInfoElement{
		ProbingDirectiveId: 1,
		NearInfo:           &api.Info{},
		FarInfo:            &api.Info{},
	}

	done := make(chan struct{})
	go func() {
		o.fieStreamHandler(client)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = o.ringBuffer.Push(fie)

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fieStreamHandler did not return after context cancel")
	}

	if buf.Len() == 0 {
		t.Error("expected FIE to be written to buffer")
	}
}

func TestFieStreamHandler_SendFIEError(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fieClient{
		ctx:     ctx,
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&failWriter{}),
	}

	fie := &api.ForwardingInfoElement{
		ProbingDirectiveId: 1,
		NearInfo:           &api.Info{},
		FarInfo:            &api.Info{},
	}

	done := make(chan struct{})
	go func() {
		o.fieStreamHandler(client)
		close(done)
	}()

	// Wait for consumer to be created before pushing.
	time.Sleep(20 * time.Millisecond)
	_ = o.ringBuffer.Push(fie)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fieStreamHandler did not return on sendFIE error")
	}
}

// -- agentHandler -------------------------------------------------------------

func TestAgentHandler_DuplicateConnection(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := &agentAuthStatus{agentID: "agent-1"}

	// Pre-register a consumer to simulate an already-connected agent.
	_, err = o.pdQueue.NewConsumer("agent-1")
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return for duplicate connection")
	}
}

func TestAgentHandler_ReceivesAndForwardsPD(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-1"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Wait for agentHandler to register its consumer before pushing.
	time.Sleep(20 * time.Millisecond)

	pd := &api.ProbingDirective{ProbingDirectiveId: 1, AgentId: "agent-1"}
	if err := o.pdQueue.TryPush("agent-1", pd); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}

	var received api.ProbingDirective
	readMessage(t, clientConn, &received)
	if received.ProbingDirectiveId != 1 {
		t.Errorf("expected PD ID 1, got %d", received.ProbingDirectiveId)
	}

	cancel()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after context cancel")
	}
}

func TestAgentHandler_ReceivesFIE(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-2"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Give agentHandler time to start its goroutines.
	time.Sleep(20 * time.Millisecond)
	sendFIEs(t, clientConn)
	time.Sleep(50 * time.Millisecond)

	cancel()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after context cancel")
	}
}

func TestAgentHandler_SendPDError(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-3"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Wait for agentHandler to register its consumer, then close the
	// server connection so the next sendPD fails.
	time.Sleep(20 * time.Millisecond)
	_ = serverConn.Close()

	// Push a PD — sendPD will fail on the closed connection.
	pd := &api.ProbingDirective{ProbingDirectiveId: 1, AgentId: "agent-3"}
	_ = o.pdQueue.TryPush("agent-3", pd)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after sendPD error")
	}
}

// -- filterFIE ----------------------------------------------------------------

func TestFilterFIE_PolicyAny(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o.config.FIEFilterPolicy = "any"

	allow, err := o.filterFIE(&api.ForwardingInfoElement{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allow {
		t.Error("expected policy 'any' to allow all FIEs")
	}
}

func TestFilterFIE_PolicyOne(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o.config.FIEFilterPolicy = "one"

	tests := []struct {
		name string
		fie  *api.ForwardingInfoElement
		want bool
	}{
		{"both nil", &api.ForwardingInfoElement{}, false},
		{"near only", &api.ForwardingInfoElement{NearInfo: &api.Info{}}, true},
		{"far only", &api.ForwardingInfoElement{FarInfo: &api.Info{}}, true},
		{"both set", &api.ForwardingInfoElement{NearInfo: &api.Info{}, FarInfo: &api.Info{}}, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			allow, err := o.filterFIE(tt.fie)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if allow != tt.want {
				t.Errorf("filterFIE(%s) = %v, want %v", tt.name, allow, tt.want)
			}
		})
	}
}

func TestFilterFIE_InvalidPolicy(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Force an invalid policy after construction to trigger the filterFIE error path.
	o.config.FIEFilterPolicy = "invalid"

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-filter"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	// Send a complete FIE — filterFIE will fail on the invalid policy.
	fie := &api.ForwardingInfoElement{
		ProbingDirectiveId: 1,
		NearInfo:           &api.Info{},
		FarInfo:            &api.Info{},
	}
	writeMessage(t, clientConn, fie)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after filterFIE error")
	}
}

// -- agentAuthHandler ---------------------------------------------------------

func TestAgentAuthHandler_ValidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(&api.AuthRequest{Secret: "mysecret"})
	if !resp.Authenticated {
		t.Errorf("expected authenticated, got: %s", resp.Message)
	}
}

func TestAgentAuthHandler_InvalidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(&api.AuthRequest{Secret: "wrong"})
	if resp.Authenticated {
		t.Fatal("expected not authenticated")
	}
}
