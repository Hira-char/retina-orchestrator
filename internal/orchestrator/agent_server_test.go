// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v2"
	"google.golang.org/protobuf/proto"
)

// Remaining coverage gaps are unreachable without refactoring *net.TCPConn to
// an interface, as syscall-level errors cannot be injected on a real connection:
//   - newAgentStream: SetKeepAlive, SetKeepAlivePeriod, SetReadBuffer, SetWriteBuffer
//   - handshake: send error after auth recv — kernel buffers the write on loopback
//     even when the client has already closed. SetDeadline error after successful
//     auth — both require syscall-level injection unavailable on loopback.
//   - listenAndServe: non-TCP type assertion, newAgentStream error continue,
//     second shutdown race after listener setup — all require syscall-level
//     error injection on *net.TCPConn.

// -- helpers ------------------------------------------------------------------

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func newTCPPair(t *testing.T) (client, server *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot create listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn.(*net.TCPConn)
	}()

	dial, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	return dial.(*net.TCPConn), <-accepted
}

func newTestAgentServer(t *testing.T, auth authHandleFunc, agent agentHandleFunc) (*agentServer, string) {
	t.Helper()
	addr := freeAddr(t)
	s, err := newAgentServer(&agentServerConfig{
		address:          addr,
		handshakeTimeout: time.Second,
		bufferLength:     4096,
		authHandler:      auth,
		agentHandler:     agent,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s, addr
}

var allowAll authHandleFunc = func(_ api.AuthRequest) api.AuthResponse {
	return api.AuthResponse{Authenticated: true}
}

var denyAll authHandleFunc = func(_ api.AuthRequest) api.AuthResponse {
	return api.AuthResponse{Authenticated: false, Message: "denied"}
}

var nopAgentHandler agentHandleFunc = func(_ *agentAuthStatus, _ *agentStream) {}

func startAgentServer(t *testing.T, s *agentServer) {
	t.Helper()
	go func() { _ = s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)
}

// doHandshake sends an AuthRequest and returns the AuthResponse along with the
// persistent decoder. Callers that read further messages from conn must reuse
// the returned decoder — creating a new one would re-buffer bytes already
// consumed, silently discarding them.
func doHandshake(t *testing.T, conn net.Conn, req api.AuthRequest) api.AuthResponse {
	t.Helper()

	reqBytes, err := proto.Marshal(&req)
	if err != nil {
		t.Fatalf("cannot marshal auth request: %v", err)
	}

	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, uint32(len(reqBytes)))
	if _, err := conn.Write(lengthPrefix); err != nil {
		t.Fatalf("cannot write length prefix: %v", err)
	}
	if _, err := conn.Write(reqBytes); err != nil {
		t.Fatalf("cannot write auth request: %v", err)
	}

	respLengthPrefix := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLengthPrefix); err != nil {
		t.Fatalf("cannot read auth response length: %v", err)
	}
	respLen := binary.BigEndian.Uint32(respLengthPrefix)

	respBytes := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBytes); err != nil {
		t.Fatalf("cannot read auth response payload: %v", err)
	}

	var resp api.AuthResponse
	if err := proto.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("cannot unmarshal auth response: %v", err)
	}

	return resp
}

// -- newAgentServer -----------------------------------------------------------

func TestNewAgentServer_NilAuthHandler(t *testing.T) {
	t.Parallel()
	_, err := newAgentServer(&agentServerConfig{agentHandler: nopAgentHandler}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for nil authHandler, got nil")
	}
}

func TestNewAgentServer_NilAgentHandler(t *testing.T) {
	t.Parallel()
	_, err := newAgentServer(&agentServerConfig{authHandler: allowAll}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for nil agentHandler, got nil")
	}
}

func TestNewAgentServer_Valid(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	if s == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestNewAgentServer_NilLogger(t *testing.T) {
	t.Parallel()
	s, err := newAgentServer(&agentServerConfig{
		address:          "127.0.0.1:0",
		handshakeTimeout: time.Second,
		bufferLength:     4096,
		authHandler:      allowAll,
		agentHandler:     nopAgentHandler,
	}, nil, testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with nil logger: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil server")
	}
}

// -- listenAndServe -----------------------------------------------------------

func TestListenAndServe_BindError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot bind: %v", err)
	}
	defer func() { _ = ln.Close() }()

	s, err := newAgentServer(&agentServerConfig{
		address:      ln.Addr().String(),
		bufferLength: 4096,
		authHandler:  allowAll,
		agentHandler: nopAgentHandler,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.listenAndServe(); err == nil || err == ErrServerShutdown {
		t.Fatalf("expected bind error, got %v", err)
	}
}

func TestListenAndServe_ShutdownBeforeListen(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	_ = s.close(time.Second)
	if err := s.listenAndServe(); err != ErrServerShutdown {
		t.Fatalf("expected ErrServerShutdown, got %v", err)
	}
}

func TestListenAndServe_ReturnsErrServerShutdownAfterClose(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)

	done := make(chan error, 1)
	go func() { done <- s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)
	_ = s.close(time.Second)

	select {
	case err := <-done:
		if err != ErrServerShutdown {
			t.Fatalf("expected ErrServerShutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listenAndServe did not return after close")
	}
}

func TestListenAndServe_AcceptError(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)

	done := make(chan error, 1)
	go func() { done <- s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)

	s.mutex.Lock()
	_ = s.listener.Close()
	s.mutex.Unlock()

	select {
	case err := <-done:
		if err == nil || err == ErrServerShutdown {
			t.Fatalf("expected raw accept error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listenAndServe did not return after listener was closed")
	}
}

// -- close --------------------------------------------------------------------

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	startAgentServer(t, s)

	if err := s.close(time.Second); err != nil {
		t.Fatalf("first close: unexpected error: %v", err)
	}
	if err := s.close(time.Second); err != nil {
		t.Fatalf("second close: unexpected error: %v", err)
	}
}

func TestClose_Timeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{})
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, _ *agentStream) {
		close(started)
		<-block
	})
	startAgentServer(t, s)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	doHandshake(t, conn, api.AuthRequest{AgentId: "a1"})
	<-started

	err = s.close(time.Millisecond)
	close(block)
	if err == nil {
		t.Fatal("expected timeout error from close, got nil")
	}
}

// -- handshake ----------------------------------------------------------------

func TestHandshake_Success(t *testing.T) {
	t.Parallel()
	statusCh := make(chan *agentAuthStatus, 1)
	s, addr := newTestAgentServer(t, allowAll, func(status *agentAuthStatus, _ *agentStream) {
		statusCh <- status
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp := doHandshake(t, conn, api.AuthRequest{AgentId: "agent-1", Secret: "s"})
	if !resp.Authenticated {
		t.Fatalf("expected authenticated, got: %s", resp.Message)
	}
	select {
	case status := <-statusCh:
		if status.agentID != "agent-1" {
			t.Errorf("expected agentID agent-1, got %s", status.agentID)
		}
	case <-time.After(time.Second):
		t.Fatal("agent handler not called")
	}
}

func TestHandshake_Failure(t *testing.T) {
	t.Parallel()
	s, addr := newTestAgentServer(t, denyAll, nopAgentHandler)
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp := doHandshake(t, conn, api.AuthRequest{AgentId: "bad", Secret: "wrong"})
	if resp.Authenticated {
		t.Fatal("expected not authenticated")
	}
	if resp.Message != "denied" {
		t.Errorf("expected message 'denied', got %q", resp.Message)
	}
}

func TestHandshake_ConnectionClosedBeforeAuth(t *testing.T) {
	t.Parallel()
	s, addr := newTestAgentServer(t, allowAll, nopAgentHandler)
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestHandshake_DeadlineClearedAfterAuth(t *testing.T) {
	t.Parallel()

	handshakeTimeout := 100 * time.Millisecond
	addr := freeAddr(t)
	fieCh := make(chan *api.ForwardingInfoElement, 1)

	s, err := newAgentServer(&agentServerConfig{
		address:          addr,
		handshakeTimeout: handshakeTimeout,
		bufferLength:     4096,
		authHandler:      allowAll,
		agentHandler: func(_ *agentAuthStatus, stream *agentStream) {
			if err := stream.sendPD(&api.ProbingDirective{ProbingDirectiveId: 1}); err != nil {
				return
			}
			fie, err := stream.receiveFIE()
			if err != nil {
				return
			}
			fieCh <- fie
		},
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Reuse the decoder from doHandshake to avoid losing buffered bytes.
	_ = doHandshake(t, conn, api.AuthRequest{AgentId: "a1"})

	// Read the PD immediately — the server sends it right after auth.
	var pd api.ProbingDirective
	readMessage(t, conn, &pd)

	// Wait longer than handshakeTimeout before sending the FIE — if the
	// deadline is not cleared, the server-side connection will have timed
	// out by now and the encode below will fail.
	time.Sleep(handshakeTimeout * 3)

	if err := json.NewEncoder(conn).Encode(&api.ForwardingInfoElement{ProbingDirectiveId: 1}); err != nil {
		t.Fatalf("connection timed out after handshake — deadline not cleared: %v", err)
	}

	select {
	case got := <-fieCh:
		if got.ProbingDirectiveId != 1 {
			t.Errorf("expected FIE ID 1, got %d", got.ProbingDirectiveId)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive FIE — connection may have timed out")
	}
}

// -- send / receive -----------------------------------------------------------

func TestSendReceive_RoundTrip(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	pd := &api.ProbingDirective{ProbingDirectiveId: 99}
	if err := send(client, json.NewEncoder(client), 0, pd); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	got, err := receive[api.ProbingDirective](server, json.NewDecoder(server), 0)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}
	if got.ProbingDirectiveId != 99 {
		t.Errorf("expected ID 99, got %d", got.ProbingDirectiveId)
	}
}

func TestSendReceive_WithTimeout(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	fie := &api.ForwardingInfoElement{ProbingDirectiveId: 7}
	if err := send(client, json.NewEncoder(client), time.Second, fie); err != nil {
		t.Fatalf("send with timeout failed: %v", err)
	}
	got, err := receive[api.ForwardingInfoElement](server, json.NewDecoder(server), time.Second)
	if err != nil {
		t.Fatalf("receive with timeout failed: %v", err)
	}
	if got.ProbingDirectiveId != 7 {
		t.Errorf("expected ID 7, got %d", got.ProbingDirectiveId)
	}
}

func TestSend_WriteDeadlineError(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	_ = server.Close()
	_ = client.Close()

	if err := send(client, json.NewEncoder(client), time.Second, &api.ProbingDirective{}); err == nil {
		t.Fatal("expected error sending on closed connection, got nil")
	}
}

func TestSend_EncodeError(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	_ = server.Close()
	_ = client.Close()

	if err := send(client, json.NewEncoder(client), 0, &api.ProbingDirective{}); err == nil {
		t.Fatal("expected encode error sending on closed connection, got nil")
	}
}

func TestReceive_DecodeError(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	defer func() { _ = server.Close() }()

	if _, err := client.Write([]byte("not json\n")); err != nil {
		t.Fatalf("cannot write: %v", err)
	}
	_ = client.Close()

	if _, err := receive[api.ProbingDirective](server, json.NewDecoder(server), 0); err == nil {
		t.Fatal("expected decode error, got nil")
	}
}

func TestReceive_DeadlineExceeded(t *testing.T) {
	t.Parallel()
	_, server := newTCPPair(t)
	defer func() { _ = server.Close() }()

	if _, err := receive[api.ProbingDirective](server, json.NewDecoder(server), time.Millisecond); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestReceive_ReadDeadlineError(t *testing.T) {
	t.Parallel()
	_, server := newTCPPair(t)
	_ = server.Close()

	if _, err := receive[api.ProbingDirective](server, json.NewDecoder(server), time.Second); err == nil {
		t.Fatal("expected error receiving on closed connection, got nil")
	}
}

// -- agentStream --------------------------------------------------------------

func TestAgentStream_Context(t *testing.T) {
	t.Parallel()
	ctxCh := make(chan bool, 1)
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, stream *agentStream) {
		ctxCh <- stream.context() != nil
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	doHandshake(t, conn, api.AuthRequest{AgentId: "a1"})

	select {
	case ok := <-ctxCh:
		if !ok {
			t.Error("expected non-nil context")
		}
	case <-time.After(time.Second):
		t.Fatal("agent handler not called")
	}
}

func TestAgentStream_SendPDReceiveFIE(t *testing.T) {
	t.Parallel()
	fieCh := make(chan *api.ForwardingInfoElement, 1)
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, stream *agentStream) {
		if err := stream.sendPD(&api.ProbingDirective{ProbingDirectiveId: 42}); err != nil {
			return
		}
		fie, err := stream.receiveFIE()
		if err != nil {
			return
		}
		fieCh <- fie
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Reuse the decoder from doHandshake to avoid re-buffering bytes already consumed.
	_ = doHandshake(t, conn, api.AuthRequest{AgentId: "a1"})

	var pd api.ProbingDirective
	readMessage(t, conn, &pd)
	if pd.ProbingDirectiveId != 42 {
		t.Errorf("expected PD ID 42, got %d", pd.ProbingDirectiveId)
	}

	if err := json.NewEncoder(conn).Encode(&api.ForwardingInfoElement{ProbingDirectiveId: 42}); err != nil {
		t.Fatalf("cannot encode FIE: %v", err)
	}

	select {
	case got := <-fieCh:
		if got.ProbingDirectiveId != 42 {
			t.Errorf("expected FIE ID 42, got %d", got.ProbingDirectiveId)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive FIE in time")
	}
}

func readMessage(t *testing.T, conn net.Conn, msg proto.Message) {
	t.Helper()

	lengthPrefix := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthPrefix); err != nil {
		t.Fatalf("cannot read message length: %v", err)
	}
	msgLen := binary.BigEndian.Uint32(lengthPrefix)

	msgBytes := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		t.Fatalf("cannot read message payload: %v", err)
	}

	if err := proto.Unmarshal(msgBytes, msg); err != nil {
		t.Fatalf("cannot unmarshal message: %v", err)
	}
}
