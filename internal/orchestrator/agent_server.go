// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v2"
	"github.com/dioptra-io/retina-commons/network"
)

// agentKeepalivePeriod is the interval between TCP keepalive probes
// for agent connections.
const agentKeepalivePeriod = 10 * time.Second

// agentSendTimeout is the deadline for sending a probing directive to an agent.
// Without a deadline, a dead agent will block the sender goroutine indefinitely
// once the TCP send buffer fills up
const agentSendTimeout = 5 * time.Second

type agentAuthStatus struct {
	agentID       string
	remoteAddress net.Addr
}

// agentHandleFunc is called in a separate goroutine for each authenticated
// agent connection.
type agentHandleFunc func(status *agentAuthStatus, s *agentStream)

// authHandleFunc handles agent authentication. If Authenticated is false, the connection is closed.
type authHandleFunc func(req api.AuthRequest) api.AuthResponse

type agentServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// handshakeTimeout is the deadline for the initial authentication exchange.
	handshakeTimeout time.Duration
	bufferLength     int
	agentHandler     agentHandleFunc
	authHandler      authHandleFunc
}

// agentServer handles bidirectional PD/FIE communication with agents over newline-delimited JSON.
type agentServer struct {
	config   *agentServerConfig
	logger   *slog.Logger
	metrics  *Metrics
	shutdown atomic.Bool
	mutex    sync.Mutex
	// connections tracks all active agent connections for shutdown.
	connections  map[int]*agentStream
	listener     net.Listener
	nextStreamID int
	wg           sync.WaitGroup
}

func newAgentServer(config *agentServerConfig, logger *slog.Logger, metrics *Metrics) (*agentServer, error) {
	if config.authHandler == nil || config.agentHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &agentServer{
		config:      config,
		logger:      logger,
		metrics:     metrics,
		connections: make(map[int]*agentStream),
	}, nil
}

// listenAndServe accepts incoming agent connections. Returns ErrServerShutdown if close has been called.
func (s *agentServer) listenAndServe() error {
	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	listener, err := net.Listen("tcp", s.config.address)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	s.logger.Info("Agent server listening", slog.String("addr", s.config.address))

	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return ErrServerShutdown
			}
			return err
		}

		s.mutex.Lock()
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			s.mutex.Unlock()
			return fmt.Errorf("expected TCP connection, got %T", conn)
		}
		stream, err := newAgentStream(s.nextStreamID, tcpConn, s)
		if err != nil {
			s.mutex.Unlock()
			s.logger.Error("Failed to configure agent connection",
				slog.String("remote_addr", conn.RemoteAddr().String()),
				slog.Any("err", err))
			_ = tcpConn.Close()
			continue
		}
		s.connections[s.nextStreamID] = stream
		s.nextStreamID++
		s.wg.Add(1)
		s.mutex.Unlock()

		go s.handleAgent(stream)
	}
}

// close closes the listener and all open connections. Multiple calls are a no-op.
func (s *agentServer) close(timeout time.Duration) error {
	if s.shutdown.Swap(true) {
		return nil
	}

	s.logger.Info("Shutting down agent server")

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	s.mutex.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for _, stream := range s.connections {
		s.removeConnection(stream)
	}
	s.mutex.Unlock()

	// Wait for active goroutines to finish, but respect the deadline.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-exitCtx.Done():
		s.logger.Warn("Agent server shutdown timed out", slog.Duration("timeout", timeout))
		return exitCtx.Err()
	}
}

func (s *agentServer) handleAgent(stream *agentStream) {
	defer s.wg.Done()
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeConnection(stream)
	}()

	status, err := s.handshake(stream)
	if err != nil {
		s.logger.Warn("Handshake failed",
			slog.String("remote_addr", stream.conn.RemoteAddr().String()),
			slog.Any("err", err))
		return
	}
	s.metrics.AgentsConnected.Inc()
	defer func() {
		s.metrics.AgentDisconnectionsTotal.WithLabelValues(status.agentID).Inc()
		s.metrics.AgentsConnected.Dec()
	}()

	s.logger.Info("Agent authenticated",
		slog.String("agent_id", status.agentID),
		slog.String("remote_addr", status.remoteAddress.String()))
	s.config.agentHandler(status, stream)
}

// Adapt the Authentication Handshake
func (s *agentServer) handshake(stream *agentStream) (*agentAuthStatus, error) {
	// Ajout du préfixe network.
	envelope, err := network.ReceiveStreamMessage(stream.conn, s.config.handshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("could not receive handshake message: %w", err)
	}

	authReqPayload, ok := envelope.GetPayload().(*api.StreamMessage_AuthRequest)
	if !ok {
		return nil, fmt.Errorf("expected AuthRequest, got: %T", envelope.GetPayload())
	}
	authReq := authReqPayload.AuthRequest

	authResp := s.config.authHandler(*authReq)

	responseEnvelope := &api.StreamMessage{
		Payload: &api.StreamMessage_AuthResponse{
			AuthResponse: &authResp,
		},
	}

	// Ajout du préfixe network.
	if err := network.SendStreamMessage(stream.conn, s.config.handshakeTimeout, responseEnvelope); err != nil {
		return nil, fmt.Errorf("could not send auth response: %w", err)
	}

	if !authResp.Authenticated {
		s.metrics.AuthFailuresTotal.Inc()
		return nil, fmt.Errorf("agent not authenticated: %s", authResp.Message)
	}

	_ = stream.conn.SetDeadline(time.Time{})

	return &agentAuthStatus{
		agentID:       authReq.AgentId,
		remoteAddress: stream.conn.RemoteAddr(),
	}, nil
}

// removeConnection must be called with s.mutex held.
func (s *agentServer) removeConnection(stream *agentStream) {
	if _, ok := s.connections[stream.id]; !ok {
		return
	}
	stream.cancel()
	_ = stream.conn.Close()
	delete(s.connections, stream.id)
}

type agentStream struct {
	id     int
	ctx    context.Context
	cancel context.CancelFunc
	conn   *net.TCPConn
	server *agentServer
}

func newAgentStream(id int, conn *net.TCPConn, server *agentServer) (*agentStream, error) {
	if err := conn.SetKeepAlive(true); err != nil {
		return nil, fmt.Errorf("failed to enable keepalive: %w", err)
	}
	if err := conn.SetKeepAlivePeriod(agentKeepalivePeriod); err != nil {
		return nil, fmt.Errorf("failed to set keepalive period: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118
	return &agentStream{
		id:     id,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		server: server,
	}, nil
}

func (s *agentStream) context() context.Context {
	return s.ctx
}

func (s *agentStream) sendPD(e *api.ProbingDirective) error {
	envelope := &api.StreamMessage{
		Payload: &api.StreamMessage_ProbingDirective{
			ProbingDirective: e,
		},
	}
	return network.SendStreamMessage(s.conn, agentSendTimeout, envelope)
}

func (s *agentStream) receiveFIE() (*api.ForwardingInfoElement, error) {
	envelope, err := network.ReceiveStreamMessage(s.conn, 0)
	if err != nil {
		return nil, err
	}

	fiePayload, ok := envelope.GetPayload().(*api.StreamMessage_ForwardingInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected message type on stream: %T", envelope.GetPayload())
	}

	return fiePayload.ForwardingInfo, nil
}

func send[E any](conn *net.TCPConn, encoder *json.Encoder, timeout time.Duration, e *E) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return fmt.Errorf("send failed: cannot set write deadline: %w", err)
		}
	}
	if err := encoder.Encode(e); err != nil {
		return fmt.Errorf("send failed: cannot encode: %w", err)
	}
	return nil
}

func receive[E any](conn *net.TCPConn, decoder *json.Decoder, timeout time.Duration) (*E, error) {
	var e E
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, fmt.Errorf("receive failed: cannot set read deadline: %w", err)
		}
	}
	if err := decoder.Decode(&e); err != nil {
		return nil, fmt.Errorf("receive failed: cannot decode: %w", err)
	}
	return &e, nil
}
