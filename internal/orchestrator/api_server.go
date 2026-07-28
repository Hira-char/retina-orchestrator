// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	api "github.com/dioptra-io/retina-commons/api/v2"
	_ "github.com/dioptra-io/retina-orchestrator/docs"
)

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}

type fieHandleFunc func(s *fieClient)

type apiServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// readHeaderTimeout is the timeout for reading HTTP request headers.
	readHeaderTimeout time.Duration
	fieHandler        fieHandleFunc
	logger            *slog.Logger
}

type apiServer struct {
	config  *apiServerConfig
	logger  *slog.Logger
	server  *http.Server
	mutex   sync.Mutex
	clients map[*fieClient]struct{}
}

func newAPIServer(config *apiServerConfig) (*apiServer, error) {
	if config.fieHandler == nil {
		return nil, fmt.Errorf("fieHandler cannot be nil")
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}

	s := &apiServer{
		config:  config,
		logger:  config.logger,
		clients: make(map[*fieClient]struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/stream", s.handleStream)
	mux.HandleFunc("/api/v1/swagger/", httpSwagger.WrapHandler)

	s.server = &http.Server{
		Addr:              config.address,
		Handler:           mux,
		ReadHeaderTimeout: config.readHeaderTimeout,
	}

	return s, nil
}

func (s *apiServer) listenAndServe() error {
	if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *apiServer) close(timeout time.Duration) error {
	s.logger.Info("Shutting down API server")

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	if err := s.server.Shutdown(exitCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Warn("API server shutdown timed out", slog.Duration("timeout", timeout))
		return err
	}
	return nil
}

// @Summary		Stream forwarding info elements
// @Description	Opens a long-lived NDJSON stream of FIEs from connected agents.
// @Tags			stream
// @Produce		application/x-ndjson
// @Success		200	{object}	SequencedFIE
// @Failure		500	{string}	string	"internal server error"
// @Router			/stream [get]
func (s *apiServer) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Streaming unsupported: ResponseWriter does not implement http.Flusher")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	client := &fieClient{
		ctx:     r.Context(),
		flusher: flusher,
		encoder: json.NewEncoder(w),
	}
	s.addClient(client)
	s.logger.Debug("Client connected", slog.String("remote_addr", r.RemoteAddr))
	defer func() {
		s.removeClient(client)
		s.logger.Debug("Client disconnected", slog.String("remote_addr", r.RemoteAddr))
	}()

	s.config.fieHandler(client)
}

func (s *apiServer) addClient(client *fieClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.clients[client] = struct{}{}
}

func (s *apiServer) removeClient(client *fieClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.clients, client)
}

type fieClient struct {
	ctx     context.Context
	flusher http.Flusher
	encoder *json.Encoder
}

func (s *fieClient) sendFIE(fie *SequencedFIE) error {
	if err := s.encoder.Encode(fie); err != nil {
		return fmt.Errorf("failed to send FIE: %w", err)
	}
	s.flusher.Flush()
	return nil
}

func (s *fieClient) context() context.Context {
	return s.ctx
}
