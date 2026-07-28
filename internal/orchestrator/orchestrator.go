// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package orchestrator implements the Retina orchestrator, which schedules
// ProbingDirectives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements to HTTP clients.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dioptra-io/retina-commons/api/v2"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
	"golang.org/x/sync/errgroup"
)

// Config is the main configuration struct used in the orchestrator.
type Config struct {
	// AgentAddress is the TCP listening address for agent connections, in the form "host:port".
	AgentAddress      string
	AgentBufferLength int

	// PDQueueSize is the number of PDs that can be queued per agent.
	// Increase this value if agents are slow to consume directives.
	PDQueueSize    int
	RingBufferSize int

	// APIAddress is the TCP listening address for the HTTP API server, in the form "host:port".
	APIAddress string
	// APIReadHeaderTimeout defaults to 5 seconds if zero.
	APIReadHeaderTimeout time.Duration

	FIEFilterPolicy string
	PDPath          string
	Seed            uint64
	// IssuanceRate is the target global issuance rate of probing directives
	// (PDs per second, approximate).
	IssuanceRate float64
	// ImpactThreshold is the maximum number of concurrent directives allowed
	// to impact a single address in the responsible probing algorithm.
	ImpactThreshold float64
	// Secret is the shared secret for agent authentication.
	// This is an MVS feature and will be removed soon.
	Secret string
}

// Validate checks all configuration fields and applies defaults where appropriate.
// Returns an error if any required field is missing or invalid.
func (c *Config) Validate() error {
	if c.AgentAddress == "" {
		return fmt.Errorf("AgentAddress cannot be empty")
	}
	if c.AgentBufferLength < 8192 {
		return fmt.Errorf("AgentBufferLength is too small: got %d, minimum 8192", c.AgentBufferLength)
	}
	if c.PDQueueSize <= 0 {
		return fmt.Errorf("PDQueueSize must be greater than zero: got %d", c.PDQueueSize)
	}
	if c.RingBufferSize <= 0 {
		return fmt.Errorf("RingBufferSize must be greater than zero: got %d", c.RingBufferSize)
	}
	if c.APIAddress == "" {
		return fmt.Errorf("APIAddress cannot be empty")
	}
	if c.APIReadHeaderTimeout == 0 {
		c.APIReadHeaderTimeout = 5 * time.Second
	}
	if c.FIEFilterPolicy == "" {
		c.FIEFilterPolicy = "both"
	}
	if !slices.Contains([]string{"any", "one", "both"}, c.FIEFilterPolicy) {
		return fmt.Errorf("supported FIE filtering policies are 'any', 'one', or 'both' got %s", c.FIEFilterPolicy)
	}
	if c.PDPath == "" {
		return fmt.Errorf("PDPath cannot be empty")
	}
	if c.IssuanceRate <= 0 {
		return fmt.Errorf("IssuanceRate must be greater than zero: got %f", c.IssuanceRate)
	}
	if c.ImpactThreshold <= 0 {
		return fmt.Errorf("ImpactThreshold must be greater than zero: got %f", c.ImpactThreshold)
	}
	return nil
}

type orch struct {
	config      *Config
	logger      *slog.Logger
	metrics     *Metrics
	scheduler   *Scheduler
	agentServer *agentServer
	apiServer   *apiServer
	pdQueue     *structures.Queue[api.ProbingDirective]
	ringBuffer  *structures.RingBuffer[api.ForwardingInfoElement]
}

// NewOrch creates a new orchestrator from the given configuration. Returns an
// error if the configuration is invalid or any component creation fails.
func NewOrch(config *Config, logger *slog.Logger, metrics *Metrics) (*orch, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	o := &orch{
		config:  config,
		logger:  logger,
		metrics: metrics,
	}

	scheduler, err := NewScheduler(config.Seed, config.IssuanceRate, config.PDPath, logger.With("component", "scheduler"), metrics)
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	apiServer, err := newAPIServer(&apiServerConfig{
		address:           config.APIAddress,
		readHeaderTimeout: config.APIReadHeaderTimeout,
		fieHandler:        o.fieStreamHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating API server: %w", err)
	}
	o.apiServer = apiServer

	agentServer, err := newAgentServer(&agentServerConfig{
		bufferLength:     config.AgentBufferLength,
		handshakeTimeout: 5 * time.Second,
		address:          config.AgentAddress,
		agentHandler:     o.agentHandler,
		authHandler:      o.agentAuthHandler,
	}, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("error on creating agent server: %w", err)
	}
	o.agentServer = agentServer

	pdQueue, err := structures.NewQueue[api.ProbingDirective](config.PDQueueSize)
	if err != nil {
		return nil, fmt.Errorf("error on creating pd queue: %w", err)
	}
	o.pdQueue = pdQueue

	ringBuffer, err := structures.NewRingBuffer[api.ForwardingInfoElement](config.RingBufferSize)
	if err != nil {
		return nil, fmt.Errorf("error on creating ring buffer: %w", err)
	}
	o.ringBuffer = ringBuffer

	return o, nil
}

func (o *orch) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)
	group.Go(func() error {
		return o.runAPIServer(ctx)
	})
	group.Go(func() error {
		return o.runAgentServer(ctx)
	})
	group.Go(func() error {
		return o.runScheduler(ctx)
	})

	return group.Wait()
}

func (o *orch) runScheduler(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pd := o.scheduler.NextPD()
		if pd == nil {
			continue
		}

		if err := o.pdQueue.TryPush(pd.AgentId, pd); err != nil {
			o.logger.Debug("PD dropped: no queue for agent",
				slog.String("agent_id", pd.AgentId),
				slog.Uint64("pd_id", pd.ProbingDirectiveId))
		} else {
			o.metrics.AgentQueueSize.WithLabelValues(pd.AgentId).Inc()
		}
	}
}

func (o *orch) runAPIServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.apiServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.apiServer.close(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *orch) runAgentServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.agentServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.agentServer.close(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) && !errors.Is(err, ErrServerShutdown) {
		return err
	}
	return nil
}

func (o *orch) fieStreamHandler(s *fieClient) {
	var closeReason string
	consumer := o.ringBuffer.NewConsumer()
	o.metrics.StreamClientsConnected.Inc()
	o.metrics.StreamConnectionsTotal.Inc()
	defer func() {
		consumer.Close()
		o.metrics.StreamClientsConnected.Dec()
		o.metrics.StreamDisconnectionsTotal.WithLabelValues(closeReason).Inc()
		o.logger.Debug("FIE stream closed", slog.String("reason", closeReason))
	}()

	for {
		fie, seq, err := consumer.Pop(s.context())
		if err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		seqFIE := &SequencedFIE{
			ForwardingInfoElement: *fie,
			SequenceNumber:        seq,
		}

		o.logger.Debug("Sending FIE to client",
			slog.Uint64("seq", seq),
			slog.Uint64("pd_id", fie.ProbingDirectiveId))
		if err = s.sendFIE(seqFIE); err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		o.metrics.FIEsStreamedTotal.Inc()
		o.metrics.StreamLagSeconds.Observe(time.Since(time.Unix(0, seqFIE.ProductionTimestampNs)).Seconds())
	}
}

//nolint:funlen
func (o *orch) agentHandler(status *agentAuthStatus, s *agentStream) {
	consumer, err := o.pdQueue.NewConsumer(status.agentID)
	if err != nil {
		o.logger.Warn("Agent already connected, rejecting", "agent_id", status.agentID)
		return
	}
	defer consumer.Close()

	o.logger.Info("Agent connected", "agent_id", status.agentID)
	o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Set(0)
	defer func() {
		o.logger.Info("Agent disconnected", "agent_id", status.agentID)
		o.metrics.AgentQueueSize.DeleteLabelValues(status.agentID)
	}()

	group, ctx := errgroup.WithContext(s.context())

	group.Go(func() error {
		for {
			fie, err := s.receiveFIE()
			if err != nil {
				return err
			}
			o.metrics.FIEsReceivedTotal.WithLabelValues(status.agentID).Inc()

			o.logger.Debug("FIE received",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", fie.ProbingDirectiveId),
				slog.Bool("complete", fie.NearInfo != nil && fie.FarInfo != nil))
			if err := o.scheduler.UpdateFromFIE(fie); err != nil {
				o.logger.Error("Failed to update scheduler from FIE", "agent_id", status.agentID, "err", err)
			}

			allow, err := o.filterFIE(fie)
			if err != nil {
				return fmt.Errorf("error on filtering FIE: %w", err)
			}
			if !allow {
				continue
			}

			_ = o.ringBuffer.Push(fie)
		}
	})

	group.Go(func() error {
		for {
			pd, err := consumer.Pop(ctx)
			if err != nil {
				return err
			}

			o.logger.Debug("Sending PD to agent",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveId),
				slog.String("dest", pd.DestinationAddress))
			if err = s.sendPD(pd); err != nil {
				return err
			}
			o.metrics.PDsSentTotal.WithLabelValues(status.agentID).Inc()
			o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Dec()
		}
	})

	group.Go(func() error {
		<-ctx.Done()
		_ = s.conn.Close()
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		o.logger.Error("Agent stream failed", "agent_id", status.agentID, "err", err)
	}
}

func (o *orch) agentAuthHandler(auth api.AuthRequest) api.AuthResponse {
	if auth.Secret == o.config.Secret {
		return api.AuthResponse{
			Authenticated: true,
			Message:       "authenticated",
		}
	}
	o.logger.Warn("Agent authentication failed")
	return api.AuthResponse{
		Authenticated: false,
		Message:       "secret is not correct",
	}
}

// filterFIE reports whether a FIE should be streamed based on the policy.
// Returns true if the FIE is allowed.
func (o *orch) filterFIE(fie *api.ForwardingInfoElement) (bool, error) {
	switch o.config.FIEFilterPolicy {
	case "any": // allow all FIEs
		return true, nil
	case "both": // allow FIEs with two non-nil response addresses
		return fie.NearInfo != nil && fie.FarInfo != nil, nil
	case "one": // allow FIEs with at least one non-nil response address
		return fie.NearInfo != nil || fie.FarInfo != nil, nil
	default:
		return false, fmt.Errorf("unsupported fie filtering policy: %q", o.config.FIEFilterPolicy)
	}
}
