package main

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// leader tracks whether this replica currently holds the aggregator lease so
// pollers can fence themselves.
type leader struct {
	status atomic.Bool
}

func (l *leader) IsLeader() bool { return l.status.Load() }

// startLeaderElection runs client-go leaderelection on the aggregator lease.
// onStarted is cancelled on lease loss (its ctx); poller goroutines are
// spawned from it and stop via ctx. In-cluster only; without a cluster config
// the replica assumes it is the sole leader (local dev).
func startLeaderElection(
	logger *slog.Logger,
	l *leader,
	namespace string,
	onStarted func(ctx context.Context),
) {
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("k8s_config", "error", err)
		l.status.Store(true)
		onStarted(context.Background())
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error("k8s_client", "error", err)
		l.status.Store(true)
		onStarted(context.Background())
		return
	}
	identity, err := os.Hostname()
	if err != nil {
		identity = "aggregator"
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      platform.Env("LEASE_NAME", "aggregator-leader"),
		},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   platform.EnvDurationSeconds("LEASE_DURATION_SECONDS", 15*time.Second),
		RenewDeadline:   platform.EnvDurationSeconds("LEASE_RENEW_SECONDS", 10*time.Second),
		RetryPeriod:     platform.EnvDurationSeconds("LEASE_RETRY_SECONDS", 5*time.Second),
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				correlation := platform.NewCorrelationID()
				logger.Info("leader_acquired", "correlation_id", correlation, "identity", identity)
				l.status.Store(true)
				onStarted(ctx)
			},
			OnStoppedLeading: func() {
				correlation := platform.NewCorrelationID()
				logger.Info("leader_lost", "correlation_id", correlation, "identity", identity)
				l.status.Store(false)
			},
		},
	})
	if err != nil {
		logger.Error("leader_election", "error", err)
		l.status.Store(true)
		onStarted(context.Background())
		return
	}
	go elector.Run(context.Background())
}
