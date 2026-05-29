package raft

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	RoleChanges       prometheus.Counter
	CommittedEntries  prometheus.Counter
	AppliedEntries    prometheus.Counter
	SnapshotsCreated  prometheus.Counter
	ReplicationErrors prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer, nodeID string) *Metrics {
	m := &Metrics{
		RoleChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raftkv_role_changes_total",
			Help:        "Total raft role transitions.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		CommittedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raftkv_committed_entries_total",
			Help:        "Total committed raft log entries.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		AppliedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raftkv_applied_entries_total",
			Help:        "Total applied raft log entries.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		SnapshotsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raftkv_snapshots_created_total",
			Help:        "Total snapshots created.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		ReplicationErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raftkv_replication_errors_total",
			Help:        "Total replication failures.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
	}
	reg.MustRegister(m.RoleChanges, m.CommittedEntries, m.AppliedEntries, m.SnapshotsCreated, m.ReplicationErrors)
	return m
}
