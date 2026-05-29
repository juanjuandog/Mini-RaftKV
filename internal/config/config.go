package config

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID                  string            `yaml:"id"`
	Addr                string            `yaml:"addr"`
	MetricsAddr         string            `yaml:"metrics_addr"`
	DataPath            string            `yaml:"data_path"`
	Peers               map[string]string `yaml:"peers"`
	ElectionTicks       int               `yaml:"election_ticks"`
	ElectionJitterTicks int               `yaml:"election_jitter_ticks"`
	HeartbeatTicks      int               `yaml:"heartbeat_ticks"`
	SnapshotThreshold   uint64            `yaml:"snapshot_threshold"`
}

func Load(path string) (NodeConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return NodeConfig{}, err
	}
	var cfg NodeConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return NodeConfig{}, err
	}
	cfg.ApplyDefaults()
	return cfg, nil
}

func (c *NodeConfig) ApplyDefaults() {
	if c.ID == "" {
		c.ID = "n1"
	}
	if c.Addr == "" {
		c.Addr = "127.0.0.1:7001"
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = "127.0.0.1:9001"
	}
	if c.DataPath == "" {
		c.DataPath = "data/" + c.ID + ".db"
	}
	if c.Peers == nil {
		c.Peers = map[string]string{}
	}
	delete(c.Peers, c.ID)
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.ElectionJitterTicks == 0 {
		c.ElectionJitterTicks = c.ElectionTicks
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 2
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1024
	}
}

func ParsePeers(raw string, self string) map[string]string {
	peers := map[string]string{}
	if raw == "" {
		return peers
	}
	for _, part := range strings.Split(raw, ",") {
		pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" || pair[0] == self {
			continue
		}
		peers[pair[0]] = pair[1]
	}
	return peers
}
