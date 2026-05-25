package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/twinfer/reflow/pkg/reflow/config"
)

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return p
}

func TestLoad_FromYAMLFile(t *testing.T) {
	dir := t.TempDir()
	yaml := `
node:
  id: 7
  raft_addr: "127.0.0.1:5410"
storage:
  data_dir: "/tmp/reflow"
ingress:
  addr: ":8080"
cluster:
  num_partition_shards: 3
`
	cfg, _, err := config.Load(config.FromFile(writeFile(t, dir, "config.yaml", yaml)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != 7 {
		t.Errorf("Node.ID = %d; want 7", cfg.Node.ID)
	}
	if cfg.Node.RaftAddr != "127.0.0.1:5410" {
		t.Errorf("Node.RaftAddr = %q", cfg.Node.RaftAddr)
	}
	if cfg.Storage.DataDir != "/tmp/reflow" {
		t.Errorf("Storage.DataDir = %q", cfg.Storage.DataDir)
	}
	if cfg.Ingress.Addr != ":8080" {
		t.Errorf("Ingress = %+v", cfg.Ingress)
	}
	if cfg.Cluster.NumPartitionShards != 3 {
		t.Errorf("Cluster.NumPartitionShards = %d; want 3", cfg.Cluster.NumPartitionShards)
	}
}

func TestLoad_FromJSONFile(t *testing.T) {
	dir := t.TempDir()
	body := `{"node":{"id":2,"raft_addr":"x:1"},"storage":{"data_dir":"d"}}`
	cfg, _, err := config.Load(config.FromFile(writeFile(t, dir, "c.json", body)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != 2 || cfg.Node.RaftAddr != "x:1" || cfg.Storage.DataDir != "d" {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoad_EnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "c.yaml", "node:\n  id: 1\n  raft_addr: \"file:1\"\nstorage:\n  data_dir: \"file\"\n")
	t.Setenv("REFLOW_NODE_RAFT_ADDR", "env:2")
	t.Setenv("REFLOW_STORAGE_DATA_DIR", "envdata")

	cfg, _, err := config.Load(
		config.FromFile(path),
		config.FromEnv(),
	)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != 1 {
		t.Errorf("Node.ID = %d; want 1 (file)", cfg.Node.ID)
	}
	if cfg.Node.RaftAddr != "env:2" {
		t.Errorf("Node.RaftAddr = %q; want env override", cfg.Node.RaftAddr)
	}
	if cfg.Storage.DataDir != "envdata" {
		t.Errorf("Storage.DataDir = %q; want env override", cfg.Storage.DataDir)
	}
}

func TestLoad_FromEnvOnly(t *testing.T) {
	t.Setenv("REFLOW_NODE_ID", "9")
	t.Setenv("REFLOW_NODE_RAFT_ADDR", ":5410")
	t.Setenv("REFLOW_STORAGE_DATA_DIR", "/var/reflow")
	t.Setenv("REFLOW_INGRESS_ADDR", ":8080")
	t.Setenv("REFLOW_LOGGING_LEVEL", "DEBUG")

	cfg, _, err := config.Load(config.FromEnv())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != 9 {
		t.Errorf("Node.ID = %d; want 9", cfg.Node.ID)
	}
	if cfg.Node.RaftAddr != ":5410" {
		t.Errorf("Node.RaftAddr = %q", cfg.Node.RaftAddr)
	}
	if cfg.Storage.DataDir != "/var/reflow" {
		t.Errorf("Storage.DataDir = %q", cfg.Storage.DataDir)
	}
	if cfg.Ingress.Addr != ":8080" {
		t.Errorf("Ingress.Addr = %q", cfg.Ingress.Addr)
	}
}

func TestLoad_FromMapDefaults(t *testing.T) {
	defaults := map[string]any{
		"node.id":          uint64(1),
		"node.raft_addr":   "127.0.0.1:0",
		"storage.data_dir": "/tmp/d",
		"ingress.addr":     ":0",
	}
	cfg, _, err := config.Load(config.FromMap(defaults))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != 1 || cfg.Node.RaftAddr != "127.0.0.1:0" {
		t.Errorf("Node = %+v", cfg.Node)
	}
	if cfg.Storage.DataDir != "/tmp/d" {
		t.Errorf("Storage.DataDir = %q", cfg.Storage.DataDir)
	}
	if cfg.Ingress.Addr != ":0" {
		t.Errorf("Ingress.Addr = %q", cfg.Ingress.Addr)
	}
}

// TestLoad_SecretsAsConfig demonstrates the secrets-as-config pattern.
// A real production setup would point a koanf provider (Vault, AWS SM,
// GCP SM) at the cred-driver's file/string fields — the
// CertProvider/TLS driver paths consume those via creds.Build. This
// test stands in for that by injecting a path through ingress.creds
// and checking the value lands in the right place.
func TestLoad_SecretsAsConfig(t *testing.T) {
	cfg, _, err := config.Load(
		config.FromMap(map[string]any{
			"node.id":                     uint64(1),
			"node.raft_addr":              ":1",
			"storage.data_dir":            "/tmp/d",
			"ingress.creds.driver":        "tls",
			"ingress.creds.tls.cert_file": "/etc/reflow/ingress.crt",
			"ingress.creds.tls.key_file":  "/etc/reflow/ingress.key",
			"ingress.creds.tls.ca_file":   "/etc/reflow/ca.crt",
		}),
	)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Ingress.Creds.TLS == nil {
		t.Fatal("Ingress.Creds.TLS unmarshalled nil")
	}
	if cfg.Ingress.Creds.TLS.CertFile != "/etc/reflow/ingress.crt" {
		t.Errorf("Creds.TLS.CertFile = %q", cfg.Ingress.Creds.TLS.CertFile)
	}
}

func TestLoad_LayeringOrderLastWins(t *testing.T) {
	a := config.FromMap(map[string]any{
		"node.id":          uint64(1),
		"node.raft_addr":   "first",
		"storage.data_dir": "first",
	})
	b := config.FromMap(map[string]any{
		"node.raft_addr": "second",
	})
	c := config.FromMap(map[string]any{
		"node.raft_addr": "third",
	})
	cfg, _, err := config.Load(a, b, c)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.RaftAddr != "third" {
		t.Errorf("RaftAddr = %q; want third (last source wins)", cfg.Node.RaftAddr)
	}
	if cfg.Storage.DataDir != "first" {
		t.Errorf("DataDir = %q; want first (only set there)", cfg.Storage.DataDir)
	}
}

func TestLoad_RejectsNilProvider(t *testing.T) {
	_, _, err := config.Load(config.Source{})
	if err == nil {
		t.Fatal("expected error for nil provider")
	}
}
