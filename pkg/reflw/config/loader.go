// Package config builds reflw.Config from layered sources via koanf.
//
// The model is: each Source is a koanf.Provider (+ optional parser) that
// emits a slice of the config tree. Sources are layered in the order
// passed to Load; later sources override earlier ones at the per-key
// level. Common sources land under their canonical names below
// (FromFile, FromEnv, FromMap); for production secret backends, pass a
// koanf provider from github.com/knadh/koanf/providers/vault,
// providers/secretsmanager, providers/gcpsecretmanager, etc. directly.
//
// Secrets-as-config: there is intentionally no inline ${secret:key}
// template resolution. The chosen koanf provider populates fields like
// IngressConfig.TLSCert with the actual bytes, and the config tree
// merges naturally with file/env overrides.
package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	koanf "github.com/knadh/koanf/v2"

	"github.com/twinfer/reflw/pkg/reflw"
)

// DefaultEnvPrefix is the prefix the FromEnv helper strips from env
// variable names before mapping to config paths. Override via
// FromEnvPrefix when embedding reflw into a binary with its own
// namespace.
const DefaultEnvPrefix = "REFLW_"

// Source pairs a koanf.Provider with the parser the provider's bytes
// need (or nil when the provider emits a native map[string]any, e.g.
// env, vault, secretsmanager).
type Source struct {
	Provider koanf.Provider
	Parser   koanf.Parser
}

// Load merges sources into a reflw.Config. Later sources override
// earlier ones at the leaf-key level. Returns the merged config and the
// underlying koanf instance for callers that need to read raw keys or
// install additional providers post-hoc.
//
// Validation of required fields (Node.ID > 0, RaftAddr non-empty,
// DataDir non-empty) happens in reflw.Run, not here — the loader is
// composable and a partial config is a legal intermediate state.
func Load(sources ...Source) (reflw.Config, *koanf.Koanf, error) {
	k := koanf.New(".")
	for i, src := range sources {
		if src.Provider == nil {
			return reflw.Config{}, nil, fmt.Errorf("config: source %d: nil provider", i)
		}
		if err := k.Load(src.Provider, src.Parser); err != nil {
			return reflw.Config{}, nil, fmt.Errorf("config: source %d load: %w", i, err)
		}
	}
	var cfg reflw.Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return reflw.Config{}, nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	return cfg, k, nil
}

// FromFile returns a Source for a YAML or JSON file selected by extension.
// Unknown extensions default to YAML (yaml.v3 also accepts JSON).
func FromFile(path string) Source {
	src := Source{Provider: file.Provider(path)}
	if strings.HasSuffix(strings.ToLower(path), ".json") {
		src.Parser = json.Parser()
	} else {
		src.Parser = yaml.Parser()
	}
	return src
}

// FromEnv returns a Source that reads env vars with the default prefix.
// Mapping: REFLW_<SECTION>_<snake_case_field> → section.snake_case_field.
// One underscore separates section from field; underscores inside the
// field name are preserved. Examples:
//
//	REFLW_NODE_ID                 → node.id
//	REFLW_NODE_RAFT_ADDR          → node.raft_addr
//	REFLW_STORAGE_DATA_DIR        → storage.data_dir
//	REFLW_INGRESS_GRPC_ADDR       → ingress.grpc_addr
//	REFLW_LOGGING_LEVEL           → logging.level
//
// For multi-value scalar fields, set the var to a comma-separated list
// and koanf+mapstructure handles the split.
func FromEnv() Source {
	return FromEnvPrefix(DefaultEnvPrefix)
}

// FromEnvPrefix is FromEnv with a caller-chosen prefix. Useful when
// embedding the engine into a binary that owns its own env namespace.
func FromEnvPrefix(prefix string) Source {
	return Source{Provider: env.Provider(prefix, ".", makeEnvNormalizer(prefix))}
}

// makeEnvNormalizer maps "<PREFIX>SECTION_FIELD_WITH_UNDERSCORES" →
// "section.field_with_underscores". The split is on the FIRST
// underscore after the prefix; everything to its right keeps its
// underscores so snake_case field tags match.
func makeEnvNormalizer(prefix string) func(string) string {
	return func(s string) string {
		s = strings.TrimPrefix(s, prefix)
		s = strings.ToLower(s)
		before, after, ok := strings.Cut(s, "_")
		if !ok {
			return s
		}
		return before + "." + after
	}
}

// FromMap returns a Source that loads from an in-memory map. Useful for
// programmatic defaults baked into a binary or for tests. Keys use
// dotted koanf paths (e.g. "ingress.grpc_addr"); the confmap provider
// expands them into the nested form mapstructure expects.
func FromMap(m map[string]any) Source {
	if m == nil {
		m = map[string]any{}
	}
	return Source{Provider: confmap.Provider(m, ".")}
}
