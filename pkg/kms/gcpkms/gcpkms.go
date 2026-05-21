// Package gcpkms wires the Tink GCP Cloud KMS integration into
// Reflow's process-global KMS registry. Importing this package (blank
// or named) registers a gcp-kms:// KMSClient that resolves any
// gcp-kms:// URI against GCP Cloud KMS via Google Application Default
// Credentials (env → ADC → metadata server → workload identity).
//
// Operators don't configure this provider in cfg.* — credentials come
// from the environment / service account attached to the host. URI
// form follows Tink's convention:
//
//	gcp-kms://projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>
//
// Tink's KMSClient registry is process-global and dispatches by URI
// prefix; the broad "gcp-kms://" prefix here means any gcp-kms URI
// resolves through GCP KMS.
package gcpkms

import (
	"context"
	"log/slog"

	gcpkms "github.com/tink-crypto/tink-go-gcpkms/v2/integration/gcpkms"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

const uriPrefix = "gcp-kms://"

func init() {
	client, err := gcpkms.NewClientWithOptions(context.Background(), uriPrefix)
	if err != nil {
		slog.Default().Error("gcpkms: register failed", "err", err)
		return
	}
	registry.RegisterKMSClient(client)
}
