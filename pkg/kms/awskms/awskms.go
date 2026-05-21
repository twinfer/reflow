// Package awskms wires the Tink AWS KMS integration into Reflow's
// process-global KMS registry. Importing this package (blank or named)
// registers an aws-kms:// KMSClient that resolves any aws-kms:// URI
// against AWS KMS via the AWS SDK v2 default credential chain (env →
// shared credentials → instance metadata → ECS task role → IMDSv2 →
// workload identity).
//
// Operators don't configure this provider in cfg.* — credentials come
// from the environment / IAM role attached to the host, the same as
// every other AWS SDK consumer. URI form follows Tink's convention:
//
//	aws-kms://arn:aws:kms:<region>:<account>:key/<key-id>
//
// Tink's KMSClient registry is process-global and dispatches by URI
// prefix; the broad "aws-kms://" prefix here means any aws-kms URI
// resolves through AWS KMS.
package awskms

import (
	"log/slog"
	"sync"

	awskms "github.com/tink-crypto/tink-go-awskms/v2/integration/awskms"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

const uriPrefix = "aws-kms://"

var registerOnce sync.Once

// Register installs the AWS KMS client in Tink's process-global KMS
// registry. Idempotent via sync.Once. Called automatically from init()
// when this package is imported.
func Register() {
	registerOnce.Do(func() {
		client, err := awskms.NewClientWithOptions(uriPrefix)
		if err != nil {
			// Lazy credentials: Tink's awskms.NewClient initialises an
			// AWS SDK session without resolving credentials yet, so an
			// error here means a malformed prefix — unrecoverable.
			slog.Default().Error("awskms: register failed", "err", err)
			return
		}
		registry.RegisterKMSClient(client)
	})
}

func init() {
	Register()
}
