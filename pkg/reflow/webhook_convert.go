package reflow

import (
	"errors"
	"fmt"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// webhookSourceProtoFromConfig translates a koanf-loaded WebhookSource
// into a WebhookSourceRecord proto for the bootstrap-seed path.
//
// Rejects inline plaintext (WebhookSource.Secret): cluster persistence
// would write the bytes into Raft snapshot tarballs, defeating the
// point of moving config off-disk. Operators on the legacy
// secret:${env:FOO} koanf shape migrate by setting secret_env: FOO
// (the env name itself is fine in Raft — only the value is sensitive).
//
// Returns an error suitable for surfacing as a reflow.Run startup
// failure.
func webhookSourceProtoFromConfig(src WebhookSource) (*enginev1.WebhookSourceRecord, error) {
	if src.Path == "" {
		return nil, errors.New("path is required")
	}
	name := src.Name
	if name == "" {
		name = src.Path
	}
	if src.Secret != "" {
		return nil, fmt.Errorf("webhook %q: inline 'secret' field is not allowed (cluster-persisted plaintext leaks via Raft snapshots); use 'secret_env' or 'secret_file' instead", name)
	}
	if src.Verifier == "" {
		return nil, fmt.Errorf("webhook %q: verifier is required", name)
	}
	if src.Invocation.Service == "" {
		return nil, fmt.Errorf("webhook %q: invocation.service is required", name)
	}
	if src.Invocation.Handler == "" {
		return nil, fmt.Errorf("webhook %q: invocation.handler is required", name)
	}
	ref, err := buildSecretRef(name, src.SecretEnv, src.SecretFile, src.SecretBlobURI, src.SecretKEKURI)
	if err != nil {
		return nil, err
	}
	return &enginev1.WebhookSourceRecord{
		Name:      name,
		Path:      src.Path,
		Verifier:  src.Verifier,
		SecretRef: ref,
		Service:   src.Invocation.Service,
		Handler:   src.Invocation.Handler,
		ObjectKey: src.Invocation.ObjectKey,
		Metadata:  copyStringMap(src.Invocation.Metadata),
	}, nil
}

func buildSecretRef(name, env, file, blobURI, kekURI string) (*enginev1.SecretRef, error) {
	set := 0
	if env != "" {
		set++
	}
	if file != "" {
		set++
	}
	if blobURI != "" || kekURI != "" {
		set++
	}
	if set > 1 {
		return nil, fmt.Errorf("webhook %q: secret_env, secret_file, and secret_blob_uri/secret_kek_uri are mutually exclusive; set exactly one", name)
	}
	switch {
	case env != "":
		return &enginev1.SecretRef{Source: &enginev1.SecretRef_EnvVarName{EnvVarName: env}}, nil
	case file != "":
		return &enginev1.SecretRef{Source: &enginev1.SecretRef_FilePath{FilePath: file}}, nil
	case blobURI != "" && kekURI != "":
		return &enginev1.SecretRef{Source: &enginev1.SecretRef_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: blobURI,
				KekUri:  kekURI,
			},
		}}, nil
	case blobURI != "":
		return nil, fmt.Errorf("webhook %q: secret_blob_uri set without secret_kek_uri", name)
	case kekURI != "":
		return nil, fmt.Errorf("webhook %q: secret_kek_uri set without secret_blob_uri", name)
	default:
		return nil, fmt.Errorf("webhook %q: secret_env, secret_file, or secret_blob_uri+secret_kek_uri is required", name)
	}
}
