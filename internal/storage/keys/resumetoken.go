package keys

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// Resume tokens are the externally-completable-task analog of awakeable ids (see
// AwakeableOwnerPartitionKey): a self-routing handle for a parked BPMN user task
// / CMMN human task that an external caller round-trips to complete it. Unlike an
// awakeable id the token is fully deterministic — the same parked task always
// mints the same token — so it needs no stored directory row and never traverses
// Raft: ingress mints it on a GetProcessInstance read and decodes it on
// DeliverProcessEvent. The owner partition_key is embedded first so ingress routes
// to the owning shard from the token alone, exactly like an awakeable id.
const (
	resumeTokenPrefix  = "rpt_"
	resumeTokenVersion = 1
)

// ResumeTarget is the addressing tuple a resume token decodes to: the owning
// instance's routing partition_key plus the (service, instance_key, node_id)
// needed to build a ProcessEvent for the parked task. NodeID is a BPMN flow-node
// id or a CMMN replicaKey ("pi1#cfi[0]").
type ResumeTarget struct {
	PartitionKey uint64
	Service      string
	InstanceKey  string
	NodeID       string
}

// MintResumeToken encodes a ResumeTarget as "rpt_" + base64url(version | pk |
// service | instance_key | node_id) — each string u16-length-prefixed, all
// integers big-endian, RawURLEncoding so the token is URL-safe for the
// /v1/tasks/{token} REST path. Returns an error only when a field exceeds the
// u16 length ceiling (unreachable for real model names / instance keys / node
// ids).
func MintResumeToken(pk uint64, service, instanceKey, nodeID string) (string, error) {
	body := make([]byte, 0, 1+8+6+len(service)+len(instanceKey)+len(nodeID))
	body = append(body, resumeTokenVersion)
	var pkb [8]byte
	binary.BigEndian.PutUint64(pkb[:], pk)
	body = append(body, pkb[:]...)
	var err error
	if body, err = appendU16String(body, "service", service); err != nil {
		return "", err
	}
	if body, err = appendU16String(body, "instance_key", instanceKey); err != nil {
		return "", err
	}
	if body, err = appendU16String(body, "node_id", nodeID); err != nil {
		return "", err
	}
	return resumeTokenPrefix + base64.RawURLEncoding.EncodeToString(body), nil
}

// DecodeResumeToken reverses MintResumeToken, validating the prefix, version, and
// framing. Any malformation is an error the caller should surface as
// InvalidArgument.
func DecodeResumeToken(tok string) (ResumeTarget, error) {
	if len(tok) <= len(resumeTokenPrefix) || tok[:len(resumeTokenPrefix)] != resumeTokenPrefix {
		return ResumeTarget{}, fmt.Errorf("resume token must start with %q", resumeTokenPrefix)
	}
	body, err := base64.RawURLEncoding.DecodeString(tok[len(resumeTokenPrefix):])
	if err != nil {
		return ResumeTarget{}, fmt.Errorf("resume token body decode: %w", err)
	}
	// version(1) + pk(8) + the three u16 length prefixes(6) is the minimum.
	if len(body) < 1+8+6 {
		return ResumeTarget{}, fmt.Errorf("resume token body length = %d; too short", len(body))
	}
	if body[0] != resumeTokenVersion {
		return ResumeTarget{}, fmt.Errorf("resume token version = %d; want %d", body[0], resumeTokenVersion)
	}
	rest := body[1:]
	pk := binary.BigEndian.Uint64(rest[:8])
	rest = rest[8:]

	service, rest, err := readU16String("service", rest)
	if err != nil {
		return ResumeTarget{}, err
	}
	instanceKey, rest, err := readU16String("instance_key", rest)
	if err != nil {
		return ResumeTarget{}, err
	}
	nodeID, rest, err := readU16String("node_id", rest)
	if err != nil {
		return ResumeTarget{}, err
	}
	if len(rest) != 0 {
		return ResumeTarget{}, fmt.Errorf("resume token has %d trailing byte(s)", len(rest))
	}
	return ResumeTarget{PartitionKey: pk, Service: service, InstanceKey: instanceKey, NodeID: nodeID}, nil
}

// appendU16String appends a u16 big-endian length prefix followed by s.
func appendU16String(dst []byte, field, s string) ([]byte, error) {
	if len(s) > 0xFFFF {
		return nil, fmt.Errorf("resume token %s length = %d exceeds %d", field, len(s), 0xFFFF)
	}
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(s)))
	dst = append(dst, n[:]...)
	return append(dst, s...), nil
}

// readU16String reads a u16-length-prefixed string from the front of b, returning
// the string and the bytes after it.
func readU16String(field string, b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, fmt.Errorf("resume token truncated reading %s length", field)
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	b = b[2:]
	if len(b) < n {
		return "", nil, fmt.Errorf("resume token truncated reading %s (want %d, have %d)", field, n, len(b))
	}
	return string(b[:n]), b[n:], nil
}
