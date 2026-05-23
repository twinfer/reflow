package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	connect "connectrpc.com/connect"

	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
)

// uploadFsync, when false in tests, skips fsync calls so the suite runs
// fast on macOS APFS without per-file sync stalls. Production always
// fsyncs.
var uploadFsync = true

// UploadLPTransferSST is the client-streaming entrypoint for the LP
// transfer SST-shipping path. The first frame MUST be a header; the
// server writes subsequent chunks into
// `<DataDir>/p<dest_shard>/state.lpstage_in/<transfer_id>/<namespace>.sst.tmp`,
// verifies size + sha256, fsyncs, and atomically renames to `.sst`.
//
// Accepts on any replica that hosts dest_shard (not leader-only): the
// apply-time Pebble Ingest is replica-local, so every replica needs
// the file. The source fans out to all replicas in parallel; on a
// node that does not host dest_shard at all we return NotLeader as a
// pre-flight redirect to where dest_shard's metadata lives.
func (s *Server) UploadLPTransferSST(
	ctx context.Context,
	stream *connect.ClientStream[deliveryv1.UploadLPTransferSSTRequest],
) (*connect.Response[deliveryv1.UploadLPTransferSSTResponse], error) {
	// First frame must be the header.
	if !stream.Receive() {
		if err := stream.Err(); err != nil {
			return nil, err
		}
		return uploadErr("empty upload stream"), nil
	}
	headMsg := stream.Msg()
	hdr := headMsg.GetHeader()
	if hdr == nil {
		return uploadErr("first frame must be header"), nil
	}
	if hdr.GetDestShard() == 0 ||
		hdr.GetTransferId() == "" ||
		hdr.GetNamespace() == "" {
		return uploadErr("malformed header"), nil
	}
	if !validRelativeName(hdr.GetTransferId()) || !validRelativeName(hdr.GetNamespace()) {
		return uploadErr("transfer_id or namespace contains path separators"), nil
	}

	dataDir, ok := s.host.PartitionDataDir(hdr.GetDestShard())
	if !ok {
		return s.uploadNotLeader(hdr.GetDestShard()), nil
	}

	stageDir := dataDir + ".lpstage_in"
	transferDir := filepath.Join(stageDir, hdr.GetTransferId())
	if err := os.MkdirAll(transferDir, 0o755); err != nil {
		s.log.Warn("delivery upload: mkdir transfer dir", "dir", transferDir, "err", err)
		return uploadErr(fmt.Sprintf("mkdir: %v", err)), nil
	}

	tmpPath := filepath.Join(transferDir, hdr.GetNamespace()+".sst.tmp")
	finalPath := filepath.Join(transferDir, hdr.GetNamespace()+".sst")
	f, err := os.Create(tmpPath)
	if err != nil {
		s.log.Warn("delivery upload: create tmp", "path", tmpPath, "err", err)
		return uploadErr(fmt.Sprintf("create tmp: %v", err)), nil
	}
	hasher := sha256.New()
	var written uint64
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}

	for stream.Receive() {
		if err := ctx.Err(); err != nil {
			cleanup()
			return nil, err
		}
		msg := stream.Msg()
		if h := msg.GetHeader(); h != nil {
			cleanup()
			return uploadErr("header after chunk"), nil
		}
		chunk := msg.GetChunk()
		if len(chunk) == 0 {
			continue
		}
		if _, err := f.Write(chunk); err != nil {
			cleanup()
			return uploadErr(fmt.Sprintf("write: %v", err)), nil
		}
		hasher.Write(chunk)
		written += uint64(len(chunk))
		if hdr.GetSizeBytes() > 0 && written > hdr.GetSizeBytes() {
			cleanup()
			return uploadErr(fmt.Sprintf("size overflow: got %d want %d", written, hdr.GetSizeBytes())), nil
		}
	}
	if err := stream.Err(); err != nil {
		cleanup()
		return nil, err
	}
	if hdr.GetSizeBytes() > 0 && written != hdr.GetSizeBytes() {
		cleanup()
		return uploadErr(fmt.Sprintf("size mismatch: got %d want %d", written, hdr.GetSizeBytes())), nil
	}
	if want := hdr.GetSha256Hex(); want != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(got, want) {
			cleanup()
			return uploadErr(fmt.Sprintf("sha256 mismatch: got %s want %s", got, want)), nil
		}
	}
	if uploadFsync {
		if err := f.Sync(); err != nil {
			cleanup()
			return uploadErr(fmt.Sprintf("fsync: %v", err)), nil
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return uploadErr(fmt.Sprintf("close: %v", err)), nil
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return uploadErr(fmt.Sprintf("rename: %v", err)), nil
	}

	relative := hdr.GetNamespace() + ".sst"
	return connect.NewResponse(&deliveryv1.UploadLPTransferSSTResponse{
		Kind: &deliveryv1.UploadLPTransferSSTResponse_Ack{
			Ack: &deliveryv1.UploadLPTransferSSTAck{RelativePath: relative},
		},
	}), nil
}

// uploadNotLeader builds a NotLeader UploadLPTransferSSTResponse, populating
// leader_node_id from gossip when available.
func (s *Server) uploadNotLeader(destShard uint64) *connect.Response[deliveryv1.UploadLPTransferSSTResponse] {
	leaderID, _ := s.host.PartitionLeaderHint(destShard)
	return connect.NewResponse(&deliveryv1.UploadLPTransferSSTResponse{
		Kind: &deliveryv1.UploadLPTransferSSTResponse_NotLeader{
			NotLeader: &deliveryv1.NotLeader{LeaderNodeId: leaderID},
		},
	})
}

func uploadErr(msg string) *connect.Response[deliveryv1.UploadLPTransferSSTResponse] {
	return connect.NewResponse(&deliveryv1.UploadLPTransferSSTResponse{
		Kind: &deliveryv1.UploadLPTransferSSTResponse_Err{
			Err: &deliveryv1.Err{Message: msg},
		},
	})
}

// validRelativeName rejects names containing "/" or ".." so we cannot
// be tricked into writing outside the per-transfer staging directory.
// "transfer_id" and "namespace" are both single path segments by
// design; the server never accepts a relative path with separators.
func validRelativeName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}
