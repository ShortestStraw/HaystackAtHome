package server

import (
	"context"
	"errors"
	"io"
	"sync"

	"HaystackAtHome/internal/ss/models"
	"HaystackAtHome/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultBatchSize = 256
	defaultChunkSize = 256 * 1024 // 256 KiB
	maxChunkSize     = 1 << 20    // 1 MiB cap on client-requested chunk size
)

func (s *Server) GetServiceInfo(ctx context.Context, _ *transport.GetServiceInfoReq) (*transport.GetServiceInfoResp, error) {
	info, err := s.svc.GetServiceInfo(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return &transport.GetServiceInfoResp{
		Uid: info.UID,
		Space: &transport.StorageSpace{
			Total: info.Space.Total,
			Used:  info.Space.Used,
		},
		Features: &transport.StorageFeatures{
			Checksum: info.Features.Checksum,
		},
	}, nil
}

func (s *Server) GetObjsMap(req *transport.GetObjsMapReq, stream grpc.ServerStreamingServer[transport.GetObjsMapResp]) error {
	metas, err := s.svc.GetObjsMap(stream.Context())
	if err != nil {
		return toStatus(err)
	}

	batchSize := int(req.GetBatchSize())
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	for i := 0; i < len(metas); i += batchSize {
		end := i + batchSize
		if end > len(metas) {
			end = len(metas)
		}
		batch := metas[i:end]

		objs := make([]*transport.ObjMapMeta, len(batch))
		for j, m := range batch {
			objs[j] = &transport.ObjMapMeta{Key: m.Key, Size: m.Size}
		}
		if err := stream.Send(&transport.GetObjsMapResp{Obj: objs}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) PutObj(stream grpc.ClientStreamingServer[transport.PutObjReq, transport.PutObjResp]) error {
	// First message must carry the object metadata.
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected meta message: %v", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "first message must contain PutObjMeta")
	}

	ctx := stream.Context()
	w, err := s.svc.PutObj(ctx, meta.Key, meta.Size)
	if err != nil {
		return toStatus(err)
	}

	// Use sync.Once so Close is called exactly once across both the error/cancel
	// path (deferred) and the success path (explicit). The deferred call
	// zero-pads any partial write so the per-volume FIFO queue stays unblocked.
	var (
		once     sync.Once
		closeErr error
	)
	closeWriter := func() { once.Do(func() { closeErr = w.Close() }) }
	defer closeWriter()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Canceled, "recv: %v", err)
		}

		chunk := msg.GetChunk()
		if len(chunk) == 0 {
			continue
		}

		if _, err := w.Write(chunk); err != nil && !errors.Is(err, io.EOF) {
			return status.Errorf(codes.Internal, "write: %v", err)
		}
	}

	closeWriter()
	if closeErr != nil {
		return status.Errorf(codes.Internal, "close: %v", closeErr)
	}
	return stream.SendAndClose(&transport.PutObjResp{})
}

// chunkSender adapts a gRPC server stream to io.Writer so that io.CopyBuffer
// can drive the read loop. Each Write call becomes one stream message; the
// caller controls chunk size by sizing the buffer passed to io.CopyBuffer.
type chunkSender struct {
	stream grpc.ServerStreamingServer[transport.GetObjResp]
}

func (cs *chunkSender) Write(p []byte) (int, error) {
	if err := cs.stream.Send(&transport.GetObjResp{
		Data: &transport.GetObjResp_Chunk{Chunk: p},
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *Server) GetObj(req *transport.GetObjReq, stream grpc.ServerStreamingServer[transport.GetObjResp]) error {
	ctx := stream.Context()
	rc, size, err := s.svc.GetObj(ctx, req.Key)
	if err != nil {
		return toStatus(err)
	}
	defer rc.Close()

	// First message carries the object size so the client can pre-allocate.
	if err := stream.Send(&transport.GetObjResp{
		Data: &transport.GetObjResp_Size{Size: size},
	}); err != nil {
		return err
	}

	// The read chain has no internal buffers: the buf below travels all the way
	// down to the ReadAt syscall on the volume file. One allocation, zero copies.
	chunkSz := int(req.GetChunkSz())
	if chunkSz <= 0 || chunkSz > maxChunkSize {
		chunkSz = defaultChunkSize
	}

	if _, err := io.CopyBuffer(&chunkSender{stream}, rc, make([]byte, chunkSz)); err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	return nil
}

func (s *Server) DelObj(ctx context.Context, req *transport.DelObjReq) (*transport.DelObjResp, error) {
	if err := s.svc.DelObj(ctx, req.Key); err != nil {
		return nil, toStatus(err)
	}
	return &transport.DelObjResp{}, nil
}

// toStatus maps service-layer errors to gRPC status codes.
func toStatus(err error) error {
	switch {
	case errors.Is(err, &models.ErrNotFound{}):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, &models.ErrExists{}):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, &models.ErrNoMem{}):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	case errors.Is(err, &models.ErrInvalidParams{}):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
