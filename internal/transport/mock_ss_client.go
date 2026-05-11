package transport

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockSSClient — моковая реализация SSClient для тестов.
// Все операции возвращают успех, данные "записываются" и "читаются" без реального хранилища.
type MockSSClient struct {
	Endpoint string
	// Внутреннее хранилище для эмуляции состояния между вызовами (опционально)
	storage map[uint64][]byte
}

func NewMockSSClient(endpoint string) *MockSSClient {
	return &MockSSClient{
		Endpoint: endpoint,
		storage:  make(map[uint64][]byte),
	}
}

// === Реализация интерфейса SSClient ===

func (m *MockSSClient) GetServiceInfo(ctx context.Context, in *GetServiceInfoReq, opts ...grpc.CallOption) (*GetServiceInfoResp, error) {
	return &GetServiceInfoResp{
		Uid: 1,
		Space: &StorageSpace{
			Total: 1024 * 1024 * 1024, // 1GB
			Used:  0,
		},
		Features: &StorageFeatures{Checksum: true},
	}, nil
}

func (m *MockSSClient) GetObjsMap(ctx context.Context, in *GetObjsMapReq, opts ...grpc.CallOption) (SS_GetObjsMapClient, error) {
	return &mockGetObjsMapClient{
		storage: m.storage,
		ctx:     ctx,
		done:    false,
	}, nil
}

func (m *MockSSClient) PutObj(ctx context.Context, opts ...grpc.CallOption) (SS_PutObjClient, error) {
	return &mockPutObjClient{
		storage: m.storage,
		ctx:     ctx,
	}, nil
}

func (m *MockSSClient) GetObj(ctx context.Context, in *GetObjReq, opts ...grpc.CallOption) (SS_GetObjClient, error) {
	data, exists := m.storage[in.Key]
	if !exists {
		// Возвращаем 404 через status.Error
		return nil, status.Errorf(codes.NotFound, "object with key %d not found", in.Key)
	}
	return &mockGetObjClient{
		data:     data,
		key:      in.Key,
		ctx:      ctx,
		sentSize: false,
	}, nil
}

func (m *MockSSClient) DelObj(ctx context.Context, in *DelObjReq, opts ...grpc.CallOption) (*DelObjResp, error) {
	delete(m.storage, in.Key)
	return &DelObjResp{}, nil
}

// === Моковые стрим-клиенты ===

type mockPutObjClient struct {
	storage map[uint64][]byte
	ctx     context.Context
	meta    *PutObjMeta
	grpc.ClientStream
}

func (c *mockPutObjClient) Send(req *PutObjReq) error {
	if meta := req.GetMeta(); meta != nil {
		c.meta = meta
	}
	if chunk := req.GetChunk(); chunk != nil && c.meta != nil {
		c.storage[c.meta.Key] = chunk
	}
	return nil
}

func (c *mockPutObjClient) CloseAndRecv() (*PutObjResp, error) {
	return &PutObjResp{}, nil
}

func (c *mockPutObjClient) Context() context.Context {
	return c.ctx
}

type mockGetObjClient struct {
	data     []byte
	key      uint64
	ctx      context.Context
	sentSize bool
	grpc.ClientStream
}

func (c *mockGetObjClient) Recv() (*GetObjResp, error) {
	if !c.sentSize {
		c.sentSize = true
		return &GetObjResp{Data: &GetObjResp_Size{Size: uint64(len(c.data))}}, nil
	}
	if len(c.data) > 0 {
		chunk := c.data
		c.data = nil // отправляем данные один раз
		return &GetObjResp{Data: &GetObjResp_Chunk{Chunk: chunk}}, nil
	}
	return nil, io.EOF
}

func (c *mockGetObjClient) Context() context.Context {
	return c.ctx
}

type mockGetObjsMapClient struct {
	storage map[uint64][]byte
	ctx     context.Context
	done    bool
	grpc.ClientStream
}

func (c *mockGetObjsMapClient) Recv() (*GetObjsMapResp, error) {
	if c.done {
		return nil, io.EOF
	}
	c.done = true

	var objs []*ObjMapMeta
	for k, v := range c.storage {
		objs = append(objs, &ObjMapMeta{Key: k, Size: uint64(len(v))})
	}
	return &GetObjsMapResp{Obj: objs}, nil
}

func (c *mockGetObjsMapClient) Context() context.Context {
	return c.ctx
}
