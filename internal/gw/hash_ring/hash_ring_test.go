package hashring

import (
	"testing"

	"HaystackAtHome/internal/transport"
)

func TestHashRing_ChooseServer(t *testing.T) {
	// Подготовка моковых клиентов
	clients := map[string]*transport.MockSSClient{
		"ss1": transport.NewMockSSClient("127.0.0.1:8081"),
		"ss2": transport.NewMockSSClient("127.0.0.1:8082"),
		"ss3": transport.NewMockSSClient("127.0.0.1:8083"),
	}

	conMap := make(ConnectionMap)
	for name, client := range clients {
		conMap[name] = StorageServer{
			Endpoint: client.Endpoint,
			Client:   client,
		}
	}

	ring := NewMd5Ring(&conMap)
	if ring == nil {
		t.Fatal("Failed to create hash ring")
	}

	// Тест: выбор сервера для разных ключей должен возвращать валидный клиент
	testKeys := []int{1, 42, 100, 999, 12345}
	for _, key := range testKeys {
		client := ring.ChooseServer(key)
		if client == nil {
			t.Errorf("ChooseServer(%d) returned nil", key)
		}
		// Проверяем, что возвращённый клиент — один из моковых
		found := false
		for _, expected := range clients {
			if client == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ChooseServer(%d) returned unknown client", key)
		}
	}
}

func TestHashRing_GetServer(t *testing.T) {
	conMap := make(ConnectionMap)
	mockClient := transport.NewMockSSClient("127.0.0.1:9090")
	conMap["test"] = StorageServer{
		Endpoint: mockClient.Endpoint,
		Client:   mockClient,
	}

	ring := NewMd5Ring(&conMap)

	// Валидный индекс
	client := ring.GetServer(0)
	if client != mockClient {
		t.Error("GetServer(0) should return the mock client")
	}

	// Неверный индекс — должен вернуть nil
	if ring.GetServer(100) != nil {
		t.Error("GetServer(100) should return nil for out-of-range index")
	}
	if ring.GetServer(-1) != nil {
		t.Error("GetServer(-1) should return nil for negative index")
	}
}

func TestHashRing_Empty(t *testing.T) {
	var emptyMap ConnectionMap
	ring := NewMd5Ring(&emptyMap)
	if ring == nil {
		t.Fatal("NewMd5Ring with empty map should not return nil")
	}

	if ring.ChooseServer(42) != nil {
		t.Error("ChooseServer on empty ring should return nil")
	}
	if ring.GetServer(0) != nil {
		t.Error("GetServer on empty ring should return nil")
	}
}
