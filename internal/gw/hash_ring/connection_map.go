package hashring

import (
	"HaystackAtHome/internal/gw/config"
	"HaystackAtHome/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StorageServer struct {
	Endpoint string
	Client   transport.SSClient
}

type ConnectionMap map[string]StorageServer

func ConnMapFromConfig(cfg *config.ClusterConfig) (*ConnectionMap, error) {
	var conMap ConnectionMap = make(map[string]StorageServer, len(cfg.StorageServers))
	for k, v := range cfg.StorageServers {
		conn, err := grpc.NewClient(v.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		ss := StorageServer{
			Client:   transport.NewSSClient(conn),
			Endpoint: v.Endpoint,
		}
		conMap[k] = ss
	}
	return &conMap, nil
}
