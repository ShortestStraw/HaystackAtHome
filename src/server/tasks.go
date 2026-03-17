package server

type Task struct {
	AccessKey string
	SecretKey string
	TaskType  string
	Handler   TaskHandler
	Server    *HaystackServer
}
