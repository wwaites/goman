package goman

import (
	"fmt"
	"os"
)

type Job struct {
	Handle []byte
	Method string
	Data   []byte
}

func (j Job) String() string {
	return fmt.Sprintf("%s - %s(%s)", j.Handle, j.Method, j.Data)
}

type IncomingJob struct {
	*Job
}

type ProgressHandler interface {
	OnProgress(done int, total int)
}

type Client interface {
	// For being a worker:
	RegisterWorker(method string, handler func(job *IncomingJob) []byte)
	Work() os.Error

	// For being a client:
	Call(method string, data []byte) ([]byte, os.Error)
	CallHighPriority(method string, data []byte) ([]byte, os.Error)
	CallBackground(method string, data []byte) ([]byte, os.Error)
	CallWithProgress(method string, data []byte, progress ProgressHandler) ([]byte, os.Error)
	CallHighPriorityWithProgress(method string, data []byte, progress ProgressHandler) ([]byte, os.Error)
	GetStatus(jobhandle []byte) (*Status, os.Error)
}

type Status struct {
	Handle []byte
	Known, Running bool
	Done, Total int
}

func (s Status) String() string {
	return fmt.Sprintf("%s - known %b running %b done %d total %d", 
		string(s.Handle), s.Known, s.Running, s.Done, s.Total)
}

func (ij *IncomingJob) SendProgress(done int, total int) {
	// TODO: implement
}

func NewClient(hostport string) Client {
	return &client{hosts: []string{hostport}, hostState: make([]hostState, 1)}
}

func NewLoadBalancedClient(hostports []string) Client {
	return &client{hosts: hostports, hostState: make([]hostState, len(hostports))}
}
