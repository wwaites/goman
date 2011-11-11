package goman

import (
	"testing"
	"net"
	"http"
	"io/ioutil"
	"strings"
)

const TEST_ROUTER = "localhost:7003"
var TEST_ROUTERS = []string{"localhost:6481", "localhost:6482", "localhost:6483"}

func routerIsUp(hostPort string) bool {
	c, err := net.Dial("tcp", hostPort)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func TestOne(t *testing.T) {
	if !routerIsUp(TEST_ROUTER) {
		t.Fatalf("Can't run unit tests without gearmand running on %s", TEST_ROUTER)
	}

	go func() {
		w := NewClient(TEST_ROUTER)
		if w == nil {
			t.Fatal("Got nil client")
		}
		w.RegisterWorker("geturl", geturl)
		w.Work()
	}()
	c := NewClient(TEST_ROUTER)
	if c == nil {
		t.Fatal("Got nil client")
	}
	res, err := c.Call("geturl", []byte("http://google.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("No response")
	}
	if !strings.Contains ( string(res), "Feeling Lucky" )  {
		t.Fatal("Bad response")
	}
	res, err = c.CallBackground("geturl", []byte("http://google.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("No response")
	}
	status, err := c.GetStatus(res);
	if err != nil {
		t.Fatal(err)
	}
	if !status.Known || !status.Running {
		t.Fatal("Bad status")
	}
}

func TestMore ( t *testing.T ) {
	for _, v := range TEST_ROUTERS {
		if !routerIsUp(v) {
			t.Fatalf("Can't run unit tests without gearmand running on %s", v)
		}
	}
	go func() {
		w := NewLoadBalancedClient(TEST_ROUTERS)
		if w == nil {
			t.Fatal("Got nil client")
		}
		w.RegisterWorker("geturl", geturl)
		w.Work()
	}()
	c := NewLoadBalancedClient(TEST_ROUTERS)
	if c == nil {
		t.Fatal("Got nil client")
	}
	res, err := c.Call("geturl", []byte("http://google.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("No response")
	}
	if !strings.Contains ( string(res), "Feeling Lucky" )  {
		t.Fatal("Bad response")
	}
}


func geturl(job *IncomingJob) []byte {
	url := string(job.Data)
	resp, e := http.Get(url)
	if e != nil {
		return nil
	}
	body, e := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil {
		return nil
	}
	return body
}
