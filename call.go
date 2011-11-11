// The "caller" side of a Gearman client.

//        SUBMIT_JOB = 7                   // =>  [ 'I', "submit_job" ],    # C->J  FUNC[0]UNIQ[0]ARGS
//        SUBMIT_JOB_HIGH = 21             // =>  [ 'I', "submit_job_high" ],    # C->J  FUNC[0]UNIQ[0]ARGS
//        SUBMIT_JOB_BG = 18               // => [ 'I', "submit_job_bg" ], # C->J     " "   "  " "

//        JOB_CREATED = 8                  // =>  [ 'O', "job_created" ], # J->C HANDLE

//        WORK_STATUS = 12                 // => [ 'IO',  "work_status" ],   # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
//        WORK_COMPLETE = 13               // => [ 'IO',  "work_complete" ], # W->J/C: HANDLE[0]RES
//        WORK_FAIL = 14                   // => [ 'IO',  "work_fail" ],     # W->J/C: HANDLE

//        GET_STATUS = 15                  // => [ 'I',  "get_status" ],  # C->J: HANDLE
//        STATUS_RES = 20                  // => [ 'O',  "status_res" ],  # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM

//        ECHO_REQ = 16                    // => [ 'I',  "echo_req" ],    # ?->J TEXT
//        ECHO_RES = 17                    // => [ 'O',  "echo_res" ],    # J->? TEXT

//        ERROR = 19                       // => [ 'O',  "error" ],       # J->? ERRCODE[0]ERR_T
package goman

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"rand"
	"strconv"
	"time"
)

func (c *client) Call(method string, data []byte) ([]byte, os.Error) {
	return c.call(method, data, false, false, nil)
}

func (c *client) CallHighPriority(method string, data []byte) ([]byte, os.Error) {
	return c.call(method, data, true, false, nil )
}

func (c *client) CallBackground(method string, data []byte) ([]byte, os.Error) {
	return c.call ( method, data, false, true, nil )
}

func (c *client) CallWithProgress(method string, data []byte, progress ProgressHandler) ([]byte, os.Error) {
	return c.call(method, data, false, false, progress )
}

func (c *client) CallHighPriorityWithProgress(method string, data []byte, progress ProgressHandler) ([]byte, os.Error) {
	return c.call ( method, data, true, false, progress )
}

func (c *client) call(method string, data []byte, highprio bool, background bool, progress ProgressHandler) (response []byte, err os.Error) {
	maxtries := len(c.hosts) * 2
	var n net.Conn = nil
	var jobhandle []byte = nil
	rand.Seed(time.Nanoseconds())

	// find a jobserver that will handle this method
	for maxtries > 0 {
		maxtries--
		rnum := rand.Intn(len(c.hosts))
		// is this conn alive?
		n = c.hostState[rnum].conn
		if n == nil || n.RemoteAddr() == nil {
			n, err = net.Dial("tcp", c.hosts[rnum])
			if err != nil {
				//log.Println ( "finding a job server " + e.String() )
				continue
			}
		}
		// submit job packet
		buf := []byte(method)
		buf = append(buf, 0)
		c.id = "jfidfjid"
		buf = append(buf, []byte(c.id)...)
		buf = append(buf, 0)
		buf = append(buf, data...)
		job_type := SUBMIT_JOB
		if highprio {
			job_type = SUBMIT_JOB_HIGH
		}
		if background {
			job_type = SUBMIT_JOB_BG
		}
		if _, err = n.Write(make_req(job_type, buf)); err != nil {
			n.Close()
			continue
		}
		cmd, cmd_len, to, err := read_header(n)
		if err != nil || to {
			n.Close()
			continue
		}
		data := make([]byte, cmd_len)
		if _, err = io.ReadFull(n, data); err != nil {
			n.Close()
			continue
		}
		if cmd != JOB_CREATED {
			continue
		}
		jobhandle = data
		break
	}
	if jobhandle == nil {
		return
	}

	if background {
		response = jobhandle
		return
	}
	
	for {
		cmd, cmd_len, to, err := read_header(n)
		if err != nil {
			return
		}
		data := make([]byte, cmd_len)
		if _, err = io.ReadFull(n, data); err != nil {
			return
		}
		if to {
			continue
		}
		switch cmd {
		case WORK_COMPLETE:
			if len(data) == 0 {
				return
			}
			a := bytes.SplitN(data, []byte{0}, 2)
			if len(a) != 2 {
				return
			}
			response = a[1]
			return
		}
	}
	return
}

func (c *client) GetStatus(jobhandle []byte) (status *Status, err os.Error) {
	maxtries := len(c.hosts) * 2
	var n net.Conn = nil
	rand.Seed(time.Nanoseconds())

	req := make_req(GET_STATUS, jobhandle)

	// find a jobserver that will handle this method
	for maxtries > 0 {
		maxtries--
		rnum := rand.Intn(len(c.hosts))
		// is this conn alive?
		n = c.hostState[rnum].conn
		if n == nil || n.RemoteAddr() == nil {
			n, err = net.Dial("tcp", c.hosts[rnum])
			if err != nil {
				//log.Println ( "finding a job server " + e.String() )
				continue
			}
		}

		_, err = n.Write(req)
		if err != nil {
			n.Close()
			continue
		}
		cmd, cmd_len, to, err := read_header(n)
		if err != nil || to {
			n.Close()
			continue
		}
		data := make([]byte, cmd_len)
		_, err = io.ReadFull(n, data)
		if err != nil {
			n.Close()
			continue
		}

		switch cmd {
		case STATUS_RES:
		default:
			err = os.NewError(fmt.Sprintf("Bad GET_STATUS response type: %d", cmd))
			continue
		}

		status = &Status{}
		null := []byte{0}
		if eor := bytes.Index(data, null); eor > 0 {
			status.JobHandle = data[:eor]
			if len(data) < eor+1 {
				err = os.NewError("Truncated GET_STATUS response")
				return
			}
			data = data[eor+1:]
		}
		if len(data) < 4 {
			err = os.NewError("Truncated GET_STATUS response")
			return
		}
		if data[0] == 49 { // "1"
			status.Known = true
		}
		if data[2] == 49 { // "1"
			status.Running = true
		}
		data = data[4:]
		if eor := bytes.Index(data, null); eor > 0 {
			status.Done, _ = strconv.Atoi(string(data[:eor]))
			if len(data) < eor+1 {
				err = os.NewError("Truncated GET_STATUS response")
				return
			}
			data = data[eor+1:]
		}
		status.Total, _ = strconv.Atoi(string(data))

		break
	}
	return
}