/*
Copyright © Max Mazurov (fox.cpp) 2018

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package zrpc

import (
	"testing"
	"time"

	"github.com/pebbe/zmq4"
	"gotest.tools/assert"
)

// TODO: We need tests using multiple goroutines to
// test library for thread-safety.

func setupSockPair(t *testing.T) (client *socket, server *socket) {
	t.Helper()
	client, err := newSocket(zmq4.DEALER)
	assert.NilError(t, err, "newSocket")
	server, err = newSocket(zmq4.ROUTER)
	assert.NilError(t, err, "newSocket")

	assert.NilError(t, server.bind("inproc://" + t.Name()), "server.bind failed")
	assert.NilError(t, client.connect("inproc://" + t.Name()), "client.connect failed")

	return client, server
}

func TestChannelEventRoundtrip(t *testing.T) {
	client, server := setupSockPair(t)

	ev := event{
		Hdr: eventHdr{
			Version: 3,
			MsgID: "5a741c25675b4ae18c7441da24d1f9cf",
		},
		Name: "event_name_goes_here",
		Args: []interface{}{int64(1), int64(2), int64(3)},
	}

	assert.NilError(t, client.writeEvent(&ev, ""), "client.writeEvent failed")

	time.Sleep(250*time.Millisecond)

	// On other side channel should be allocated and event should
	// go into a queue on this channel.
	servCh, prs := server.chans[ev.Hdr.MsgID]
	assert.Equal(t, prs, true, "channel is not present in server's pool")
	assert.Equal(t, len(servCh.queue), 1, "event is not in channel's queue")
	servEv, err := servCh.recvEvent()
	assert.NilError(t, err, "servCh.recvEvent")

	assert.DeepEqual(t, ev, *servEv)
}

func TestChannelEventReply(t *testing.T) {
	client, server := setupSockPair(t)

	ev := event{
		Hdr: eventHdr{
			Version: 3,
			MsgID: "5a741c25675b4ae18c7441da24d1f9cf",
		},
		Name: "event_name_goes_here",
		Args: []interface{}{int64(1), int64(2), int64(3)},
	}
	replyEv := event{
		Hdr: eventHdr{
			Version: 3,
			MsgID: "5a741c25675b4ae18c7441da24d1f9ca",
			ResponseTo: "5a741c25675b4ae18c7441da24d1f9cf",
		},
		Name: "event_name_goes_here",
		Args: []interface{}{int64(1), int64(2), int64(3)},
	}

	clientCh := client.openChannel(ev.Hdr.MsgID, "")
	assert.NilError(t, client.writeEvent(&ev, ""), "client.writeEvent failed")

	time.Sleep(250*time.Millisecond)

	// On other side channel should be allocated and event should
	// go into a queue on this channel.
	servCh, prs := server.chans[ev.Hdr.MsgID]
	assert.Equal(t, prs, true, "channel is not present in server's pool")
	assert.Equal(t, len(servCh.queue), 1, "event is not in channel's queue")
	servEv, err := servCh.recvEvent()
	assert.NilError(t, err, "servCh.recvEvent failed")
	assert.DeepEqual(t, ev, *servEv)

	assert.NilError(t, server.writeEvent(&replyEv, servCh.identity))
	time.Sleep(250*time.Millisecond)
	clientReplyEv, err := clientCh.recvEvent()
	assert.NilError(t, err, "clientCh.recvEvent failed")
	assert.DeepEqual(t, replyEv, *clientReplyEv)
}

func TestSocketUnsupportedVersion(t *testing.T) {
	client, server := setupSockPair(t)
	errCh := make(chan error)
	server.readErrs = errCh

	ev := event{
		Hdr: eventHdr{
			Version: ProtocolVersion + 1,
			MsgID: "5a741c25675b4ae18c7441da24d1f9cf",
		},
		Name: "event_name_goes_here",
		Args: []interface{}{int64(1), int64(2), int64(3)},
	}

	assert.NilError(t, client.writeEvent(&ev, ""), "client.writeEvent failed")

	time.Sleep(250*time.Millisecond)

	select {
	case err := <-errCh:
		if err == nil {
			t.Log("no error reported for incompatible version")
			t.FailNow()
		}
	default:
		t.Log("no error reported for incompatible version")
		t.FailNow()
	}
}