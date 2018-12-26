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

	"gotest.tools/assert"
)

// Point of reference for comparsion. Sending pile of zeros
// over a socket in one direction.
//func BenchmarkZeroMQReference(b *testing.B) {
//	client, err := zmq4.NewSocket(zmq4.DEALER)
//	assert.NilError(b, err)
//	serv, err := zmq4.NewSocket(zmq4.ROUTER)
//	assert.NilError(b, err)
//
//	defer serv.Close()
//	defer client.Close()
//
//	endpoint := fmt.Sprintf("inproc://bench-%s", randomString(32))
//	assert.NilError(b, serv.Bind(endpoint))
//	assert.NilError(b, client.Connect(endpoint))
//
//	msg := make([]byte, 128)
//
//	for i := 0; i < b.N; i++ {
//		_, err := client.SendBytes(msg, 0)
//		assert.NilError(b, err)
//		_, err = serv.RecvMessageBytes(0)
//		assert.NilError(b, err)
//	}
//}

// Simple ping-pong benchmark over a single channel.
//
// Pretty far from being realistic but still gives a
// lot of useful information.
func BenchmarkSingleChannelPingPong(b *testing.B) {
	client, server := setupSockPair(b)
	defer server.Close()
	defer client.Close()

	ok := make(chan bool)
	var servCh *channel
	server.newChannel = func(ch *channel) {
		servCh = ch
		ok <- true
	}

	ev := &event{Name: "TESTING", Args: 12345}

	clientCh, err := client.OpenSendEvent(ev)
	assert.NilError(b, err)

	<-ok

	for i := 0; i < b.N; i++ {
		// Ping.
		assert.NilError(b, clientCh.SendEvent(ev))
		_, err := servCh.RecvEvent()
		assert.NilError(b, err)

		// Pong.
		assert.NilError(b, servCh.SendEvent(ev))
		_, err = clientCh.RecvEvent()
		assert.NilError(b, err)
	}
}

// Similar to BenchmarkSingleChannelPingPong but no second event is send and messages are
// simply discarded at server side.
func BenchmarkSingleChannelFlood(b *testing.B) {
	client, server := setupSockPair(b)
	defer server.Close()
	defer client.Close()

	ok := make(chan bool)
	var servCh *channel
	server.newChannel = func(ch *channel) {
		servCh = ch
		ok <- true
	}

	ev := &event{Name: "TESTING", Args: 12345}

	clientCh, err := client.OpenSendEvent(ev)
	assert.NilError(b, err)

	<-ok

	// Hackish method, redirect queue into void.
	go func() {
		for range servCh.queue {
			// do nothing
		}
	}()

	for i := 0; i < b.N; i++ {
		assert.NilError(b, clientCh.SendEvent(ev))
	}
}

func BenchmarkSingleChannelPipe(b *testing.B) {
	client, server := setupSockPair(b)
	defer server.Close()
	defer client.Close()

	ok := make(chan bool)
	var servCh *channel
	server.newChannel = func(ch *channel) {
		servCh = ch
		ok <- true
	}

	ev := &event{Name: "TESTING", Args: 12345}

	clientCh, err := client.OpenSendEvent(ev)
	assert.NilError(b, err)

	<-ok

	for i := 0; i < b.N; i++ {
		assert.NilError(b, clientCh.SendEvent(ev))
		_, err := servCh.RecvEvent()
		assert.NilError(b, err)
	}
}

// Creating a channel to send one message and then discard it.
// This benchmark is much more close to real usage pattern.
func BenchmarkChannelClusterfuck(b *testing.B) {
	client, server := setupSockPair(b)
	defer server.Close()
	defer client.Close()

	ok := make(chan bool)
	var servCh *channel
	server.newChannel = func(ch *channel) {
		servCh = ch
		ok <- true
	}

	for i := 0; i < b.N; i++ {
		ev := &event{Name: "TESTING", Args: 12345}

		clientCh, err := client.OpenSendEvent(ev)
		assert.NilError(b, err)

		<-ok

		_, err = servCh.RecvEvent()
		assert.NilError(b, err)

		assert.NilError(b, servCh.SendEvent(ev))

		_, err = clientCh.RecvEvent()
		assert.NilError(b, err)

		server.CloseChannel(servCh.id)
		client.CloseChannel(clientCh.id)
	}
}
