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
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/pkg/errors"
)

const (
	HeartbeatEvent = "_zpc_hb"
	WindowSize = "_zpc_more"
)

// ErrIncompatibleVersion is returned when version number in received message
// doesn't matches protocol version implemented by library.
type ErrIncompatibleVersion struct {
	remoteVer int
}

func (ive ErrIncompatibleVersion) Error() string{
	return fmt.Sprintf("zrpc: incompatible version: %d (theirs) != %d (ours)", ive.remoteVer, ProtocolVersion)
}

type socket struct {
	ioLck, chansLock sync.RWMutex
	sock             *zmq4.Socket
	chans            map[string]*channel

	// Written to by sendEventFromPipe, read by writeEvent.
	pipeErrs chan error

	// Where all errors occurred during polling go, nil by default.
	readErrs chan<- error

	pipeIn, pipeOut *zmq4.Socket

	dispatchPoller *zmq4.Poller

	noVersionCheck bool
	chanBufferSize uint64
}

func newSocket(t zmq4.Type) (s *socket, err error) {
	s = &socket{
		chans: map[string]*channel{},
		pipeErrs: make(chan error),
		readErrs: nil,
	}
	s.sock, err = zmq4.NewSocket(t)
	if err != nil {
		return nil, errors.Wrap(err, "new sock")
	}

	s.pipeOut, err = zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		return nil, errors.Wrap(err, "new sock")
	}
	if err := s.pipeOut.Bind("inproc://zrpc-"+fmt.Sprintf("%p", s)); err != nil {
		return nil, errors.Wrap(err, "new sock")
	}
	s.pipeIn, err = zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		return nil, errors.Wrap(err, "new sock")
	}
	if err := s.pipeIn.Connect("inproc://zrpc-"+fmt.Sprintf("%p", s)); err != nil {
		return nil, errors.Wrap(err, "new sock")
	}
	s.dispatchPoller = zmq4.NewPoller()
	s.dispatchPoller.Add(s.pipeOut, zmq4.POLLIN)
	s.dispatchPoller.Add(s.sock, zmq4.POLLIN)

	s.chanBufferSize = 10

	go s.pollMessages()

	return s, nil
}

func (s *socket) connect(endpoint string) error {
	return s.sock.Connect(endpoint)
}

func (s *socket) bind(endpoint string) error {
	return s.sock.Bind(endpoint)
}

func (s *socket) Close() error {
	return s.sock.Close()
}

// pollMessages is main function that
func (s *socket) pollMessages() {
	// In ZeroMQ world we can't share sockets between threads so this function
	// does two things:
	// 1. It polls ZMQ in-proc "pipe" for events that should be sent to main socket.
	// 2. It polls main socket for input events.

	for {
		polled, err := s.dispatchPoller.Poll(-1)
		if err != nil {
			if err == zmq4.ErrorSocketClosed {
				return
			}
			s.reportError(err)
		}

		for _, pollRes := range polled {
			switch sock := pollRes.Socket; sock {
			case s.pipeOut:
				s.sendEventFromPipe()
			case s.sock:
				s.dispatchEvent()
			}
		}
	}
}

func (s *socket) sendEventFromPipe() {
	arr, err := s.pipeOut.RecvMessageBytes(0)
	if err != nil {
		s.pipeErrs<-err
	}

	// Weird, but we can't pass [][]byte to SendMessage.
	iarr := make([]interface{}, len(arr))
	for i, part := range arr {
		iarr[i] = part
	}

	if _, err := s.sock.SendMessage(iarr...); err != nil {
		s.pipeErrs<-err
	}
	s.pipeErrs<-nil
}

// readEvent reads and parses event sent on socket.
// Returned identity is source peer's identity if socket have ROUTER type.
func (s *socket) readEvent() (ev *event, identity string, err error) {
	arr, err := s.sock.RecvMessageBytes(0)
	if err != nil {
		return nil, "", errors.Wrap(err, "event read (IO err)")
	}

	ev = new(event)
	if err := ev.UnmarshalBinary(bytes.NewReader(arr[len(arr)-1])); err != nil {
		return nil, "", errors.Wrap(err, "event read")
	}

	if len(arr) > 1 {
		return ev, string(arr[0]), nil
	}

	return ev, "", nil
}

// writeEvent schedules event to be sent to peer identified by passed address (should always be empty for
// client-side socket). It also implicitly creates queue for channel on which event is sent.
func (s *socket) writeEvent(ev *event, identity string) error {
	evBin, err := ev.MarshalBinary()
	if err != nil {
		return errors.Wrap(err,"event write")
	}

	s.ioLck.Lock()
	defer s.ioLck.Unlock()
	if identity == "" {
		_, err = s.pipeIn.SendMessage(evBin)
	} else {
		_, err = s.pipeIn.SendMessage(identity, "", evBin)
	}

	if err != nil {
		return errors.Wrap(err, "event write")
	}

	if ev.Hdr.ResponseTo != "" {
		s.openChannel(ev.Hdr.ResponseTo, identity)
	} else {
		s.openChannel(ev.Hdr.MsgID, identity)
	}

	return <-s.pipeErrs
}

// closeChannel allocates queue for certain channel and also links identity (address) with
// it, if identity != "". If queue for channel already exists - function does nothing.
// Returned value is channel queue.
func (s *socket) openChannel(id, identity string) *channel {
	s.chansLock.Lock()
	defer s.chansLock.Unlock()

	if ch, prs := s.chans[id]; prs {
		return ch
	}

	ch := newChannel(s, id)
	if identity != "" {
		ch.identity = identity
	}
	s.chans[id] = ch
	return ch
}

// closeChannel frees all resources associated with certain channel.
func (s *socket) closeChannel(id string) {
	s.chansLock.Lock()
	defer s.chansLock.Unlock()

	ch, prs := s.chans[id]
	if !prs {
		return
	}
	delete(s.chans, id)

	ch.flushQueue()
}

// dispatchEvents reads event from socket and sends it to corresponding channel's queue
// (creating new one if it doesn't exists yet).
func (s *socket) dispatchEvent() {
	ev, identity, err := s.readEvent()
	if err != nil {
		s.reportError(errors.Wrap(err, "dispatch event"))
	}

	if !s.noVersionCheck && ev.Hdr.Version != ProtocolVersion {
		s.reportError(errors.Wrap(ErrIncompatibleVersion{remoteVer: ev.Hdr.Version}, "dispatch event"))
	}

	var ch *channel
	if ev.Hdr.ResponseTo != "" {
		var prs bool
		s.chansLock.RLock()
		ch, prs = s.chans[ev.Hdr.ResponseTo]
		s.chansLock.RUnlock()
		if !prs {
			s.reportError(errors.New("dispatch event: received reply for message we didn't sent"))
		}
	} else {
		ch = s.openChannel(ev.Hdr.MsgID, identity)
	}

	if err := ch.handle(ev); err != nil {
		select {
		case ch.errorCh <- errors.Wrap(err, "dispatch event"):
		default:
		}
	}
}

// reportError makes all currently running channel.recvEvent() calls
// return a passed error.
func (s *socket) reportError(err error) {
	s.readErrs <- err

	s.chansLock.RLock()
	defer s.chansLock.RUnlock()

	for _, ch := range s.chans {
		select {
		case ch.errorCh<-err:
		default:
		}
	}
}

type channel struct {
	parent *socket
	id, identity string
	queue        chan *event
	errorCh      chan error
	lastHeartbeat time.Time
}

func newChannel(parent *socket, id string) *channel {
	c := new(channel)
	c.parent = parent
	c.id = id
	c.queue = make(chan *event, parent.chanBufferSize)
	c.errorCh = make(chan error)

	return c
}

func (c *channel) flushQueue() {
	close(c.queue)
	close(c.errorCh)
}

func (c *channel) handle(ev *event) error {
	if ev.Name == HeartbeatEvent {
		c.lastHeartbeat = time.Now()
		return nil
	}

	if ev.Name == WindowSize {
		// TODO: Respect congestion notification.
		return nil
	}

	c.queue <- ev
	return nil
}

// recvEvent pops first event from channel's queue or returns error if
// something bad happened on parent socket.
func (c *channel) recvEvent() (*event, error) {
	select {
	case ev := <-c.queue:
		return ev, nil
	case err := <-c.errorCh:
		return nil, err
	}
}

// sendEvent is convenience wrapper for "low-level" socket.writeEvent function.
func (c *channel) sendEvent(ev *event) error {
	return c.parent.writeEvent(ev, c.identity)
}