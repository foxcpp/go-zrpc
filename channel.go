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
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/pkg/errors"
)

const (
	HeartbeatEvent = "_zpc_hb"
	WindowSize     = "_zpc_more"
)

// ErrIncompatibleVersion is returned when version number in received message
// doesn't matches protocol version implemented by library.
type ErrIncompatibleVersion struct {
	remoteVer int
}

func (ive ErrIncompatibleVersion) Error() string {
	return fmt.Sprintf("zrpc: incompatible version: %d (theirs) != %d (ours)", ive.remoteVer, ProtocolVersion)
}

var ErrRemoteLost = errors.New("zrpc: remote stopped sending heartbeats")

type socket struct {
	ioLck     sync.Mutex
	chansLock sync.RWMutex
	sock      *zmq4.Socket
	chans     map[string]*channel

	// Written to by sendEventFromPipe, read by writeEvent.
	pipeErrs chan error

	// Where all errors occurred during polling go, nil by default.
	readErrs chan<- error

	pipeIn, pipeOut *zmq4.Socket

	dispatchReactor *zmq4.Reactor
	stopChan        chan bool
	stopping        uint32

	// Configuration
	noVersionCheck           bool
	chanBufferSize           uint64
	defaultHeartbeatInterval time.Duration
}

func newSocket(t zmq4.Type) (s *socket, err error) {
	s = &socket{
		chans:    map[string]*channel{},
		pipeErrs: make(chan error),
		readErrs: nil,
		stopChan: make(chan bool),
	}
	s.sock, err = zmq4.NewSocket(t)
	if err != nil {
		return nil, errors.Wrap(err, "new sock")
	}

	s.pipeIn, s.pipeOut, err = socketPair()
	if err != nil {
		return nil, errors.Wrap(err, "new sock")
	}

	s.dispatchReactor = zmq4.NewReactor()
	s.dispatchReactor.AddSocket(s.pipeOut, zmq4.POLLIN, func(state zmq4.State) error {
		if atomic.LoadUint32(&s.stopping) == 1 {
			return errors.New("")
		}

		s.sendEventFromPipe()
		return nil
	})
	s.dispatchReactor.AddSocket(s.sock, zmq4.POLLIN, func(state zmq4.State) error {
		if atomic.LoadUint32(&s.stopping) == 1 {
			return errors.New("")
		}

		s.dispatchEvent()
		return nil
	})

	s.chanBufferSize = 10
	s.defaultHeartbeatInterval = 5 * time.Second

	go func() {
		if err := s.dispatchReactor.Run(-1); err != nil {
			if err.Error() == "" {
				s.stopChan <- true
				return
			}
			// TODO: Can we have any kind of error handling here?
		}
		s.stopChan <- true
	}()

	return s, nil
}

func (s *socket) connect(endpoint string) error {
	return s.sock.Connect(endpoint)
}

func (s *socket) bind(endpoint string) error {
	return s.sock.Bind(endpoint)
}

func (s *socket) Close() error {
	s.chansLock.Lock()
	for id := range s.chans {
		s.closeChannelNoLock(id)
	}
	s.chansLock.Unlock()

	atomic.StoreUint32(&s.stopping, 1)
	if _, err := s.pipeIn.SendMessage("WAKE_UP"); err != nil {
		return err
	}
	<-s.stopChan

	if err := s.pipeIn.Close(); err != nil {
		return err
	}
	if err := s.pipeOut.Close(); err != nil {
		return err
	}

	return s.sock.Close()
}

func (s *socket) sendEventFromPipe() {
	msgParts, err := s.pipeOut.RecvMessageBytes(0)
	if err != nil {
		s.pipeErrs <- err
	}

	// Weird, but we can't pass [][]byte to SendMessage.
	iarr := make([]interface{}, len(msgParts))
	for i, part := range msgParts {
		iarr[i] = part
	}

	if _, err := s.sock.SendMessage(iarr...); err != nil {
		s.pipeErrs <- err
	}
	s.pipeErrs <- nil
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
		return errors.Wrap(err, "event write")
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
	if id == "" {
		id = randomString(32)
	}

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

func (s *socket) closeChannelNoLock(id string) {
	ch, prs := s.chans[id]
	if !prs {
		return
	}
	delete(s.chans, id)

	ch.close()
}

// closeChannel frees all resources associated with certain channel.
func (s *socket) closeChannel(id string) {
	s.chansLock.Lock()
	defer s.chansLock.Unlock()

	s.closeChannelNoLock(id)
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
			ch = s.openChannel(ev.Hdr.ResponseTo, identity)
		}
	} else {
		ch = s.openChannel(ev.Hdr.MsgID, identity)
	}

	if atomic.LoadUint32(&s.stopping) == 1 {
		return
	}

	if err := ch.handle(ev); err != nil {
		ch.reportError(errors.Wrap(err, "dispatch event"))
	}
}

// reportError makes all currently running channel.recvEvent() calls
// return a passed error.
func (s *socket) reportError(err error) {
	s.readErrs <- err

	s.chansLock.RLock()
	defer s.chansLock.RUnlock()

	for _, ch := range s.chans {
		ch.reportError(err)
	}
}

type channel struct {
	parent       *socket
	id, identity string

	stateLck sync.Mutex
	stopping bool
	stopCh   chan struct{}

	queue chan *event

	errorCh chan error

	heartbeatStop     chan struct{}
	heartbeatTicker   *time.Ticker
	lastHeartbeat     time.Time
	heartbeatInterval time.Duration
}

func newChannel(parent *socket, id string) *channel {
	c := new(channel)
	c.parent = parent
	c.id = id
	c.queue = make(chan *event, parent.chanBufferSize)
	c.errorCh = make(chan error)

	if parent.defaultHeartbeatInterval != 0 {
		// We assume that last heartbeat was during channel initialization
		// so we will have point of reference during first check.
		c.lastHeartbeat = time.Now()

		c.heartbeatStop = make(chan struct{})
		c.heartbeatInterval = parent.defaultHeartbeatInterval
		c.heartbeatTicker = time.NewTicker(c.heartbeatInterval)
		go c.heartbeat()
	}

	return c
}

func (c *channel) close() {
	c.stateLck.Lock()
	defer c.stateLck.Unlock()

	c.stopping = true

	// nil heartbeatStop indicates that heartbeat is disabled
	// due to lost remote and channel is basically in
	// "dangling" state.
	if c.heartbeatInterval != 0 && c.heartbeatStop != nil {
		c.heartbeatStop <- struct{}{}
		c.heartbeatTicker.Stop()
	}

	close(c.queue)
	c.errorCh = nil
}

// reportError makes running channel.recvEvent(), if any, return an error.
func (c *channel) reportError(err error) {
	c.stateLck.Lock()
	defer c.stateLck.Unlock()

	if c.stopping {
		return
	}

	select {
	case c.errorCh <- err: // looks like this still blocks if channel is nil, further investigation needed
	default:
		// no one is listening, skip
	}
}

func (c *channel) heartbeat() {
	for {
		select {
		case t := <-c.heartbeatTicker.C:
			// We haven't seen heartbeat for two intervals now...
			c.stateLck.Lock()
			lastHeartbeat := c.lastHeartbeat
			c.stateLck.Unlock()
			if lastHeartbeat.Add(2 * c.heartbeatInterval).Before(t) {
				c.reportError(ErrRemoteLost)

				// Stop sending heartbeats since it makes no sense now (and channel is useless anyway).
				c.stateLck.Lock()
				c.heartbeatStop = nil
				c.heartbeatTicker.Stop()
				c.stateLck.Unlock()
				return
			}

			if err := c.sendEvent(&event{Name: HeartbeatEvent}); err != nil {
				c.reportError(errors.Wrap(err, "heartbeat"))
			}
		case <-c.heartbeatStop:
			return
		}
	}
}

// handle puts event into channel's queue or handles it specially if it
// is event layer's notification (like congestion control or heartbeat).
func (c *channel) handle(ev *event) error {
	if ev.Name == HeartbeatEvent {
		c.stateLck.Lock()
		c.lastHeartbeat = time.Now()
		c.stateLck.Unlock()
		return nil
	}

	if ev.Name == WindowSize {
		// TODO: Respect congestion notification.
		return nil
	}

	select {
	case c.queue <- ev:
	case <-c.stopCh:
		return nil
	}
	return nil
}

// recvEvent pops first event from channel's queue or returns error if
// something bad happened on parent socket.
//
// Running recvEvent concurrently from multiple goroutines will
// report error to only one of them, so special care should be taken
// in this case.
func (c *channel) recvEvent() (*event, error) {
	select {
	case ev := <-c.queue:
		return ev, nil
	case err := <-c.errorCh:
		return nil, err
	case <-c.stopCh:
		return nil, errors.New("recvEvent: cancelled")
	}
}

// sendEvent is convenience wrapper for "low-level" socket.writeEvent function.
// It should be always used when possible, because it accounts for window size reported
// by congestion control and automatically creates valid header.
func (c *channel) sendEvent(ev *event) error {
	ev.Hdr.Version = ProtocolVersion
	if ev.Hdr.MsgID == "" {
		ev.Hdr.MsgID = randomString(32)
	}
	ev.Hdr.ResponseTo = c.id

	return c.parent.writeEvent(ev, c.identity)
}

func randomString(length uint) string {
	randBytes := make([]byte, length/2)
	_, err := rand.Read(randBytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(randBytes)
}
