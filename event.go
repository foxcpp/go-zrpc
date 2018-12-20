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
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// eventHdr is a header part of event structure.
type eventHdr struct {
	Version int    `msgpack:"v",json:"v"`
	MsgID   string `msgpack:"message_id",json:"message_id"`
	ResponseTo string `msgpack:"response_to,omitempty",json:"response_to,omitempty"`
}

// The event structure represents base unit of communication defined
// at "event layer" of ZeroRPC protocol.
type event struct {
	Hdr  eventHdr
	Name string
	Args interface{}
}

// UnmarshalBinary decodes event from "on-wire" binary representation.
func (e *event) UnmarshalBinary(src io.Reader) error {
	dec := msgpack.NewDecoder(src)
	dec.UseDecodeInterfaceLoose(true)
	length, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if length != 3 {
		return errors.Errorf("event read: got %d fields, wanted 3", length)
	}

	if err := dec.Decode(&e.Hdr); err != nil {
		return errors.Wrap(err, "event read (header)")
	}
	e.Name, err = dec.DecodeString()
	if err != nil {
		return errors.Wrap(err, "event read (name)")
	}
	e.Args, err = dec.DecodeInterfaceLoose()
	if err != nil {
		return errors.Wrap(err, "event read (args)")
	}
	return nil
}

// MarshalBinary encodes event into "on-wire" binary representation.
func (e *event) MarshalBinary() ([]byte, error) {
	return msgpack.Marshal([3]interface{}{e.Hdr, e.Name, e.Args})
}

// String returns human-friendly representation of event object. Useful for debugging.
func (e *event) String() string {
	return fmt.Sprintf("Event(Hdr: (v=%d, id=%s, respTo=%s), Name: %s, Args: %v)",
		e.Hdr.Version, e.Hdr.MsgID, e.Hdr.ResponseTo, e.Name, e.Args)
}

