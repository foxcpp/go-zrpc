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
	"runtime/debug"
	"testing"
	"time"
)

func MustCompleteIn(t *testing.T, timeout time.Duration, f func()) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		t.Helper()
		f()
		done <- struct{}{}
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		if t.Failed() {
			return true
		}

		if !testing.Verbose() {
			t.Fatal("test timeout")
			return false
		}

		// This is how we print stacktrace of all goroutines to help debugging.
		debug.SetTraceback("all")
		panic("test timeout")
	}
}

func RunWithTimeout(tParent *testing.T, name string, timeout time.Duration, f func(t *testing.T)) {
	tParent.Helper()
	tParent.Run(name, func(t *testing.T) {
		t.Helper()
		MustCompleteIn(t, timeout, func() {
			t.Helper()
			f(t)
		})
	})
}
