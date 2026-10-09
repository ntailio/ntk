// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package produce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/windows"
)

const maxPipeMessage = 16 << 20

const pipeBuffer = 64 << 10

// pipeDrainWait is how long Source waits for more messages during shutdown
// before the queue counts as empty. Each message passes through a reader
// goroutine, so it allows more time than drainWait for a socket.
const pipeDrainWait = 100 * time.Millisecond

var errPipeMessageSize = errors.New("pipe message too large")

// PipeListener receives messages on a named pipe. Each client connects to its
// own pipe instance, so any number of them can send at once.
type PipeListener struct {
	Path string
	msgs chan pipeMessage
	done chan struct{}
	wg   sync.WaitGroup

	mu     sync.Mutex
	closed bool
	pipes  map[*pipeInstance]bool
}

type pipeMessage struct {
	data []byte
	err  error
}

// pipeInstance is one server end of the pipe. The kernel writes to ov and buf
// while an operation is pending, so they live here, on the heap.
type pipeInstance struct {
	h, ev windows.Handle
	ov    windows.Overlapped
	buf   []byte
}

// ListenPipe creates a message-mode named pipe at path: each write by a client
// is one message. It fails if another process already has the pipe.
func ListenPipe(path string) (*PipeListener, error) {
	l := &PipeListener{Path: path, msgs: make(chan pipeMessage), done: make(chan struct{}), pipes: map[*pipeInstance]bool{}}
	p, err := l.instance(true)
	switch {
	case errors.Is(err, windows.ERROR_ACCESS_DENIED), errors.Is(err, windows.ERROR_PIPE_BUSY):
		return nil, fmt.Errorf("another process is listening on %s", path)
	case err != nil:
		return nil, fmt.Errorf("creating %s: %w", path, err)
	}
	l.wg.Add(1)
	go l.accept(p)
	return l, nil
}

func (l *PipeListener) instance(first bool) (*pipeInstance, error) {
	name, err := windows.UTF16PtrFromString(l.Path)
	if err != nil {
		return nil, err
	}
	flags := uint32(windows.PIPE_ACCESS_INBOUND | windows.FILE_FLAG_OVERLAPPED)
	if first {
		flags |= windows.FILE_FLAG_FIRST_PIPE_INSTANCE
	}
	// Local clients only, like a Unix socket. The default security descriptor
	// lets only this user, administrators and SYSTEM write to the pipe.
	mode := uint32(windows.PIPE_TYPE_MESSAGE | windows.PIPE_READMODE_MESSAGE | windows.PIPE_WAIT | windows.PIPE_REJECT_REMOTE_CLIENTS)
	h, err := windows.CreateNamedPipe(name, flags, mode, windows.PIPE_UNLIMITED_INSTANCES, 0, pipeBuffer, 0, nil)
	if err != nil {
		return nil, err
	}
	ev, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, err
	}
	p := &pipeInstance{h: h, ev: ev, buf: make([]byte, pipeBuffer)}
	l.mu.Lock()
	l.pipes[p] = true
	l.mu.Unlock()
	return p, nil
}

func (l *PipeListener) release(p *pipeInstance) {
	l.mu.Lock()
	delete(l.pipes, p)
	l.mu.Unlock()
	_ = windows.CloseHandle(p.h)
	_ = windows.CloseHandle(p.ev)
}

// do starts an overlapped operation on p and waits for it to finish. Close
// cancels a running operation, and once closed nothing new starts.
func (l *PipeListener) do(p *pipeInstance, op func() error) (uint32, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, windows.ERROR_OPERATION_ABORTED
	}
	p.ov = windows.Overlapped{HEvent: p.ev}
	err := op()
	l.mu.Unlock()
	if err != nil && !errors.Is(err, windows.ERROR_IO_PENDING) && !errors.Is(err, windows.ERROR_MORE_DATA) {
		return 0, err
	}
	var n uint32
	err = windows.GetOverlappedResult(p.h, &p.ov, &n, true)
	return n, err
}

func (l *PipeListener) send(m pipeMessage) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	select {
	case l.msgs <- m:
		return true
	case <-l.done:
		return false
	}
}

func (l *PipeListener) accept(p *pipeInstance) {
	defer l.wg.Done()
	for {
		_, err := l.do(p, func() error { return windows.ConnectNamedPipe(p.h, &p.ov) })
		// ERROR_PIPE_CONNECTED and ERROR_NO_DATA: the client connected (and
		// perhaps already left) before ConnectNamedPipe. What it sent is still there.
		if err != nil && !errors.Is(err, windows.ERROR_PIPE_CONNECTED) && !errors.Is(err, windows.ERROR_NO_DATA) {
			l.release(p)
			l.send(pipeMessage{err: fmt.Errorf("waiting for a client on %s: %w", l.Path, err)})
			return
		}
		next, err := l.instance(false)
		l.wg.Add(1)
		go l.read(p)
		if err != nil {
			l.send(pipeMessage{err: fmt.Errorf("creating %s: %w", l.Path, err)})
			return
		}
		p = next
	}
}

func (l *PipeListener) read(p *pipeInstance) {
	defer l.wg.Done()
	defer l.release(p)
	for {
		msg := []byte{}
		for {
			var done uint32
			n, err := l.do(p, func() error { return windows.ReadFile(p.h, p.buf, &done, &p.ov) })
			msg = append(msg, p.buf[:n]...)
			if len(msg) > maxPipeMessage {
				l.send(pipeMessage{err: errPipeMessageSize})
				return
			}
			if errors.Is(err, windows.ERROR_MORE_DATA) {
				continue
			}
			if err != nil {
				// ERROR_BROKEN_PIPE: the client is gone and everything it sent was read.
				if !errors.Is(err, windows.ERROR_BROKEN_PIPE) && !errors.Is(err, windows.ERROR_PIPE_NOT_CONNECTED) &&
					!errors.Is(err, windows.ERROR_OPERATION_ABORTED) {
					l.send(pipeMessage{err: fmt.Errorf("reading %s: %w", l.Path, err)})
				}
				return
			}
			break
		}
		if !l.send(pipeMessage{data: msg}) {
			return
		}
	}
}

func (l *PipeListener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.done)
		for p := range l.pipes {
			_ = windows.CancelIoEx(p.h, nil)
		}
	}
	l.mu.Unlock()
	l.wg.Wait()
	return nil
}

// Source yields one record per pipe message, read like a datagram on a Unix
// socket. After ctx is canceled it hands over the messages already sent, then
// returns io.EOF. idle > 0 ends the input after that long without a message.
func (l *PipeListener) Source(meta bool, base kgo.Record, keep Keep, idle time.Duration) Source {
	n := 0
	return func(ctx context.Context) (*kgo.Record, error) {
		var timeout <-chan time.Time
		if idle > 0 {
			t := time.NewTimer(idle)
			defer t.Stop()
			timeout = t.C
		}
		var m pipeMessage
		select {
		case m = <-l.msgs:
		case <-timeout:
			return nil, io.EOF
		case <-ctx.Done():
			select {
			case m = <-l.msgs:
			case <-time.After(pipeDrainWait):
				return nil, io.EOF
			}
		}
		n++
		switch {
		case errors.Is(m.err, errPipeMessageSize):
			return nil, fmt.Errorf("message %d is larger than %d bytes", n, maxPipeMessage)
		case m.err != nil:
			return nil, m.err
		}
		rec, err := decode(m.data, meta, base, keep)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", n, err)
		}
		return rec, nil
	}
}
