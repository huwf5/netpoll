package netpoll

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

type readFifo struct {
	baseFifo

	onFifoReadCallback atomic.Value

	readTimeout  time.Duration
	readTimer    *time.Timer
	readTrigger  chan error
	waitReadSize int64

	inputBuffer *LinkBuffer // read buffer

	maxSize  int // The maximum size of data between two Release().
	bookSize int // The size of data that can be read at once.
}

var (
	_ BaseFifo   = &readFifo{}
	_ Reader     = &readFifo{}
	_ FifoReader = &readFifo{}
)

// ------------------------------------------ implement BaseFifo ------------------------------------------
func (f *readFifo) Close() error {
	return f.onClose()
}

func (f *readFifo) Detach() error {
	f.baseFifo.Detach()
	return f.onClose()
}

// ------------------------------------------ implement zero-copy reader ------------------------------------------

// Next implements Fifo.
func (f *readFifo) Next(n int) (p []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return p, err
	}
	return f.inputBuffer.Next(n)
}

// Peek implements Fifo.
func (f *readFifo) Peek(n int) (buf []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return buf, err
	}
	return f.inputBuffer.Peek(n)
}

// Skip implements Fifo.
func (f *readFifo) Skip(n int) (err error) {
	if err = f.waitRead(n); err != nil {
		return err
	}
	return f.inputBuffer.Skip(n)
}

// Release implements Fifo.
func (f *readFifo) Release() (err error) {
	if f.inputBuffer.Len() == 0 && f.operator.do() {
		maxSize := f.inputBuffer.calcMaxSize()
		if maxSize > mallocMax {
			maxSize = mallocMax
		}

		if maxSize > f.maxSize {
			f.maxSize = maxSize
		}
		if f.inputBuffer.Len() == 0 {
			f.inputBuffer.resetTail(f.maxSize)
		}
		f.operator.done()
	}
	return f.inputBuffer.Release()
}

// Slice implements Fifo.
func (f *readFifo) Slice(n int) (r Reader, err error) {
	if err = f.waitRead(n); err != nil {
		return nil, err
	}
	return f.inputBuffer.Slice(n)
}

// Len implements Fifo.
func (f *readFifo) Len() (length int) {
	return f.inputBuffer.Len()
}

// Until implements Fifo.
func (f *readFifo) Until(delim byte) (line []byte, err error) {
	var n, l int
	for {
		if err = f.waitRead(n + 1); err != nil {
			line, _ = f.inputBuffer.Next(f.inputBuffer.Len())
			return
		}

		l = f.inputBuffer.Len()
		i := f.inputBuffer.indexByte(delim, n)
		if i < 0 {
			n = l // skip all exists bytes
			continue
		}
		return f.Next(i + 1)
	}
}

// ReadString implements Fifo.
func (f *readFifo) ReadString(n int) (s string, err error) {
	if err = f.waitRead(n); err != nil {
		return s, err
	}
	return f.inputBuffer.ReadString(n)
}

// ReadBinary implements Fifo.
func (f *readFifo) ReadBinary(n int) (p []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return p, err
	}
	return f.inputBuffer.ReadBinary(n)
}

// ReadByte implements Fifo.
func (f *readFifo) ReadByte() (b byte, err error) {
	if err = f.waitRead(1); err != nil {
		return 0, err
	}
	return f.inputBuffer.ReadByte()
}

// ------------------------------------------ implement FifoReader ------------------------------------------
func (f *readFifo) Read(b []byte) (n int, err error) {
	l := len(b)
	if l == 0 {
		return 0, nil
	}
	if err = f.waitRead(1); err != nil {
		return 0, err
	}
	if has := f.inputBuffer.Len(); has < l {
		l = has
	}
	src, err := f.inputBuffer.Next(l)
	n = copy(b, src)
	if err == nil {
		err = f.inputBuffer.Release()
	}
	return n, err
}

func (f *readFifo) Reader() Reader {
	return f
}

func (f *readFifo) SetReadTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		f.readTimeout = timeout
	}
	return nil
}

func (f *readFifo) SetOnFifoRead(onFifoRead OnFifoRead) error {
	if onFifoRead != nil {
		f.onFifoReadCallback.Store(onFifoRead)
	}
	return nil
}

// ------------------------------------------ private methods ------------------------------------------

// init initializes the read FIFO
// Note: When readFifo is used as part of a FifoConnection,
// a closure adapter should be used to convert the onFifoTransfer callback to an onFifoRead callback.
// This way, when readFifo triggers onFifoRead, it will actually call the FifoConnection's onFifoTransfer.
// Example:
//
//	reader.SetOnFifoRead(func(ctx context.Context, fifo ReadFifo) error {
//	    // Get and call the FifoConnection's callback
//	    onTransfer := conn.onFifoTransferCallback.Load()
//	    if onTransfer != nil {
//	        return onTransfer.(OnFifoTransfer)(ctx, conn)
//	    }
//	    return nil
//	})
func (f *readFifo) init(path string, opts *options) error {
	f.path = path

	// create fifo file
	if err := syscall.Mkfifo(f.path, 0666); err != nil && !os.IsExist(err) {
		return err
	}
	fd, err := syscall.Open(f.path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}

	f.fd = fd
	f.inputBuffer = NewLinkBuffer(defaultLinkBufferSize)
	f.readTrigger = make(chan error, 1)
	f.bookSize, f.maxSize = defaultLinkBufferSize, defaultLinkBufferSize

	f.initOperator()
	f.initFinalizer()

	// set non-blocking mode
	syscall.SetNonblock(f.fd, true)

	return f.onPrepare(opts)
}

func (f *readFifo) initOperator() {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = f.fd

	op.OnRead, op.OnWrite, op.OnHup = nil, nil, f.onHup
	op.Inputs, op.InputAck = f.inputs, f.inputAck
	op.Outputs, op.OutputAck = nil, nil
	f.operator = op
}

func (f *readFifo) initFinalizer() {
	f.AddCloseCallback(func(fifo BaseFifo) error {
		f.operator.Free()

		// close fd
		if !f.detaching && f.fd > 2 {
			err := syscall.Close(f.fd)
			if err != nil {
				logger.Printf("NETPOLL: FIFO close fd failed: %v", err)
			}
		}

		f.closeBuffer()
		return nil
	})
}

func (f *readFifo) triggerRead(err error) {
	select {
	case f.readTrigger <- err:
	default:
	}
}

func (f *readFifo) waitRead(n int) error {
	if n <= f.inputBuffer.Len() {
		return nil
	}
	atomic.StoreInt64(&f.waitReadSize, int64(n))
	defer atomic.StoreInt64(&f.waitReadSize, 0)
	if f.readTimeout > 0 {
		return f.waitReadWithTimeout(n)
	}
	// wait full n
	for f.inputBuffer.Len() < n {
		switch f.status(fifoClosing) {
		case poller:
			return Exception(ErrEOF, "wait read")
		case user:
			return Exception(ErrConnClosed, "wait read")
		default:
			err := <-f.readTrigger
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *readFifo) waitReadWithTimeout(n int) (err error) {
	// set read timeout
	if f.readTimer == nil {
		f.readTimer = time.NewTimer(f.readTimeout)
	} else {
		f.readTimer.Reset(f.readTimeout)
	}

	for f.inputBuffer.Len() < n {
		switch f.status(fifoClosing) {
		case poller:
			err = Exception(ErrEOF, "wait read")
			goto RET
		case user:
			err = Exception(ErrConnClosed, "wait read")
			goto RET
		default:
			select {
			case <-f.readTimer.C:
				if f.inputBuffer.Len() >= n {
					return nil
				}
				return Exception(ErrReadTimeout, "wait read")
			case err = <-f.readTrigger:
				if err != nil {
					goto RET
				}
				continue
			}
		}
	}
RET:
	if !f.readTimer.Stop() {
		<-f.readTimer.C
	}
	return err
}

// ---------------------------------------- event handling ----------------------------------------
func (f *readFifo) onPrepare(opts *options) error {

	if opts != nil {
		f.SetOnFifoRead(opts.onFifoRead)
		f.SetReadTimeout(opts.readTimeout)
	}
	if f.ctx == nil {
		f.ctx = context.Background()
	}
	return f.register(PollReadable)
}

func (f *readFifo) register(event PollEvent) (err error) {
	if event != PollReadable && event != PollDetach {
		return Exception(ErrUnsupported, "write event on read-only FIFO")
	}
	err = f.operator.Control(event)
	if err != nil {
		logger.Printf("NETPOLL: FIFO register failed: %v", err)
		f.Close()
		return Exception(ErrConnClosed, err.Error())
	}
	return nil
}

func (f *readFifo) closeCallback(needLock, needDetach bool) (err error) {
	if needLock && !f.lock(fifoProcessing) {
		return nil
	}
	if needDetach && f.operator.poll != nil {
		if err := f.operator.Control(PollDetach); err != nil {
			logger.Printf("NETPOLL: closeCallback[%v,%v] detach operator failed: %v", needLock, needDetach, err)
		}
	}
	latest := f.closeCallbacks.Load()
	if latest == nil {
		return nil
	}
	for callback := latest.(*fifoCallbackNode); callback != nil; callback = callback.pre {
		callback.fn(f)
	}
	return nil
}

// ---------------------------------------- end of event handling ----------------------------------------
