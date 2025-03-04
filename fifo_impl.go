package netpoll

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

type FifoMode int

const (
	FifoModeRead  FifoMode = 0
	FifoModeWrite FifoMode = 1
)

// FIFO is used to communicate between processes.
type fifo struct {
	// onEvent
	fifoEvent
	fifoLocker

	// FIFO specific attributes
	fd   int      // file descriptor
	path string   // FIFO file path
	mode FifoMode // open mode (read/write)

	operator     *FDOperator
	readTimeout  time.Duration
	readTimer    *time.Timer
	readTrigger  chan error
	waitReadSize int64

	writeTimeout time.Duration
	writeTimer   *time.Timer
	writeTrigger chan error

	// buffer management
	inputBuffer   *LinkBuffer // read buffer
	outputBuffer  *LinkBuffer // write buffer
	outputBarrier *barrier

	maxSize  int // The maximum size of data between two Release().
	bookSize int // The size of data that can be read at once.

	// state control
	detaching bool // atomic operation flag, indicating whether the pipe is detaching
}

var (
	_ Fifo   = &fifo{}
	_ Reader = &fifo{}
	_ Writer = &fifo{}
)

// ------------------------------------------ implement Fifo ------------------------------------------

// Read implements Fifo.
func (f *fifo) Read(b []byte) (n int, err error) {
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

// Write implements Fifo.
func (f *fifo) Write(b []byte) (n int, err error) {
	if !f.lock(fifoFlushing) {
		return 0, Exception(ErrConcurrentAccess, "when write")
	}
	defer f.unlock(fifoFlushing)

	dst, _ := f.outputBuffer.Malloc(len(b))
	n = copy(dst, b)
	f.outputBuffer.Flush()
	err = f.flush()
	return n, err
}

// Close implements Fifo.
func (f *fifo) Close() error {
	return f.onClose()
}

// Detach detaches the fifo from the poller but doesn't close it.
func (f *fifo) Detach() error {
	f.detaching = true
	return f.onClose()
}

// Reader implements Fifo.
func (f *fifo) Reader() Reader {
	return f
}

// Writer implements Fifo.
func (f *fifo) Writer() Writer {
	return f
}

// SetReadTimeout implements Fifo.
func (f *fifo) SetReadTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		f.readTimeout = timeout
	}
	return nil
}

// SetWriteTimeout implements Fifo.
func (f *fifo) SetWriteTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		f.writeTimeout = timeout
	}
	return nil
}

// ------------------------------------------ implement zero-copy reader ------------------------------------------

// Next implements Fifo.
func (f *fifo) Next(n int) (p []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return p, err
	}
	return f.inputBuffer.Next(n)
}

// Peek implements Fifo.
func (f *fifo) Peek(n int) (buf []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return buf, err
	}
	return f.inputBuffer.Peek(n)
}

// Skip implements Fifo.
func (f *fifo) Skip(n int) (err error) {
	if err = f.waitRead(n); err != nil {
		return err
	}
	return f.inputBuffer.Skip(n)
}

// Release implements Fifo.
func (f *fifo) Release() (err error) {
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
func (f *fifo) Slice(n int) (r Reader, err error) {
	if err = f.waitRead(n); err != nil {
		return nil, err
	}
	return f.inputBuffer.Slice(n)
}

// Len implements Fifo.
func (f *fifo) Len() (length int) {
	return f.inputBuffer.Len()
}

// Until implements Fifo.
func (f *fifo) Until(delim byte) (line []byte, err error) {
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
func (f *fifo) ReadString(n int) (s string, err error) {
	if err = f.waitRead(n); err != nil {
		return s, err
	}
	return f.inputBuffer.ReadString(n)
}

// ReadBinary implements Fifo.
func (f *fifo) ReadBinary(n int) (p []byte, err error) {
	if err = f.waitRead(n); err != nil {
		return p, err
	}
	return f.inputBuffer.ReadBinary(n)
}

// ReadByte implements Fifo.
func (f *fifo) ReadByte() (b byte, err error) {
	if err = f.waitRead(1); err != nil {
		return 0, err
	}
	return f.inputBuffer.ReadByte()
}

// ------------------------------------------ implement zero-copy writer ------------------------------------------

// Malloc implements Fifo.
func (f *fifo) Malloc(n int) (buf []byte, err error) {
	return f.outputBuffer.Malloc(n)
}

// MallocLen implements Fifo.
func (f *fifo) MallocLen() (length int) {
	return f.outputBuffer.MallocLen()
}

// Flush implements Fifo.
func (f *fifo) Flush() (err error) {
	if !f.lock(fifoFlushing) {
		return Exception(ErrConcurrentAccess, "when flush")
	}
	defer f.unlock(fifoFlushing)

	f.outputBuffer.Flush()
	return f.flush()
}

// MallocAck implements Fifo.
func (f *fifo) MallocAck(n int) (err error) {
	return f.outputBuffer.MallocAck(n)
}

// Append implements Fifo.
func (f *fifo) Append(w Writer) (err error) {
	return f.outputBuffer.Append(w)
}

// WriteString implements Fifo.
func (f *fifo) WriteString(s string) (n int, err error) {
	return f.outputBuffer.WriteString(s)
}

// WriteBinary implements Fifo.
func (f *fifo) WriteBinary(b []byte) (n int, err error) {
	return f.outputBuffer.WriteBinary(b)
}

// WriteDirect implements Fifo.
func (f *fifo) WriteDirect(p []byte, remainCap int) (err error) {
	return f.outputBuffer.WriteDirect(p, remainCap)
}

// WriteByte implements Fifo.
func (f *fifo) WriteByte(b byte) (err error) {
	return f.outputBuffer.WriteByte(b)
}

// ------------------------------------------ private ------------------------------------------

// init initialize fifo
func (f *fifo) init(path string, mode FifoMode, opts *options) error {
	f.path = path
	f.mode = mode

	// create fifo file
	if err := syscall.Mkfifo(f.path, 0666); err != nil && !os.IsExist(err) {
		return err
	}

	switch f.mode {
	case FifoModeRead:
		if err := f.initForRead(); err != nil {
			return err
		}
	case FifoModeWrite:
		if err := f.initForWrite(); err != nil {
			return err
		}
	}
	f.initOperator()
	f.initFinalizer()

	// set non-blocking mode
	syscall.SetNonblock(f.fd, true)

	return f.onPrepare(opts)
}

func (f *fifo) initForRead() error {
	fd, err := syscall.Open(f.path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	f.fd = fd
	f.inputBuffer = NewLinkBuffer(defaultLinkBufferSize)
	f.readTrigger = make(chan error, 1)
	f.bookSize, f.maxSize = defaultLinkBufferSize, defaultLinkBufferSize
	return nil
}

func (f *fifo) initForWrite() error {
	fd, err := syscall.Open(f.path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	f.fd = fd
	f.outputBuffer = NewLinkBuffer()
	f.outputBarrier = barrierPool.Get().(*barrier)
	f.writeTrigger = make(chan error, 1)
	return nil
}

// initOperator initialize FD operator
func (f *fifo) initOperator() {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = f.fd
	op.OnRead, op.OnWrite, op.OnHup = nil, nil, f.onHup
	switch f.mode {
	case FifoModeRead:
		op.Inputs, op.InputAck = f.inputs, f.inputAck
		op.Outputs, op.OutputAck = nil, nil
	case FifoModeWrite:
		op.Inputs, op.InputAck = nil, nil
		op.Outputs, op.OutputAck = f.outputs, f.outputAck
	}
	f.operator = op
}

func (f *fifo) initFinalizer() {
	f.AddCloseCallback(func(fifo Fifo) error {
		f.stop(fifoFlushing)
		f.operator.Free()

		// close fd
		if !f.detaching && f.fd > 2 {
			err := syscall.Close(f.fd)
			if err != nil {
				logger.Printf("NETPOLL: FIFO close fd failed: %v", err)
			}
		}
		// clear fifo file
		if f.path != "" {
			if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
				logger.Printf("NETPOLL: FIFO remove file failed: %v", err)
			}
		}
		f.closeBuffer()
		return nil
	})
}

func (f *fifo) FD() int {
	return f.fd
}

func (f *fifo) triggerRead(err error) {
	select {
	case f.readTrigger <- err:
	default:
	}
}

// triggerWrite means the write operation is done.
func (f *fifo) triggerWrite(err error) {
	select {
	case f.writeTrigger <- err:
	default:
	}
}

func (f *fifo) waitRead(n int) error {
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

func (f *fifo) waitReadWithTimeout(n int) (err error) {
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

func (f *fifo) flush() error {
	if f.outputBuffer.IsEmpty() {
		return nil
	}

	bs := f.outputBuffer.GetBytes(f.outputBarrier.bs)
	n, err := writev(f.fd, bs, f.outputBarrier.ivs)
	if err != nil && err != syscall.EAGAIN {
		return Exception(err, "when flush")
	}
	if n > 0 {
		err = f.outputBuffer.Skip(n)
		f.outputBuffer.Release()
		if err != nil {
			return Exception(err, "when flush")
		}
	}
	// return if write all buffer.
	if f.outputBuffer.IsEmpty() {
		return nil
	}
	err = f.operator.Control(PollWritable)
	if err != nil {
		return Exception(err, "when flush")
	}

	return f.waitFlush()
}

func (f *fifo) waitFlush() (err error) {
	if f.writeTimeout == 0 {
		return <-f.writeTrigger
	}

	// set write timeout
	if f.writeTimer == nil {
		f.writeTimer = time.NewTimer(f.writeTimeout)
	} else {
		f.writeTimer.Reset(f.writeTimeout)
	}

	select {
	case err = <-f.writeTrigger:
		if !f.writeTimer.Stop() {
			<-f.writeTimer.C
		}
		return err
	case <-f.writeTimer.C:
		select {
		case err = <-f.writeTrigger:
			return err
		default:
		}
		// if timeout, remove write event from poller
		// we cannot flush it again, since we don't if the poller is still process outputBuffer
		f.operator.Control(PollDetach)
		return Exception(ErrWriteTimeout, "wait flush")
	}
}
