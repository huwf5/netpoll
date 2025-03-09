package netpoll

import (
	"context"
	"os"
	"syscall"
	"time"
)

type writeFifo struct {
	baseFifo

	writeTimeout time.Duration
	writeTimer   *time.Timer
	writeTrigger chan error

	outputBuffer  *LinkBuffer // write buffer
	outputBarrier *barrier
}

var (
	_ BaseFifo  = &writeFifo{}
	_ Writer    = &writeFifo{}
	_ WriteFifo = &writeFifo{}
)

// ------------------------------------------ implement BaseFifo ------------------------------------------

func (f *writeFifo) Close() error {
	return f.onClose()
}

func (f *writeFifo) Detach() error {
	f.baseFifo.Detach()
	return f.onClose()
}

// ------------------------------------------ implement zero-copy writer ------------------------------------------

// Malloc implements Fifo.
func (f *writeFifo) Malloc(n int) (buf []byte, err error) {
	return f.outputBuffer.Malloc(n)
}

// MallocLen implements Fifo.
func (f *writeFifo) MallocLen() (length int) {
	return f.outputBuffer.MallocLen()
}

// Flush implements Fifo.
func (f *writeFifo) Flush() (err error) {
	if !f.lock(fifoFlushing) {
		return Exception(ErrConcurrentAccess, "when flush")
	}
	defer f.unlock(fifoFlushing)

	f.outputBuffer.Flush()
	return f.flush()
}

// MallocAck implements Fifo.
func (f *writeFifo) MallocAck(n int) (err error) {
	return f.outputBuffer.MallocAck(n)
}

// Append implements Fifo.
func (f *writeFifo) Append(w Writer) (err error) {
	return f.outputBuffer.Append(w)
}

// WriteString implements Fifo.
func (f *writeFifo) WriteString(s string) (n int, err error) {
	return f.outputBuffer.WriteString(s)
}

// WriteBinary implements Fifo.
func (f *writeFifo) WriteBinary(b []byte) (n int, err error) {
	return f.outputBuffer.WriteBinary(b)
}

// WriteDirect implements Fifo.
func (f *writeFifo) WriteDirect(p []byte, remainCap int) (err error) {
	return f.outputBuffer.WriteDirect(p, remainCap)
}

// WriteByte implements Fifo.
func (f *writeFifo) WriteByte(b byte) (err error) {
	return f.outputBuffer.WriteByte(b)
}

// ------------------------------------------ implement FifoWriter ------------------------------------------
// FD() and Path() are inherited from BaseFifo

func (f *writeFifo) Write(b []byte) (n int, err error) {
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

func (f *writeFifo) Writer() Writer {
	return f
}

func (f *writeFifo) SetWriteTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		f.writeTimeout = timeout
	}
	return nil
}

// ------------------------------------------ private methods ------------------------------------------

func (f *writeFifo) init(path string, opts *options) error {
	f.path = path

	// create fifo file
	if err := syscall.Mkfifo(f.path, 0666); err != nil && !os.IsExist(err) {
		return err
	}
	fd, err := syscall.Open(f.path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}

	f.fd = fd
	f.outputBuffer = NewLinkBuffer()
	f.outputBarrier = barrierPool.Get().(*barrier)
	f.writeTrigger = make(chan error, 1)

	f.initOperator()
	f.initFinalizer()

	// set non-blocking mode
	syscall.SetNonblock(f.fd, true)

	return f.onPrepare(opts)

}

func (f *writeFifo) initOperator() {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = f.fd

	op.OnRead, op.OnWrite, op.OnHup = nil, nil, f.onHup
	op.Inputs, op.InputAck = nil, nil
	op.Outputs, op.OutputAck = f.outputs, f.outputAck
	f.operator = op
}

func (f *writeFifo) initFinalizer() {
	f.AddCloseCallback(func(fifo BaseFifo) error {
		// wait for flushing finished
		f.stop(fifoFlushing)
		f.operator.Free()

		// close fd
		if !f.detaching && f.fd > 2 {
			err := syscall.Close(f.fd)
			if err != nil {
				logger.Printf("NETPOLL: FIFO close fd failed: %v", err)
			}
		}

		// TODO: decide whether to remove the fifo file
		// if f.path != "" {
		// 	if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
		// 		logger.Printf("NETPOLL: FIFO remove file failed: %v", err)
		// 	}
		// }
		f.closeBuffer()
		return nil
	})
}

// triggerWrite means the write operation is done.
func (f *writeFifo) triggerWrite(err error) {
	select {
	case f.writeTrigger <- err:
	default:
	}
}

// flush writes the data to the FIFO file descriptor.
// it will try to directly write the data first, if failed, it will register the writable event.
func (f *writeFifo) flush() error {
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

func (f *writeFifo) waitFlush() (err error) {
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

// ---------------------------------------- event handling ----------------------------------------

func (f *writeFifo) onPrepare(opts *options) error {
	if opts != nil {
		f.SetWriteTimeout(opts.writeTimeout)
	}
	if f.ctx == nil {
		f.ctx = context.Background()
	}
	// lazy register writable event until flush
	return nil
}

func (f *writeFifo) closeCallback(needLock, needDetach bool) (err error) {
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
