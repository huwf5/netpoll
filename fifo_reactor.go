package netpoll

import (
	"sync/atomic"

	"github.com/cloudwego/netpoll/internal/runner"
)

// this file implements the event handling of readFifo and writeFifo

// ------------------------------------------ readFifo's event handling ------------------------------------------

// onHup means close by poller
func (f *readFifo) onHup(p Poll) error {
	if !f.closeBy(poller) {
		return nil
	}

	f.triggerRead(Exception(ErrEOF, "peer close"))

	OnFifoRead := f.onFifoReadCallback.Load()
	// when user not set onFifoRead, it should be closed by user
	// otherwise, it should be closed actively
	needCloseByUser := OnFifoRead == nil
	if !needCloseByUser {
		// already PollDetach when call OnHup
		f.closeCallback(true, false)
	}

	return nil
}

// onClose means close by user
func (f *readFifo) onClose() error {
	if f.closeBy(user) {
		f.triggerRead(Exception(ErrConnClosed, "self close"))
		f.closeCallback(true, true)
		return nil
	}

	// closed by poller
	// still need to change closing status to `user` since OnProcess should not be processed again
	f.force(fifoClosing, user)

	return f.closeCallback(true, false)
}

func (f *readFifo) inputs(vs [][]byte) (rs [][]byte) {
	vs[0] = f.inputBuffer.book(f.bookSize, f.maxSize)
	return vs[:1]
}

func (f *readFifo) inputAck(n int) (err error) {
	if n <= 0 {
		f.inputBuffer.bookAck(0)
		return nil
	}

	// Auto size bookSize.
	if n == f.bookSize && f.bookSize < mallocMax {
		f.bookSize <<= 1
	}

	length, _ := f.inputBuffer.bookAck(n)
	if f.maxSize < length {
		f.maxSize = length
	}
	if f.maxSize > mallocMax {
		f.maxSize = mallocMax
	}

	needTrigger := true
	if length == n { // first start
		processed := f.onProcess()
		needTrigger = !processed
	}

	if needTrigger && length >= int(atomic.LoadInt64(&f.waitReadSize)) {
		f.triggerRead(nil)
	}
	return nil
}

func (f *readFifo) closeBuffer() {
	OnFifoRead, _ := f.onFifoReadCallback.Load().(OnFifoRead)
	if f.inputBuffer.Len() == 0 || OnFifoRead != nil {
		f.inputBuffer.Close()
	}
}

func (f *readFifo) onProcess() (processed bool) {
	if !f.lock(fifoProcessing) {
		return false
	}

	task := func() {
		panicked := true
		defer func() {
			if !panicked {
				return
			}
			f.unlock(fifoProcessing)
			f.Close()
		}()
	START:
		// support close callback for user use
		onFifoRead, _ := f.onFifoReadCallback.Load().(OnFifoRead)
		if onFifoRead != nil && f.Reader().Len() > 0 {
			_ = onFifoRead(f.ctx, f)
		}

		var closedBy who
		for {
			closedBy = f.status(fifoClosing)
			if closedBy == user || onFifoRead == nil || f.Reader().Len() == 0 {
				break
			}
			_ = onFifoRead(f.ctx, f)
		}
		if closedBy != none {
			needDetach := closedBy == user
			f.closeCallback(false, needDetach)
			panicked = false
			return
		}
		f.unlock(fifoProcessing)

		if f.status(fifoClosing) != 0 && f.lock(fifoProcessing) {
			f.closeCallback(false, false)
			panicked = false
			return
		}

		if onFifoRead != nil && f.Reader().Len() > 0 && f.lock(fifoProcessing) {
			goto START
		}
		panicked = false
	}
	runner.RunTask(f.ctx, task)
	return true
}

// ------------------------------------------ end of readFifo's event handling ------------------------------------------

// ------------------------------------------ writeFifo's event handling ------------------------------------------

// onHup means close by poller
func (f *writeFifo) onHup(p Poll) error {
	if !f.closeBy(poller) {
		return nil
	}

	f.triggerWrite(Exception(ErrEOF, "peer close"))

	f.closeCallback(true, false)

	return nil
}

// onClose means close by user
func (f *writeFifo) onClose() error {
	if f.closeBy(user) {
		f.triggerWrite(Exception(ErrConnClosed, "self close"))
		f.closeCallback(true, true)
		return nil
	}

	// closed by poller
	// still need to change closing status to `user` since OnProcess should not be processed again
	f.force(fifoClosing, user)

	return f.closeCallback(true, false)
}

func (f *writeFifo) outputs(vs [][]byte) (rs [][]byte, supportZeroCopy bool) {
	if f.outputBuffer.IsEmpty() {
		// means no data to write
		f.detachWrite()
		return rs, false
	}
	rs = f.outputBuffer.GetBytes(vs)
	return rs, false
}

// outputAck implements FDOperator.OutputAck.
func (f *writeFifo) outputAck(n int) (err error) {
	if n > 0 {
		f.outputBuffer.Skip(n)
		f.outputBuffer.Release()
	}

	if f.outputBuffer.IsEmpty() {
		// means no data to write
		f.detachWrite()
	}
	return nil
}

func (f *writeFifo) closeBuffer() {
	// writeFifo is used as part of a FifoConnection,
	// so it should be actively closed
	f.outputBuffer.Close()
	barrierPool.Put(f.outputBarrier)
}

// detachWrite removes the write event from the poller and triggers the write operation.
func (f *writeFifo) detachWrite() {
	f.operator.Control(PollDetach)
	f.triggerWrite(nil)
}

// ------------------------------------------ end of writeFifo's event handling ------------------------------------------
