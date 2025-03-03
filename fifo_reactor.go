package netpoll

import "sync/atomic"

// ------------------------------------------ implement FDOperator ------------------------------------------

// onHup means close by poller.
func (f *fifo) onHup(p Poll) error {
	if !f.closeBy(poller) {
		return nil
	}

	switch f.mode {
	case FifoModeRead:
		f.triggerRead(Exception(ErrEOF, "peer close"))
	case FifoModeWrite:
		f.triggerWrite(Exception(ErrConnClosed, "peer close"))
	}

	onData := f.onDataCallback.Load()
	needCloseByUser := onData == nil
	if !needCloseByUser {
		// already PollDetach when call OnHup
		f.closeCallback(true, false)
	}

	return nil
}

// onClose means close by user.
func (f *fifo) onClose() error {
	if f.closeBy(user) {
		switch f.mode {
		case FifoModeRead:
			f.triggerRead(Exception(ErrConnClosed, "self close"))
		case FifoModeWrite:
			f.triggerWrite(Exception(ErrConnClosed, "self close"))
		}
		f.closeCallback(true, true)
		return nil
	}

	// closed by poller
	// still need to change closing status to `user` since OnProcess should not be processed again
	f.force(fifoClosing, user)

	return f.closeCallback(true, false)
}

// closeBuffer recycle input & output LinkBuffer.
func (f *fifo) closeBuffer() {
	onData, _ := f.onDataCallback.Load().(OnData)

	switch f.mode {
	case FifoModeRead:
		if f.inputBuffer.Len() == 0 || onData != nil {
			f.inputBuffer.Close()
		}
	case FifoModeWrite:
		if f.outputBuffer.Len() == 0 || onData != nil {
			f.outputBuffer.Close()
			barrierPool.Put(f.outputBarrier)
		}
	}
}

// inputs implements FDOperator.Inputs.
func (f *fifo) inputs(vs [][]byte) (rs [][]byte) {
	vs[0] = f.inputBuffer.book(f.bookSize, f.maxSize)
	return vs[:1]
}

// inputAck implements FDOperator.InputAck.
func (f *fifo) inputAck(n int) (err error) {
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
	if length == n { // first start onRequest
		processed := f.onProcess()
		needTrigger = !processed
	}

	if needTrigger && length >= int(atomic.LoadInt64(&f.waitReadSize)) {
		f.triggerRead(nil)
	}
	return nil
}

// outputs implements FDOperator.Outputs.
func (f *fifo) outputs(vs [][]byte) (rs [][]byte, supportZeroCopy bool) {
	if f.outputBuffer.IsEmpty() {
		f.detachWrite()
		return rs, false
	}
	rs = f.outputBuffer.GetBytes(vs)
	return rs, false
}

// outputAck implements FDOperator.OutputAck.
func (f *fifo) outputAck(n int) (err error) {
	if n > 0 {
		f.outputBuffer.Skip(n)
		f.outputBuffer.Release()
	}

	if f.outputBuffer.IsEmpty() {
		f.detachWrite()
	}
	return nil
}

// detachWrite removes the write event from the poller and triggers the write operation.
func (f *fifo) detachWrite() {
	f.operator.Control(PollDetach)
	f.triggerWrite(nil)
}
