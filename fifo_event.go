package netpoll

import (
	"context"
	"sync/atomic"

	"github.com/cloudwego/netpoll/internal/runner"
)

type fifoEvent struct {
	ctx            context.Context
	onDataCallback atomic.Value
	closeCallbacks atomic.Value // value is latest *fifoCallbackNode
}

type fifoCallbackNode struct {
	fn  FifoCloseCallback
	pre *fifoCallbackNode
}

func (f *fifo) SetOnData(onData OnData) error {
	if onData != nil {
		f.onDataCallback.Store(onData)
	}
	return nil
}

func (f *fifo) AddCloseCallback(callback FifoCloseCallback) error {
	if callback == nil {
		return nil
	}
	cb := &fifoCallbackNode{}
	cb.fn = callback
	if pre := f.closeCallbacks.Load(); pre != nil {
		cb.pre = pre.(*fifoCallbackNode)
	}
	f.closeCallbacks.Store(cb)
	return nil
}

func (f *fifo) onPrepare(opts *options) error {

	if opts != nil {
		f.SetOnData(opts.onData)
		f.SetReadTimeout(opts.readTimeout)
		f.SetWriteTimeout(opts.writeTimeout)
	}

	if f.ctx == nil {
		f.ctx = context.Background()
	}
	switch f.mode {
	case FifoModeRead:
		return f.register(PollReadable)
	case FifoModeWrite:
		// lazy register writable event
		return nil
	}
	return nil
}

func (f *fifo) onProcess() (processed bool) {
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
		onData, _ := f.onDataCallback.Load().(OnData)
		if onData != nil && f.Reader().Len() > 0 {
			_ = onData(f.ctx, f)
		}

		var closedBy who
		for {
			closedBy = f.status(fifoClosing)
			if closedBy == user || onData == nil || f.Reader().Len() == 0 {
				break
			}
			_ = onData(f.ctx, f)
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

		if onData != nil && f.Reader().Len() > 0 && f.lock(fifoProcessing) {
			goto START
		}
		panicked = false
	}
	runner.RunTask(f.ctx, task)
	return true
}

func (f *fifo) closeCallback(needLock, needDetach bool) (err error) {
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

func (f *fifo) register(event PollEvent) (err error) {
	switch f.mode {
	case FifoModeRead:
		if event != PollReadable && event != PollDetach {
			return Exception(ErrUnsupported, "write event on read-only FIFO")
		}
	case FifoModeWrite:
		if event != PollWritable && event != PollDetach {
			return Exception(ErrUnsupported, "read event on write-only FIFO")
		}
	}

	err = f.operator.Control(event)
	if err != nil {
		logger.Printf("NETPOLL: FIFO register failed: %v", err)
		return Exception(ErrConnClosed, err.Error())
	}
	return nil
}
