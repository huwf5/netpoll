package netpoll

import (
	"context"
	"sync/atomic"
)

type FifoEvent struct {
	ctx             context.Context
	onDataCallback  atomic.Value
	onCloseCallback atomic.Value
	onErrorCallback atomic.Value

	closeCallback atomic.Value // value is latest *fifoCallbackNode
}

type fifoCallbackNode struct {
	fn  FifoCloseCallback
	pre *fifoCallbackNode
}

func (f *Fifo) SetOnData(onData OnData) error {
	if onData != nil {
		f.onDataCallback.Store(onData)
	}
	return nil
}

func (f *Fifo) SetOnClose(onClose OnClose) error {
	if onClose != nil {
		f.onCloseCallback.Store(onClose)
	}
	return nil
}

func (f *Fifo) SetOnError(onError OnError) error {
	if onError != nil {
		f.onErrorCallback.Store(onError)
	}
	return nil
}

func (f *Fifo) AddCloseCallback(callback FifoCloseCallback) error {
	if callback == nil {
		return nil
	}
	cb := &fifoCallbackNode{}
	cb.fn = callback
	if pre := f.closeCallback.Load(); pre != nil {
		cb.pre = pre.(*fifoCallbackNode)
	}
	f.closeCallback.Store(cb)
	return nil
}

func (f *Fifo) onPrepare(opts *options) (err error) {

	if opts != nil {
		f.SetOnData(opts.onData)
		f.SetOnClose(opts.onClose)
		f.SetOnError(opts.onError)
	}

	if f.ctx == nil {
		f.ctx = context.Background()
	}
	switch f.mode {
	case FifoModeRead:
		return f.register(PollReadable)
	case FifoModeWrite:
		return f.register(PollWritable)
	}
	return nil
}

func (f *Fifo) onProcess() {
	// TODO: implement onProcess
	onData, _ := f.onDataCallback.Load().(OnData)
	if onData == nil {
		return
	}
}
