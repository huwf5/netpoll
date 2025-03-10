package netpoll

import (
	"context"
	"sync/atomic"
)

type fifoEvent struct {
	ctx context.Context

	closeCallbacks atomic.Value // value is latest *fifoCallbackNode

	// onFifoReadCallback is located in readFifo
}

type fifoCallbackNode struct {
	fn  FifoCloseCallback
	pre *fifoCallbackNode
}

func (f *fifoEvent) AddCloseCallback(callback FifoCloseCallback) error {
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
