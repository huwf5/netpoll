package netpoll

import (
	"context"
	"sync/atomic"
)

type fifoEvent struct {
	ctx context.Context

	onFifoReadCallback     atomic.Value
	onFifoTransferCallback atomic.Value

	closeCallbacks atomic.Value // value is latest *fifoCallbackNode
}

func (f *fifoEvent) SetOnFifoRead(onFifoRead OnFifoRead) error {
	if onFifoRead != nil {
		f.onFifoReadCallback.Store(onFifoRead)
	}
	return nil
}

func (f *fifoEvent) SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error {
	if onFifoTransfer != nil {
		f.onFifoTransferCallback.Store(onFifoTransfer)
	}
	return nil
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
