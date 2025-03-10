package netpoll

import (
	"context"
	"fmt"
	"sync/atomic"
)

type fifoConnection struct {
	reader *readFifo
	writer *writeFifo

	ctx                    context.Context //TODO: whether to delete this field?
	onFifoTransferCallback atomic.Value

	closeCallbacks atomic.Value // value is latest *fifoConnectionCallbackNode
}

type fifoConnectionCallbackNode struct {
	fn  FifoConnectionCloseCallback
	pre *fifoConnectionCallbackNode
}

var (
	_ FifoConnection = &fifoConnection{}
)

func (f *fifoConnection) Close() (err error) {
	f.closeCallback()

	if err = f.reader.Close(); err != nil {
		return err
	}
	if err = f.writer.Close(); err != nil {
		return err
	}
	return nil
}

func (f *fifoConnection) AddCloseCallback(callback FifoConnectionCloseCallback) error {
	if callback == nil {
		return nil
	}
	cb := &fifoConnectionCallbackNode{}
	cb.fn = callback
	if pre := f.closeCallbacks.Load(); pre != nil {
		cb.pre = pre.(*fifoConnectionCallbackNode)
	}
	f.closeCallbacks.Store(cb)
	return nil
}

func (f *fifoConnection) closeCallback() {
	latest := f.closeCallbacks.Load()
	if latest == nil {
		return
	}
	for callback := latest.(*fifoConnectionCallbackNode); callback != nil; callback = callback.pre {
		callback.fn(f)
	}
}

// ------------------------------------------ implement FifoConnection ------------------------------------------

func (f *fifoConnection) GetFDs() (readerFD int, writerFD int) {
	return f.reader.FD(), f.writer.FD()
}

func (f *fifoConnection) GetPaths() (readerPath string, writerPath string) {
	return f.reader.Path(), f.writer.Path()
}

func (f *fifoConnection) GetReader() FifoReader {
	return f.reader
}

func (f *fifoConnection) GetWriter() FifoWriter {
	return f.writer
}

func (f *fifoConnection) Read(b []byte) (n int, err error) {
	return f.reader.Read(b)
}

func (f *fifoConnection) Write(b []byte) (n int, err error) {
	return f.writer.Write(b)
}

func (f *fifoConnection) SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error {
	if onFifoTransfer != nil {
		f.onFifoTransferCallback.Store(onFifoTransfer)
	}
	return nil
}

// ------------------------------------------ private methods ------------------------------------------

func (f *fifoConnection) init(readerPath string, writerPath string, opts *options) (err error) {
	if opts.onFifoTransfer == nil {
		return fmt.Errorf("onFifoTransfer is nil, please set it")
	}

	f.reader = &readFifo{}
	f.writer = &writeFifo{}

	if err = f.reader.init(readerPath, opts); err != nil {
		return err
	}
	// switch onFifoRead to onFifoTransfer
	f.reader.SetOnFifoRead(func(ctx context.Context, fifo FifoReader) error {
		return opts.onFifoTransfer(ctx, f)
	})
	if err = f.writer.init(writerPath, opts); err != nil {
		return err
	}
	return nil
}
