package netpoll

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
)

type fifoConnection struct {
	reader *readFifo
	writer *writeFifo

	onFifoTransferCallback atomic.Value

	closeCallbacks atomic.Value // value is latest *fifoConnectionCallbackNode

	opts        *options
	writerReady atomic.Bool
	writerPath  string
	writerMutex sync.Mutex
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

	var readerErr, writerErr error

	if f.reader != nil {
		readerErr = f.reader.Close()
	}

	if f.writer != nil {
		writerErr = f.writer.Close()
	}

	if readerErr != nil {
		return readerErr
	}

	return writerErr
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
	if f.reader == nil && f.writer == nil {
		return -1, -1
	}
	if f.reader == nil {
		return -1, f.writer.FD()
	}
	if f.writer == nil {
		return f.reader.FD(), -1
	}
	return f.reader.FD(), f.writer.FD()
}

func (f *fifoConnection) GetPaths() (readerPath string, writerPath string) {
	// avoid nil pointer dereference for using writerPath instead of f.writer.Path()
	return f.reader.Path(), f.writerPath
}

func (f *fifoConnection) GetReader() FifoReader {
	return f.reader
}

func (f *fifoConnection) GetWriter() FifoWriter {
	return f.writer
}

func (f *fifoConnection) Read(b []byte) (n int, err error) {
	if f.reader == nil {
		return 0, fmt.Errorf("reader is not initialized")
	}
	return f.reader.Read(b)
}

func (f *fifoConnection) IsWriterReady() bool {
	return f.writer != nil && f.writerReady.Load()
}

// TryInitWriter tries to initialize the writer fifo if it is not ready
// it will return the error directly if it fails
func (f *fifoConnection) TryInitWriter() error {
	// try to init writer fifo
	f.writerMutex.Lock()
	defer f.writerMutex.Unlock()

	if f.IsWriterReady() {
		return nil
	}

	if err := f.writer.init(f.writerPath, f.opts); err != nil {
		return err
	}

	// init success, set writerReady to true
	f.writerReady.Store(true)
	return nil
}

func (f *fifoConnection) Write(b []byte) (n int, err error) {
	// try to init writer fifo if not ready
	if !f.IsWriterReady() {
		if err := f.TryInitWriter(); err != nil {
			return 0, err
		}
	}

	return f.writer.Write(b)
}

func (f *fifoConnection) SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error {
	if onFifoTransfer != nil {
		f.onFifoTransferCallback.Store(onFifoTransfer)
	}
	return nil
}

// ------------------------------------------ private methods ------------------------------------------

// init initializes the fifo connection
// if the writer is not ready due to ENXIO, it will ignore the error
// it pass the responsibility to the caller to make sure the writer fifo is ready before using it
func (f *fifoConnection) init(readerPath string, writerPath string, opts *options) (err error) {
	if opts == nil || opts.onFifoTransfer == nil {
		return fmt.Errorf("onFifoTransfer is nil, please set it first")
	}

	f.writerPath = writerPath
	f.opts = opts
	f.writerReady.Store(false)

	f.reader = &readFifo{}
	if err = f.reader.init(readerPath, opts); err != nil {
		return err
	}
	// switch onFifoRead to onFifoTransfer
	f.reader.SetOnFifoRead(func(ctx context.Context, fifo FifoReader) error {
		return opts.onFifoTransfer(ctx, f)
	})

	// try to init writer fifo
	f.writer = &writeFifo{}
	err = f.writer.init(writerPath, opts)
	if err == nil {
		// first init success, set writerReady to true
		f.writerReady.Store(true)
		return nil
	}

	if !errors.Is(err, syscall.ENXIO) {
		return err
	}
	// ENXIO means the writer fifo is not ready,
	// we pass the responsibility to the caller to make sure the writer fifo is ready before using it
	return nil
}
