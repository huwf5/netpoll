package netpoll

import (
	"time"
)

// FifoCloseCallback is used by writeFifo and readFifo
type FifoCloseCallback func(fifo BaseFifo) error

// FifoConnectionCloseCallback is used by fifoConnection
type FifoConnectionCloseCallback func(fifo FifoConnection) error

type BaseFifo interface {
	Close() error
	AddCloseCallback(callback FifoCloseCallback) error // fifoEvent implements this method
}

type FifoReader interface {
	FD() int
	Path() string

	Read(b []byte) (n int, err error)
	Reader() Reader
	SetReadTimeout(timeout time.Duration) error
}

type FifoWriter interface {
	FD() int
	Path() string

	Write(b []byte) (n int, err error)
	Writer() Writer
	SetWriteTimeout(timeout time.Duration) error
}

type ReadFifo interface {
	BaseFifo
	FifoReader

	SetOnFifoRead(onFifoRead OnFifoRead) error
}

type WriteFifo interface {
	BaseFifo
	FifoWriter
}

type FifoConnection interface {
	FD() (readerFD int, writerFD int)
	Path() (readerPath string, writerPath string)

	// BaseFifo
	Close() error
	AddCloseCallback(callback FifoConnectionCloseCallback) error

	Reader() ReadFifo
	Writer() WriteFifo

	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)

	SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error
}
