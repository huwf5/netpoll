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
	// user is responsible for removing the fifo file
}

type ReadableFifo interface {
	FD() int
	Path() string

	Read(b []byte) (n int, err error)
	Reader() Reader
	SetReadTimeout(timeout time.Duration) error
}

type WriteableFifo interface {
	FD() int
	Path() string

	Write(b []byte) (n int, err error)
	Writer() Writer
	SetWriteTimeout(timeout time.Duration) error
}

type FifoReader interface {
	BaseFifo
	ReadableFifo

	SetOnFifoRead(onFifoRead OnFifoRead) error
}

type FifoWriter interface {
	BaseFifo
	WriteableFifo
}

type FifoConnection interface {
	GetFDs() (readerFD int, writerFD int)
	GetPaths() (readerPath string, writerPath string)

	// BaseFifo
	Close() error
	AddCloseCallback(callback FifoConnectionCloseCallback) error

	GetReader() FifoReader
	GetWriter() FifoWriter

	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)

	SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error
}
