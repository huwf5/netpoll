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

	// GetWriter returns the writer of the FIFO connection.
	// Note: Before using the writer, check if it's ready.
	//
	// for example:
	//  1. First check if the writer is ready
	//     if !fifoConnection.IsWriterReady() {
	//         // Try to initialize the writer
	//         if err := fifoConnection.TryInitWriter(); err != nil {
	//             // Handle initialization error
	//             return err
	//         }
	//     }
	//
	//  2. Get and use the writer
	//     writer := fifoConnection.GetWriter()
	//     // Use writer methods...
	//
	// Alternatively, use the Write method which automatically tries to initialize the writer:
	//     n, err := fifoConnection.Write(data)
	GetWriter() FifoWriter

	Read(b []byte) (n int, err error)

	// Write writes data to the FIFO connection.
	// If the writer is not ready, this method will automatically try to initialize it.
	// If initialization fails, the error will be returned directly.
	//
	// Returns:
	//   - n: number of bytes successfully written
	//   - err: error if any, including writer initialization failures
	Write(b []byte) (n int, err error)

	SetOnFifoTransfer(onFifoTransfer OnFifoTransfer) error

	// IsWriterReady checks if the writer is initialized and ready to use.
	// This should be called before directly using the writer obtained from GetWriter().
	IsWriterReady() bool

	// TryInitWriter attempts to initialize the writer.
	// Call this method when IsWriterReady() returns false.
	//
	// Possible errors:
	//   - System errors encountered during initialization
	//   - ENXIO error (indicating the other end is not open yet), which may require retrying later
	//
	// Note: This method is non-blocking. If initialization fails, users need to implement their own retry logic.
	TryInitWriter() error
}
