package netpoll

import "time"

type FifoCloseCallback func(fifo Fifo) error

type Fifo interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error

	Reader() Reader
	Writer() Writer

	SetOnData(onData OnData) error

	SetReadTimeout(timeout time.Duration) error

	SetWriteTimeout(timeout time.Duration) error

	AddCloseCallback(callback FifoCloseCallback) error
}
