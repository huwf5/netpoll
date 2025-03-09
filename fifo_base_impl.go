package netpoll

type FifoMode int

const (
	FifoModeRead  FifoMode = 0
	FifoModeWrite FifoMode = 1
)

type baseFifo struct {
	fifoEvent
	fifoLocker
	fd   int
	path string

	operator *FDOperator

	// state control
	detaching bool // atomic operation flag, indicating whether the pipe is detaching
}

var (
	_ BaseFifo = &baseFifo{}
)

// ------------------------------------------ private methods ------------------------------------------

// FD() and Path() implements FifoReader/FifoWriter
func (f *baseFifo) FD() int {
	return f.fd
}

func (f *baseFifo) Path() string {
	return f.path
}

// ------------------------------------------ implement BaseFifo ------------------------------------------

// this method should be overridden by the specific fifo implementation
func (f *baseFifo) Close() error {
	panic("not implemented, need to override")
}

func (f *baseFifo) Detach() {
	f.detaching = true
}
