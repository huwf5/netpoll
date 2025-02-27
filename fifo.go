package netpoll

import (
	"os"
	"syscall"
)

type FifoMode int

const (
	FifoModeRead  FifoMode = 0
	FifoModeWrite FifoMode = 1
)

type FifoCloseCallback func(fifo Fifo) error

// FIFO is used to communicate between processes.
type Fifo struct {
	// onEvent
	FifoEvent
	locker
	// TODO: add lock support

	// FIFO specific attributes
	fd   int      // file descriptor
	path string   // FIFO file path
	mode FifoMode // open mode (read/write)

	operator *FDOperator

	// buffer management
	inputBuffer   *LinkBuffer // read buffer
	outputBuffer  *LinkBuffer // write buffer
	outputBarrier *barrier

	// state control
	closed int32 // atomic operation flag, indicating whether the pipe is closed
}

var (
	_ Reader = &Fifo{}
	_ Writer = &Fifo{}
)

// init initialize fifo
func (f *Fifo) init(path string, mode FifoMode, opts *options) error {
	f.path = path
	f.mode = mode

	// create fifo file
	if err := syscall.Mkfifo(f.path, 0666); err != nil && !os.IsExist(err) {
		return err
	}

	switch f.mode {
	case FifoModeRead:
		if err := f.initForRead(); err != nil {
			return err
		}
	case FifoModeWrite:
		if err := f.initForWrite(); err != nil {
			return err
		}
	}
	f.initOperator()
	f.initFinalizer()

	// set non-blocking mode
	syscall.SetNonblock(f.fd, true)

	return f.register(PollReadable)
}

func (f *Fifo) initForRead() error {
	fd, err := syscall.Open(f.path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	f.fd = fd
	f.inputBuffer = NewLinkBuffer(defaultLinkBufferSize)
	return nil
}

func (f *Fifo) initForWrite() error {
	fd, err := syscall.Open(f.path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	f.fd = fd
	f.outputBuffer = NewLinkBuffer()
	f.outputBarrier = barrierPool.Get().(*barrier)
	return nil
}

// initOperator initialize FD operator
func (f *Fifo) initOperator() {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = f.fd
	op.OnRead, op.OnWrite, op.OnHup = nil, nil, f.OnHup
	switch f.mode {
	case FifoModeRead:
		op.Inputs, op.InputAck = f.Inputs, f.InputAck
		op.Outputs, op.OutputAck = nil, nil
	case FifoModeWrite:
		op.Inputs, op.InputAck = nil, nil
		op.Outputs, op.OutputAck = f.Outputs, f.OutputAck
	}
	f.operator = op
}

func (f *Fifo) initFinalizer() {
	f.AddCloseCallback(func(fifo Fifo) error {
		// TODO: when finish
		f.operator.Free()
		return nil
	})
}

func (f *Fifo) register(event PollEvent) (err error) {
	err = f.operator.Control(event)
	if err != nil {
		logger.Printf("NETPOLL: connection register failed: %v", err)
		return Exception(ErrConnClosed, err.Error())
	}
	return nil
}

func (f *Fifo) FD() int {
	return f.fd
}

// ------------------------------------------ implement zero-copy reader ------------------------------------------

func (f *Fifo) Next(n int) (p []byte, err error) {

}

// ------------------------------------------ private ------------------------------------------
