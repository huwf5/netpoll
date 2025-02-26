package netpoll

import (
	"os"
	"syscall"
	"time"
)

// FIFO is used to communicate between processes.
type Fifo struct {
	onEvent
	locker
	// TODO: add lock support

	// FIFO specific attributes
	fd   int    // file descriptor
	path string // FIFO file path
	mode int    // open mode (read/write)

	operator *FDOperator

	// buffer management
	inputBuffer  *LinkBuffer // read buffer
	outputBuffer *LinkBuffer // write buffer

	// timeout control
	readTimeout  time.Duration
	writeTimeout time.Duration

	// state control
	closed int32 // atomic operation flag, indicating whether the pipe is closed
}

// NewFifo creates a new fifo instance
func NewFifo(path string, mode int) (*Fifo, error) {
	fifo := &Fifo{
		path: path,
		mode: mode,
	}
	return fifo, fifo.init()
}

// init initialize fifo
func (f *Fifo) init() error {
	// create fifo file
	if err := syscall.Mkfifo(f.path, 0666); err != nil && !os.IsExist(err) {
		return err
	}

	// open fifo
	fd, err := syscall.Open(f.path, f.mode|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	f.fd = fd

	// set non-blocking mode
	if err := syscall.SetNonblock(fd, true); err != nil {
		return err
	}

	// initialize buffers
	f.inputBuffer = NewLinkBuffer(defaultLinkBufferSize)
	f.outputBuffer = NewLinkBuffer()

	// initialize event handler
	return f.initOperator()
}

// initOperator initialize FD operator
func (f *Fifo) initOperator() error {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = f.fd
	// TODO: implement read/write/hup function
	// op.OnRead, op.OnWrite, op.OnHup = nil, nil, fifo.OnHup
	// op.Inputs, op.InputAck = fifo.Inputs, fifo.InputAck
	// op.Outputs, op.OutputAck = fifo.Outputs, fifo.OutputAck
	f.operator = op

	// register to poll based on mode
	event := PollReadable
	if f.mode&syscall.O_WRONLY != 0 {
		event = PollWritable
	}
	return f.register(event)
}

func (f *Fifo) register(event PollEvent) (err error) {
	err = f.operator.Control(event)
	if err != nil {
		logger.Printf("NETPOLL: connection register failed: %v", err)
		return Exception(ErrConnClosed, err.Error())
	}
	return nil
}

// ------------------------------------------ implement FDOperator ------------------------------------------

// TODO: implement
func (f *Fifo) OnHup(p Poll) error {
	return nil
}

func (f *Fifo) Inputs(vs [][]byte) (rs [][]byte) {
	return nil
}

func (f *Fifo) InputAck(n int) (err error) {
	return nil
}

func (f *Fifo) Outputs(vs [][]byte) (rs [][]byte, supportZeroCopy bool) {
	return nil, false
}

func (f *Fifo) OutputAck(n int) (err error) {
	return nil
}
