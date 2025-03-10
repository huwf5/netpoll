package netpoll

import (
	"sync"
)

func newFifoManager(opts *options, onQuit func(err error)) *fifoManager {
	return &fifoManager{
		opts:   opts,
		onQuit: onQuit,
	}
}

// TODO: decide whether to use sync.Map or not
type fifoManager struct {
	opts   *options
	onQuit func(err error)

	readFifos       sync.Map // key=fd, value=readFifo
	writeFifos      sync.Map // key=fd, value=writeFifo
	fifoConnections sync.Map // key=readerFD, value=fifoConnection

}

// ------------------------------------------ FIFO ------------------------------------------

func (m *fifoManager) AttachReadFifo(path string) error {

	readFifo := new(readFifo)
	if err := readFifo.init(path, m.opts); err != nil {
		return err
	}

	fd := readFifo.FD()

	readFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.readFifos.Delete(fd)
		return nil
	})
	m.readFifos.Store(fd, readFifo)

	// start the handler
	readFifo.onProcess()
	return nil
}

func (m *fifoManager) GenerateWriteFifo(path string) (FifoWriter, error) {
	writeFifo := new(writeFifo)
	if err := writeFifo.init(path, m.opts); err != nil {
		return nil, err
	}

	fd := writeFifo.FD()

	writeFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.writeFifos.Delete(fd)
		return nil
	})
	m.writeFifos.Store(fd, writeFifo)

	return writeFifo, nil
}

func (m *fifoManager) AttachFifoConnection(readerPath string, writerPath string) error {
	fifoConnection := new(fifoConnection)
	if err := fifoConnection.init(readerPath, writerPath, m.opts); err != nil {
		return err
	}

	readerFd := fifoConnection.reader.FD()

	fifoConnection.AddCloseCallback(func(fifo FifoConnection) error {
		m.fifoConnections.Delete(readerFd)
		return nil
	})
	m.fifoConnections.Store(readerFd, fifoConnection)

	return nil
}

func (m *fifoManager) Close() error {
	var err error
	// close all read fifos
	m.readFifos.Range(func(key, value interface{}) bool {
		if fifo, ok := value.(FifoReader); ok {
			if err = fifo.Close(); err != nil {
				logger.Printf("Netpoll: failed to close read fifo: %v", err)
			}
		}
		return true
	})

	// close all write fifos
	m.writeFifos.Range(func(key, value interface{}) bool {
		if fifo, ok := value.(FifoWriter); ok {
			if err = fifo.Close(); err != nil {
				logger.Printf("Netpoll: failed to close write fifo: %v", err)
			}
		}
		return true
	})

	m.fifoConnections.Range(func(key, value interface{}) bool {
		if fifo, ok := value.(FifoConnection); ok {
			if err = fifo.Close(); err != nil {
				logger.Printf("Netpoll: failed to close fifo connection: %v", err)
			}
		}
		return true
	})

	if m.onQuit != nil {
		m.onQuit(err)
	}
	return err
}
