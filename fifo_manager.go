package netpoll

import (
	"errors"
	"sync"
)

func newFifoManager(opts *options, onQuit func(err error)) *fifoManager {
	return &fifoManager{
		opts:   opts,
		onQuit: onQuit,
	}
}

type fifoManager struct {
	sync.RWMutex
	opts   *options
	onQuit func(err error)

	readFifos       sync.Map // key=fd, value=readFifo
	writeFifos      sync.Map // key=fd, value=writeFifo
	fifoConnections sync.Map // key=readerPath, value=fifoConnection

}

var _ FifoManager = &fifoManager{}

// ------------------------------------------ FIFO ------------------------------------------

func (m *fifoManager) AttachReadFifo(path string) error {

	readFifo := new(readFifo)
	if err := readFifo.init(path, m.opts); err != nil {
		return err
	}

	fd := readFifo.FD()

	m.Lock()
	readFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.readFifos.Delete(fd)
		return nil
	})
	m.readFifos.Store(fd, readFifo)
	m.Unlock()

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

	m.Lock()
	writeFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.writeFifos.Delete(fd)
		return nil
	})
	m.writeFifos.Store(fd, writeFifo)
	m.Unlock()

	return writeFifo, nil
}

func (m *fifoManager) AttachFifoConnection(readerPath string, writerPath string) error {
	fifoConnection := new(fifoConnection)
	if err := fifoConnection.init(readerPath, writerPath, m.opts); err != nil {
		return err
	}

	m.Lock()
	fifoConnection.AddCloseCallback(func(fifo FifoConnection) error {
		m.fifoConnections.Delete(readerPath)
		return nil
	})
	m.fifoConnections.Store(readerPath, fifoConnection)
	m.Unlock()

	return nil
}

func (m *fifoManager) Close() error {
	var err error

	m.Lock()
	defer m.Unlock()

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

func (m *fifoManager) GetFifoConnections() ([]FifoConnection, error) {
	m.RLock()
	defer m.RUnlock()

	connections := make([]FifoConnection, 0)
	m.fifoConnections.Range(func(key, value interface{}) bool {
		if fifo, ok := value.(FifoConnection); ok {
			connections = append(connections, fifo)
		}
		return true
	})
	return connections, nil
}

func (m *fifoManager) GetFifoConnectionByReaderPath(readerPath string) (FifoConnection, error) {
	m.RLock()
	defer m.RUnlock()

	value, ok := m.fifoConnections.Load(readerPath)
	if !ok {
		return nil, errors.New("fifo connection not found")
	}
	return value.(FifoConnection), nil
}
