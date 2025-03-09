package netpoll

import "sync"

type fifoManager struct {
	opts  *options
	fifos sync.Map // key=fd, value=fifo
}

// ------------------------------------------ FIFO ------------------------------------------

func (m *fifoManager) AttachReadFifo(path string) error {
	readFifo := new(readFifo)
	if err := readFifo.init(path, m.opts); err != nil {
		return err
	}

	readFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.fifos.Delete(path)
		return nil
	})
	m.fifos.Store(path, readFifo)

	readFifo.onProcess()
	return nil
}

func (m *fifoManager) GenerateWriteFifo(path string) (WriteFifo, error) {
	writeFifo := new(writeFifo)
	if err := writeFifo.init(path, m.opts); err != nil {
		return nil, err
	}

	writeFifo.AddCloseCallback(func(fifo BaseFifo) error {
		m.fifos.Delete(path)
		return nil
	})
	m.fifos.Store(path, writeFifo)

	return writeFifo, nil
}

func (m *fifoManager) AttachFifoConnection(readerPath string, writerPath string) error {
	fifoConnection := new(fifoConnection)
	if err := fifoConnection.init(readerPath, writerPath, m.opts); err != nil {
		return err
	}

	fifoConnection.AddCloseCallback(func(fifo FifoConnection) error {
		m.fifos.Delete(fifoConnection.reader.FD())
		m.fifos.Delete(fifoConnection.writer.FD())
		return nil
	})
	m.fifos.Store(fifoConnection.reader.FD(), fifoConnection)
	m.fifos.Store(fifoConnection.writer.FD(), fifoConnection)

	return nil
}
