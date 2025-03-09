package netpoll

// import (
// 	"bytes"
// 	"context"
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"sync"
// 	"sync/atomic"
// 	"syscall"
// 	"testing"
// 	"time"
// )

// // TODO: Benchmark the performance of fifo
// func BenchmarkFifoReadWrite(b *testing.B) {

// }

// func beforeTest() (dir string) {
// 	// create tmp fifo file
// 	dir, err := os.MkdirTemp("", "fifo_test")
// 	if err != nil {
// 		panic(err)
// 	}
// 	return dir
// }

// func afterTest(dir string) {
// 	os.RemoveAll(dir)
// }

// func TestFifoBasicReadWrite(t *testing.T) {
// 	dir := beforeTest()
// 	defer afterTest(dir)

// 	fifoPath := filepath.Join(dir, "fifo")

// 	// test data
// 	testData := []byte("hello, world")

// 	var wg sync.WaitGroup
// 	wg.Add(2)

// 	// read task
// 	go func() {
// 		defer wg.Done()

// 		// create fifo
// 		readFifo := new(fifo)
// 		err := readFifo.init(fifoPath, FifoModeRead, &options{})
// 		if err != nil {
// 			t.Errorf("failed to init fifo: %v", err)
// 			return
// 		}
// 		defer readFifo.Close()

// 		// wait for write task to be ready since it's non-blocking
// 		time.Sleep(200 * time.Millisecond)

// 		// read data
// 		buf := make([]byte, len(testData))
// 		n, err := readFifo.Read(buf)
// 		if err != nil {
// 			t.Errorf("failed to read data: %v", err)
// 			return
// 		}
// 		if n != len(testData) || !bytes.Equal(buf, testData) {
// 			t.Errorf("read data mismatch: expected %v, got %v", testData, buf[:n])
// 		}
// 	}()

// 	// write task
// 	go func() {
// 		defer wg.Done()

// 		// wait for read task to be ready
// 		time.Sleep(100 * time.Millisecond)

// 		// create fifo
// 		writeFifo := new(fifo)
// 		err := writeFifo.init(fifoPath, FifoModeWrite, &options{})
// 		if err != nil {
// 			t.Errorf("failed to init fifo: %v", err)
// 			return
// 		}
// 		defer writeFifo.Close()

// 		// write data
// 		n, err := writeFifo.Write(testData)
// 		if err != nil {
// 			t.Errorf("failed to write data: %v", err)
// 			return
// 		}
// 		if n != len(testData) {
// 			t.Errorf("write data length mismatch: expected %d, got %d", len(testData), n)
// 		}
// 	}()

// 	wg.Wait()
// }

// func TestFifoWrite(t *testing.T) {
// 	dir := beforeTest()
// 	defer afterTest(dir)

// 	fifoPath := filepath.Join(dir, "fifo_write")
// 	opts := &options{}

// 	cycle, caps := 10000, 256
// 	msg, buf := make([]byte, caps), make([]byte, caps)
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	var count int32
// 	expect := int32(cycle * caps)
// 	opts.onData = func(ctx context.Context, fifo Fifo) error {
// 		n, err := fifo.Read(buf)
// 		MustNil(t, err)
// 		if atomic.AddInt32(&count, int32(n)) >= expect {
// 			wg.Done()
// 		}
// 		return nil
// 	}
// 	r, w := &fifo{}, &fifo{}
// 	err := r.init(fifoPath, FifoModeRead, opts)
// 	MustNil(t, err)
// 	err = w.init(fifoPath, FifoModeWrite, opts)
// 	MustNil(t, err)

// 	for i := 0; i < cycle; i++ {
// 		n, err := w.Write(msg)
// 		MustNil(t, err)
// 		Equal(t, n, len(msg))
// 	}
// 	wg.Wait()
// 	Equal(t, atomic.LoadInt32(&count), expect)

// 	r.Close()
// }

// func TestFifoLargeWrite(t *testing.T) {
// 	dir := beforeTest()
// 	defer afterTest(dir)

// 	fmt.Println("start testing Write event in epoll_wait")
// 	totalSize := 1024 * 1024 * 10 // 10MB
// 	readSize := 1024              // 1KB

// 	fifoPath := filepath.Join(dir, "fifo_large_write")

// 	var wg sync.WaitGroup
// 	wg.Add(1)

// 	// trace read/write process
// 	var totalRead int64

// 	opts := &options{}
// 	opts.onData = func(ctx context.Context, fifo Fifo) error {
// 		if fifo.Reader().Len() > 0 {
// 			var size int
// 			if fifo.Reader().Len() > readSize {
// 				size = readSize
// 			} else {
// 				size = fifo.Reader().Len()
// 			}
// 			_, err := fifo.Reader().Next(size)
// 			MustNil(t, err)
// 			fifo.Reader().Release()
// 			newTotalRead := atomic.AddInt64(&totalRead, int64(size))

// 			time.Sleep(3 * time.Millisecond)

// 			if newTotalRead >= int64(totalSize) {
// 				wg.Done()
// 			}
// 		}
// 		return nil
// 	}

// 	r, w := &fifo{}, &fifo{}
// 	err := r.init(fifoPath, FifoModeRead, opts)
// 	MustNil(t, err)
// 	err = w.init(fifoPath, FifoModeWrite, opts)
// 	MustNil(t, err)
// 	fmt.Printf("readFifo: %d, writeFifo: %d\n", r.FD(), w.FD())

// 	// set fifo buffer size to 64KB
// 	syscall.Syscall(syscall.SYS_FCNTL, uintptr(r.FD()), syscall.F_SETPIPE_SZ, uintptr(65536)) // 64KB

// 	largeMsg := make([]byte, totalSize)
// 	fmt.Println("Start writing large message")

// 	n, err := w.Write(largeMsg)
// 	MustNil(t, err)
// 	Equal(t, n, len(largeMsg))

// 	wg.Wait()

// 	fmt.Println("Total read: ", totalRead)
// 	fmt.Println("should read all data: ", totalRead == int64(totalSize))
// 	r.Close()
// 	w.Close()
// }

// func TestFifoRead(t *testing.T) {
// 	dir := beforeTest()
// 	defer afterTest(dir)

// 	fifoPath := filepath.Join(dir, "fifo_read")
// 	r, w := &fifo{}, &fifo{}
// 	err := r.init(fifoPath, FifoModeRead, nil)
// 	MustNil(t, err)
// 	err = w.init(fifoPath, FifoModeWrite, nil)
// 	MustNil(t, err)

// 	size, cycleTime := 256, 10000
// 	msg := make([]byte, size)
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < cycleTime; i++ {
// 			buf, err := r.Reader().Next(size)
// 			MustNil(t, err)
// 			Equal(t, len(buf), size)
// 			r.Reader().Release()
// 		}
// 	}()
// 	for i := 0; i < cycleTime; i++ {
// 		n, err := w.Write(msg)
// 		MustNil(t, err)
// 		Equal(t, n, len(msg))
// 	}

// 	wg.Wait()
// 	r.Close()
// }
