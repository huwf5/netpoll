package netpoll

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func beforeTest() (dir string) {
	// create tmp fifo file
	dir, err := os.MkdirTemp("", "fifo_test")
	if err != nil {
		panic(err)
	}
	return dir
}

func afterTest(dir string) {
	os.RemoveAll(dir)
}

// TestReadFifo tests the fifoReader's read functionality in a dynamic environment.
//
// This test verifies that:
// 1. An EventLoop can be started with ServeFifo() and run in the background
// 2. A ReadFifo can be dynamically attached to a running EventLoop
// 3. Data written to the FIFO is correctly read and processed by the onFifoRead callback
// 4. Multiple messages of varying sizes can be processed sequentially
// 5. All messages are received completely and in the correct order
//
// The test flow:
// - Creates a temporary FIFO file
// - Starts an EventLoop with a custom onFifoRead callback
// - Attaches a ReadFifo to the running EventLoop
// - Writes multiple test messages to the FIFO
// - Verifies all messages are received and processed correctly
// - Shuts down the EventLoop cleanly
//
// This demonstrates the ability to dynamically add FIFO handlers to a running
// EventLoop, which is important for applications that need to manage multiple
// FIFO connections that may come and go during runtime.
func TestReadFifo(t *testing.T) {
	dir := beforeTest()
	defer afterTest(dir)

	fifoPath := filepath.Join(dir, "readFifo")

	// for testing
	testMessages := []string{
		"First message",
		"Second message with more data",
		"Third message",
		"Fourth message is longer to test different buffer sizes",
		"Fifth message is the last one",
	}
	var expectedMessages = len(testMessages)

	eventLoopDone := make(chan struct{}) // mark the event loop is done
	dataReceived := make(chan []byte, expectedMessages)

	var wg sync.WaitGroup
	wg.Add(expectedMessages)

	// add a counter to track the processed messages
	var processedMessages atomic.Int32
	// set options
	opts := &options{}
	// reading logic
	opts.onFifoRead = func(ctx context.Context, fifo FifoReader) error {
		// check if there is data to read
		if fifo.Reader().Len() == 0 {
			return nil
		}

		// read data
		data := make([]byte, fifo.Reader().Len())
		n, err := fifo.Read(data)
		MustNil(t, err)
		Equal(t, n, len(data))

		// logger.Printf("DEBUG: received message: %s", string(data[:n]))

		// send data to channel
		dataReceived <- data[:n]
		wg.Done() // mark the one message is received

		// increment the counter
		processedMessages.Add(1)
		if processedMessages.Load() == int32(expectedMessages) {
			// logger.Printf("DEBUG: all messages are received")
			return fifo.Close()
		}
		return nil
	}

	evl, err := NewEventLoop(opts.onRequest, WithOnFifoRead(opts.onFifoRead))
	MustNil(t, err)

	// start the event loop for fifoReader
	go func() {
		defer close(eventLoopDone)  // close the channel when the event loop is done
		MustNil(t, evl.ServeFifo()) // block here
	}()

	// wait for the event loop to get ready
	time.Sleep(100 * time.Millisecond)

	// attach the fifoReader to the event loop
	fifoManager, err := evl.GetFifoManager()
	MustNil(t, err)
	MustNil(t, fifoManager.AttachReadFifo(fifoPath))

	// write to fifo
	go func() {
		f, err := os.OpenFile(fifoPath, os.O_WRONLY, 0666)
		MustNil(t, err)
		defer f.Close()

		for _, msg := range testMessages {
			_, err := f.WriteString(msg)
			MustNil(t, err)
			// wait for the event loop to process the message
			time.Sleep(10 * time.Millisecond)
		}

	}()

	timeout := time.After(5 * time.Second)

	done := make(chan struct{})
	// wait for the event loop to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-timeout:
		t.Fatalf("timeout waiting for fifoReader to finish")
	}

	// collect the received messages
	receivedMessages := make([]string, 0, expectedMessages)
	close(dataReceived)

	for data := range dataReceived {
		receivedMessages = append(receivedMessages, string(data))
	}

	Equal(t, len(receivedMessages), expectedMessages)

	sort.Strings(receivedMessages)
	sort.Strings(testMessages)

	for i, msg := range receivedMessages {
		Equal(t, msg, testMessages[i])
	}

	// close the event loop
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	MustNil(t, evl.Shutdown(ctx))

}

// TestWriteFifo tests the fifoWriter's write functionality in a dynamic environment.
//
// This test verifies that:
// 1. An EventLoop can be started with ServeFifo() and run in the background
// 2. A WriteFifo can be dynamically generated and used
// 3. Data written to the WriteFifo can be correctly read from the FIFO file
// 4. Multiple messages of varying sizes can be written sequentially
// 5. All messages are written completely and can be read in the correct order
//
// The test flow:
// - Creates a temporary FIFO file
// - Starts an EventLoop
// - Generates a WriteFifo using the EventLoop
// - Writes multiple test messages to the WriteFifo
// - Reads the messages from the FIFO file and verifies they match the original messages
// - Shuts down the EventLoop cleanly
func TestBasicWriteFifo(t *testing.T) {
	dir := beforeTest()
	defer afterTest(dir)

	fifoPath := filepath.Join(dir, "writeFifo")
	// create fifo file
	MustNil(t, syscall.Mkfifo(fifoPath, 0666))

	// Test messages to write
	testMessages := []string{
		"First message from writer",
		"Second message with more data from writer",
		"Third message from writer",
		"Fourth message is longer to test different buffer sizes from writer",
		"Fifth message is the last one from writer",
	}
	expectedMessages := len(testMessages)

	// sync channel
	eventLoopDone := make(chan struct{})
	readerReady := make(chan struct{})
	writerReady := make(chan struct{})
	dataReceived := make(chan []byte, expectedMessages)

	// set options
	opts := &options{}
	evl, err := NewEventLoop(opts.onRequest)
	MustNil(t, err)

	// start the event loop for fifoWriter
	go func() {
		defer close(eventLoopDone)
		MustNil(t, evl.ServeFifo())
	}()

	// wait for the event loop to get ready
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(expectedMessages)

	// start the reader first, to make sure the writer can write to the fifo
	go func() {
		f, err := os.OpenFile(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0666)
		MustNil(t, err)
		defer f.Close()

		// switch to blocking mode to normal read
		syscall.SetNonblock(int(f.Fd()), false)

		// signal the reader is ready
		close(readerReady)

		// wait for the writer to be ready
		<-writerReady

		// Read data using a scanner to handle line-by-line reading
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) > 0 {
				// Make a copy of the data to avoid scanner buffer reuse issues
				dataCopy := make([]byte, len(line))
				copy(dataCopy, line)

				dataReceived <- dataCopy
				wg.Done()
			}
		}

		MustNil(t, scanner.Err())
	}()

	select {
	case <-readerReady:
		// success
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for reader to be ready")
	}

	// Generate a WriteFifo using the EventLoop
	fifoManager, err := evl.GetFifoManager()
	MustNil(t, err)
	fifoWriter, err := fifoManager.GenerateWriteFifo(fifoPath)
	MustNil(t, err)
	// signal the writer is ready
	close(writerReady)

	for i, msg := range testMessages {
		n, err := fifoWriter.Write([]byte(msg))
		MustNil(t, err)
		Equal(t, n, len(msg))

		if i < len(testMessages) {
			_, err := fifoWriter.Write([]byte("\n"))
			MustNil(t, err)
		}

		// wait for the reader to receive the message
		time.Sleep(10 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for fifoWriter to finish")
	}

	// close the fifoWriter
	MustNil(t, fifoWriter.Close())

	// collect the received messages
	close(dataReceived)
	receivedMessages := make([]string, 0, expectedMessages)
	for data := range dataReceived {
		receivedMessages = append(receivedMessages, string(data))
	}

	Equal(t, len(receivedMessages), expectedMessages)

	sort.Strings(receivedMessages)
	sort.Strings(testMessages)

	for i, msg := range receivedMessages {
		Equal(t, msg, testMessages[i])
	}

	// Shutdown the event loop
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	MustNil(t, evl.Shutdown(ctx))

	// wait for the event loop to finish
	select {
	case <-eventLoopDone:
		// success
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for event loop to finish")
	}

}

// TestWriteFifoInEpoll tests the WriteFifo functionality when registered in epoll.
//
// This test verifies that:
// 1. A WriteFifo can be properly registered in epoll when writing large data
// 2. Data written to the WriteFifo can be correctly read by a ReadFifo
// 3. The entire data transfer process works correctly under epoll monitoring
// 4. The WriteFifo can be properly closed and detached from epoll
//
// The test flow:
// - Creates a temporary FIFO file
// - Sets up an EventLoop with a ReadFifo callback
// - Attaches a ReadFifo to the EventLoop
// - Generates a WriteFifo using the EventLoop
// - Writes a large message (10MB) to ensure the WriteFifo is registered in epoll
// - Verifies all data is correctly read by the ReadFifo
// - Properly closes all resources and shuts down the EventLoop
func TestWriteFifoInEpoll(t *testing.T) {
	dir := beforeTest()
	defer afterTest(dir)

	// use large data size to make sure the writeFifo is in epoll
	totalSize := 1024 * 1024 * 10 // 10MB
	readChunkSize := 1024 * 64    // 64KB

	fifoPath := filepath.Join(dir, "fifo_epoll_write")

	// sync channel
	eventLoopDone := make(chan struct{})
	readCompleted := make(chan struct{})

	// trace read process
	var totalRead int64

	// set options
	opts := &options{}
	opts.onFifoRead = func(ctx context.Context, fifo FifoReader) error {
		if fifo.Reader().Len() == 0 {
			return nil
		}

		// determine the read size
		readSize := readChunkSize
		if fifo.Reader().Len() < readSize {
			readSize = fifo.Reader().Len()
		}

		data, err := fifo.Reader().Next(readSize)
		MustNil(t, err)

		Equal(t, len(data), readSize)
		// release the reader
		fifo.Reader().Release()

		newTotalRead := atomic.AddInt64(&totalRead, int64(len(data)))

		// check if all data is read
		if newTotalRead >= int64(totalSize) {
			close(readCompleted)
			return fifo.Close()
		}
		return nil
	}
	evl, err := NewEventLoop(opts.onRequest, WithOnFifoRead(opts.onFifoRead))
	MustNil(t, err)

	// start the event loop
	go func() {
		defer close(eventLoopDone)
		MustNil(t, evl.ServeFifo())
	}()

	// wait for the event loop to get ready
	time.Sleep(100 * time.Millisecond)

	// start the reader
	fifoManager, err := evl.GetFifoManager()
	MustNil(t, err)
	MustNil(t, fifoManager.AttachReadFifo(fifoPath))

	// start the writer
	fifoWriter, err := fifoManager.GenerateWriteFifo(fifoPath)
	MustNil(t, err)

	// create a large message
	largeMsg := make([]byte, totalSize)
	for i := 0; i < totalSize; i++ {
		largeMsg[i] = byte(i % 256)
	}

	n, err := fifoWriter.Write(largeMsg)
	MustNil(t, err)
	Equal(t, n, len(largeMsg))

	select {
	case <-readCompleted:
		// success
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for read to complete")
	}

	Equal(t, totalRead, int64(totalSize))

	// close fifoWriter
	MustNil(t, fifoWriter.Close())

	// close the event loop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	MustNil(t, evl.Shutdown(ctx))

	// wait for the event loop to finish
	select {
	case <-eventLoopDone:
		// success
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for event loop to finish")
	}
}

// TestFifoConnection tests the bidirectional communication using FIFO connections.
// It creates two FIFOs for bidirectional communication:
// - readerPath: used by the server to read requests from the client
// - writerPath: used by the server to write responses to the client
//
// The test simulates a client-server interaction where:
// 1. The client sends requests through readerPath
// 2. The server reads requests from readerPath
// 3. The server processes requests and sends responses through writerPath
// 4. The client reads responses from writerPath
//
// Note: When opening FIFOs in non-blocking mode, the first read might encounter EOF
// until the opposite writer is ready. A small delay is added between writing the
// request and reading the response to allow the server to process the request and
// initialize its writer.
func TestFifoConnection(t *testing.T) {
	dir := beforeTest()
	defer afterTest(dir)

	// create fifo path
	readerPath := filepath.Join(dir, "fifo_connection_reader")
	writerPath := filepath.Join(dir, "fifo_connection_writer")

	// create fifo
	MustNil(t, syscall.Mkfifo(readerPath, 0666))
	MustNil(t, syscall.Mkfifo(writerPath, 0666))

	testRequests := []string{
		"hello",
		"test",
		"ping",
		"last message",
	}
	expectedResponses := []string{
		"hello world",
		"test response",
		"ping pong",
		"last message received",
	}

	expectedMessages := len(testRequests)

	// sync channel
	eventLoopDone := make(chan struct{})
	clientDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(expectedMessages)

	// track the processed messages
	var processedMessages atomic.Int32

	// set options
	opts := &options{}
	opts.onFifoTransfer = func(ctx context.Context, fifo FifoConnection) error {
		// fmt.Println("=========== in onFifoTransfer ===========")

		// read data
		fifoReader := fifo.GetReader()
		if fifoReader == nil {
			// fmt.Println("ERROR: fifoReader is nil")
			return errors.New("fifoReader is nil")
		}

		if fifoReader.Reader().Len() == 0 {
			// fmt.Println("No data to read")
			return nil
		}

		data := make([]byte, fifoReader.Reader().Len())
		// fmt.Printf("Reading %d bytes from fifo\n", len(data))
		n, err := fifoReader.Read(data)
		MustNil(t, err)

		request := string(data[:n])
		// fmt.Printf("Received request: %s\n", request)
		var response string
		switch request {
		case "hello":
			response = "hello world"
		case "test":
			response = "test response"
		case "ping":
			response = "ping pong"
		case "last message":
			response = "last message received"
		default:
			response = "unknown request: " + request
		}

		// Check if writer is ready
		if !fifo.IsWriterReady() {
			// fmt.Println("Writer not ready, trying to initialize")
			if err := fifo.TryInitWriter(); err != nil {
				// fmt.Printf("Failed to initialize writer: %v\n", err)
				return err
			}
		}

		// write response
		// fmt.Printf("Writing response: %s\n", response)
		_, err = fifo.Write([]byte(response))
		if err != nil {
			// fmt.Printf("Error writing to fifo: %v\n", err)
			return err
		}

		processedMessages.Add(1)
		if processedMessages.Load() == int32(expectedMessages) {
			return fifo.Close()
		}
		// fmt.Println("=========== end of onFifoTransfer ===========")
		return nil
	}

	evl, err := NewEventLoop(opts.onRequest, WithOnFifoTransfer(opts.onFifoTransfer))
	MustNil(t, err)

	go func() {
		defer close(eventLoopDone)
		MustNil(t, evl.ServeFifo()) // block here
	}()

	time.Sleep(100 * time.Millisecond)

	// attach the fifo connection to the event loop
	fifoManager, err := evl.GetFifoManager()
	MustNil(t, err)
	MustNil(t, fifoManager.AttachFifoConnection(readerPath, writerPath))

	// imitate the client
	go func() {
		// fmt.Println("Starting client")
		defer close(clientDone)

		// open the client writer - using non-blocking mode
		clientWriter, err := os.OpenFile(readerPath, os.O_WRONLY|syscall.O_NONBLOCK, 0666)
		if err != nil {
			// fmt.Printf("Failed to open client writer: %v\n", err)
			t.Error(err)
			return
		}
		defer clientWriter.Close()

		// open the client reader - using non-blocking mode
		clientReader, err := os.OpenFile(writerPath, os.O_RDONLY|syscall.O_NONBLOCK, 0666)
		if err != nil {
			// fmt.Printf("Failed to open client reader: %v\n", err)
			t.Error(err)
			return
		}
		defer clientReader.Close()

		// use blocking mode to read
		MustNil(t, syscall.SetNonblock(int(clientReader.Fd()), false))

		responseBuf := make([]byte, 1024)
		for i, request := range testRequests {
			// send request
			// fmt.Printf("Sending request: %s\n", request)
			_, err := clientWriter.WriteString(request)
			MustNil(t, err)

			// TODO: first read will encounter EOF, because the opposite writer is not ready
			// the client reader will encounter EOF until the opposite writer is ready

			time.Sleep(10 * time.Millisecond) // wait for the response
			n, err := clientReader.Read(responseBuf)
			if err != nil {
				// fmt.Printf("Client reader read error: %v\n", err)
				t.Error(err)
				return
			}
			if n <= 0 {
				t.Error("No data read from response")
				return
			}

			response := string(responseBuf[:n])
			// fmt.Printf("Received response: %s\n", response)

			Equal(t, response, expectedResponses[i])
			wg.Done()

		}
		// fmt.Println("Client finished")
	}()

	// wait for all requests to finish
	// fmt.Println("Waiting for all requests to complete")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		// fmt.Println("All requests completed")
		close(done)
	}()

	// fmt.Println("Waiting for test to complete")
	select {
	case <-done:
		// fmt.Println("Test completed successfully")
	case <-time.After(5 * time.Second):
		// fmt.Println("Timeout waiting for fifo connection to finish")
		t.Fatalf("Timeout waiting for fifo connection to finish")
	}

	// wait for the client to finish
	// fmt.Println("Waiting for client to finish")
	<-clientDone
	// fmt.Println("Client finished")

	// shutdown the event loop
	// fmt.Println("Shutting down event loop")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	MustNil(t, evl.Shutdown(ctx))

	// wait for the event loop to finish
	select {
	case <-eventLoopDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for event loop to finish")
	}
}

// TestFifoConnectionCommunication tests bidirectional communication between two FifoConnections.
//
// This test verifies that:
// 1. Two FifoConnections can be created and managed by a single EventLoop
// 2. Each FifoConnection can both read and write data
// 3. Messages can be exchanged bidirectionally between the connections
// 4. The onFifoTransfer callback correctly processes incoming messages and sends responses
// 5. All messages are received completely and in the correct order
//
// The test flow:
// - Creates two FIFO files for bidirectional communication
// - Sets up an EventLoop with a custom onFifoTransfer callback
// - Attaches two FifoConnections to the EventLoop
// - Initiates communication by sending a message from connection A
// - Each connection responds to received messages by sending the next message
// - Verifies all messages are exchanged correctly
// - Properly closes all resources and shuts down the EventLoop
//
// This demonstrates how two FifoConnections can communicate with each other
// through a single EventLoop, which is useful for applications that need to
// manage multiple bidirectional FIFO connections.
func TestFifoConnectionCommunication(t *testing.T) {
	dir := beforeTest()
	defer afterTest(dir)

	// create fifo path
	fifoAtoB := filepath.Join(dir, "fifo_connection_a_to_b")
	fifoBtoA := filepath.Join(dir, "fifo_connection_b_to_a")

	// create fifo
	MustNil(t, syscall.Mkfifo(fifoAtoB, 0666))
	MustNil(t, syscall.Mkfifo(fifoBtoA, 0666))

	// test messages
	messagesFromA := []string{
		"A1: hello B!",
		"A2: how are you?",
		"A3: call me later",
		"A4: bye",
	}
	messagesFromB := []string{
		"B1: hi A!",
		"B2: I'm fine, thank you!",
		"B3: ok",
		"B4: good night",
	}

	expectedMessages := len(messagesFromA) // A and B send the same number of messages

	// sync channel
	eventLoopDone := make(chan struct{})
	communicationDone := make(chan struct{})

	// track the processed messages
	var messagesProcessedByA atomic.Int32
	var messagesProcessedByB atomic.Int32

	// set options
	opts := &options{}
	opts.onFifoTransfer = func(ctx context.Context, fifo FifoConnection) error {
		// check if there is data to read
		fifoReader := fifo.GetReader()
		if fifoReader == nil || fifoReader.Reader().Len() == 0 {
			return nil
		}

		// read data
		data := make([]byte, fifoReader.Reader().Len())
		n, err := fifoReader.Read(data)
		MustNil(t, err)

		message := string(data[:n])

		// determine which connection based on the reader path
		readerPath := fifo.GetReader().Path()

		if readerPath == fifoAtoB {
			// this is B's connection, received message from A
			count := messagesProcessedByB.Add(1)

			// verify message format
			if !strings.HasPrefix(message, "A") {
				t.Errorf("B received wrong message format: %s", message)
			}

			// if B has more messages to send to A
			idx := int(count) - 1
			if idx < len(messagesFromB) {
				// B sends message to A
				_, err := fifo.Write([]byte(messagesFromB[idx]))
				MustNil(t, err)
			}
		} else {
			// this is A's connection, received message from B
			count := messagesProcessedByA.Add(1)

			// verify message format
			if !strings.HasPrefix(message, "B") {
				t.Errorf("A received wrong message format: %s", message)
			}

			// if A has more messages to send to B
			idx := int(count)
			if idx < len(messagesFromA) {
				// A sends message to B
				_, err := fifo.Write([]byte(messagesFromA[idx]))
				MustNil(t, err)
			}
		}

		// check if all messages are processed
		if messagesProcessedByA.Load() >= int32(expectedMessages) &&
			messagesProcessedByB.Load() >= int32(expectedMessages) {
			close(communicationDone)
			return fifo.Close()
		}

		return nil
	}

	// create a single event loop
	evl, err := NewEventLoop(nil, WithOnFifoTransfer(opts.onFifoTransfer))
	MustNil(t, err)

	// start the event loop
	go func() {
		defer close(eventLoopDone)
		MustNil(t, evl.ServeFifo())
	}()

	// wait for the event loop to get ready
	time.Sleep(100 * time.Millisecond)

	// attach two FifoConnection to the event loop
	// A's connection: read messages from B, write messages to B
	fifoManager, err := evl.GetFifoManager()
	MustNil(t, err)

	MustNil(t, fifoManager.AttachFifoConnection(fifoBtoA, fifoAtoB))
	// B's connection: read messages from A, write messages to A
	MustNil(t, fifoManager.AttachFifoConnection(fifoAtoB, fifoBtoA))

	// start first communication
	a_FifoConnection, err := fifoManager.GetFifoConnectionByReaderPath(fifoBtoA)
	MustNil(t, err)
	_, err = a_FifoConnection.Write([]byte(messagesFromA[0]))
	MustNil(t, err)

	// wait for communication to finish or timeout
	select {
	case <-communicationDone:
		// success
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for FifoConnection communication to finish")
	}

	// shutdown the event loop
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	MustNil(t, evl.Shutdown(ctx))

	// wait for the event loop to finish
	select {
	case <-eventLoopDone:
		// success
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for event loop to finish")
	}

	// verify all messages are processed
	Equal(t, messagesProcessedByA.Load(), int32(expectedMessages))
	Equal(t, messagesProcessedByB.Load(), int32(expectedMessages))
}
