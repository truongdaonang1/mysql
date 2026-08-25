package mysql

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuthenticatedConnectionSendQuitOnClose(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	mc := &mysqlConn{
		netConn: clientConn,
		status:  statusAuthenticated,
	}

	bufChan := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := serverConn.Read(buf)
		bufChan <- buf[:n]
		_ = serverConn.Close()
	}()

	err := mc.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	data := <-bufChan
	expected := []byte{0x01, 0x00, 0x00, 0x00, comQuit}
	if !bytes.Equal(data, expected) {
		t.Fatalf("expected COM_QUIT packet %v, got %v", expected, data)
	}
}

func TestUnauthenticatedConnectionNoQuitOnClose(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	mc := &mysqlConn{
		netConn: clientConn,
		status:  statusUnauthenticated,
	}

	bufChan := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := io.ReadFull(serverConn, buf)
		bufChan <- buf[:n]
		_ = serverConn.Close()
	}()

	err := mc.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	data := <-bufChan
	if len(data) != 0 {
		t.Fatalf("expected no data sent for unauthenticated close, got %v", data)
	}
}

func TestContextCancellationSendsQuit(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	mc := &mysqlConn{
		netConn: clientConn,
		status:  statusAuthenticated,
	}

	bufChan := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := serverConn.Read(buf)
		bufChan <- buf[:n]
		_ = serverConn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cleanup, err := mc.watchCancel(ctx)
	if err != nil {
		t.Fatalf("failed to watch cancel: %v", err)
	}

	cancel()

	select {
	case data := <-bufChan:
		expected := []byte{0x01, 0x00, 0x00, 0x00, comQuit}
		if !bytes.Equal(data, expected) {
			t.Fatalf("expected COM_QUIT packet %v, got %v", expected, data)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for COM_QUIT on cancel")
	}

	cleanup()
	if atomic.LoadInt32(&mc.closed) != 1 {
		t.Fatalf("expected connection to be marked closed")
	}
}

func TestConcurrentCloseOnlyOneQuit(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	mc := &mysqlConn{
		netConn: clientConn,
		status:  statusAuthenticated,
	}

	readBytes := make(chan []byte, 1)
	go func() {
		var all []byte
		buf := make([]byte, 32)
		for {
			n, err := serverConn.Read(buf)
			if n > 0 {
				all = append(all, buf[:n]...)
			}
			if err != nil {
				break
			}
		}
		readBytes <- all
		_ = serverConn.Close()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mc.Close()
		}()
	}
	wg.Wait()

	data := <-readBytes
	expected := []byte{0x01, 0x00, 0x00, 0x00, comQuit}
	if !bytes.Equal(data, expected) {
		t.Fatalf("expected exactly one COM_QUIT (%v), got %v", expected, data)
	}
}
