package mysql

import (
	"context"
	"database/sql/driver"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type mysqlConn struct {
	netConn  net.Conn
	status   statusFlag
	closed   int32
	quitSent int32
	sequence byte
	mu       sync.Mutex
}

func (mc *mysqlConn) sendQuit() {
	if atomic.CompareAndSwapInt32(&mc.quitSent, 0, 1) {
		if mc.status >= statusAuthenticated && mc.netConn != nil {
			_ = mc.netConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			_ = mc.writeCommandPacket(comQuit)
		}
	}
}

func (mc *mysqlConn) Close() (err error) {
	if atomic.CompareAndSwapInt32(&mc.closed, 0, 1) {
		defer func() {
			if mc.netConn != nil {
				closeErr := mc.netConn.Close()
				if err == nil {
					err = closeErr
				}
			}
		}()
		mc.sendQuit()
		return nil
	}
	return ErrInvalidConn
}

func (mc *mysqlConn) watchCancel(ctx context.Context) (func(), error) {
	if ctx.Done() == nil {
		return func() {}, nil
	}

	if atomic.LoadInt32(&mc.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	stopWatching := make(chan struct{})
	done := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = mc.Close()
		case <-stopWatching:
		}
		close(done)
	}()

	return func() {
		close(stopWatching)
		<-done
	}, nil
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	if atomic.LoadInt32(&mc.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	return nil, nil
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	if atomic.LoadInt32(&mc.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	return nil, nil
}
