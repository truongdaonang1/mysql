package mysql

import (
	"errors"
)

var (
	ErrInvalidConn = errors.New("invalid connection")
)

func (mc *mysqlConn) writePacket(data []byte) error {
	pktLen := len(data)
	if pktLen == 0 {
		return nil
	}

	header := []byte{
		byte(pktLen),
		byte(pktLen >> 8),
		byte(pktLen >> 16),
		mc.sequence,
	}
	mc.sequence++

	if mc.netConn == nil {
		return ErrInvalidConn
	}

	if _, err := mc.netConn.Write(append(header, data...)); err != nil {
		return err
	}
	return nil
}

func (mc *mysqlConn) writeCommandPacket(command byte) error {
	mc.sequence = 0
	return mc.writePacket([]byte{command})
}

func (mc *mysqlConn) writeCommandPacketStr(command byte, arg string) error {
	mc.sequence = 0
	pkt := make([]byte, 1+len(arg))
	pkt[0] = command
	copy(pkt[1:], arg)
	return mc.writePacket(pkt)
}
