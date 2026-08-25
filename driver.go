package mysql

import (
	"database/sql"
	"database/sql/driver"
	"net"
)

func init() {
	sql.Register("mysql", &MySQLDriver{})
}

type MySQLDriver struct{}

func (d *MySQLDriver) Open(name string) (driver.Conn, error) {
	conn, err := net.Dial("tcp", name)
	if err != nil {
		return nil, err
	}
	return &mysqlConn{
		netConn: conn,
		status:  statusAuthenticated,
	}, nil
}
