package transports

import (
	"io"
)

type Connection interface {
	io.ReadWriteCloser
}

type Dialer interface {
	Dial(addr string) (conn Connection, err error)
}

type Listener interface {
	Accept() (conn Connection, err error)
	Close()
}

type Transport interface {
	Dialer() (dialer Dialer)
	Listener() (listener Listener)
}
