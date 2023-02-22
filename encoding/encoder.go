package encoding

import (
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"time"
)

func NewEncoder() *Encoder {
	return &Encoder{
		buf: bytebufferpool.Get(),
	}
}

type Encoder struct {
	buf *bytebufferpool.ByteBuffer
}

func (encoder *Encoder) Bytes() (p []byte) {
	p = encoder.buf.Bytes()
	return
}

func (encoder *Encoder) WriteUint64(v uint64) {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, v)
	_, _ = encoder.buf.Write(p)
	return
}

func (encoder *Encoder) WriteBool(v bool) {
	n := uint64(0)
	if v {
		n = uint64(1)
	}
	encoder.WriteUint64(n)
	return
}

func (encoder *Encoder) WriteTime(v time.Time) {
	encoder.WriteUint64(uint64(v.UnixNano()))
	return
}

func (encoder *Encoder) WriteRaw(p []byte) {
	if p == nil || len(p) == 0 {
		return
	}
	_, _ = encoder.buf.Write(p)
}

func (encoder *Encoder) WriteLengthFieldBasedFrame(p []byte) {
	if p == nil {
		p = []byte{}
	}
	length := uint64(len(p))
	encoder.WriteUint64(length)
	if length > 0 {
		encoder.WriteRaw(p)
	}
}

func (encoder *Encoder) Reset() {
	encoder.buf.Reset()
}

func (encoder *Encoder) Close() {
	bytebufferpool.Put(encoder.buf)
}
