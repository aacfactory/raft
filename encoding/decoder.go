package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"io"
	"time"
)

func NewDecoderFromReader(r io.Reader) *Decoder {
	return &Decoder{
		reader: r,
	}
}

func NewDecoder(p []byte) *Decoder {
	return &Decoder{
		reader: bytes.NewReader(p),
	}
}

type Decoder struct {
	reader io.Reader
}

func (decoder *Decoder) Uint64() (v uint64, err error) {
	p := make([]byte, 8)
	n, readErr := decoder.reader.Read(p)
	if readErr != nil {
		err = fmt.Errorf("decoder get uint64 failed, %v", readErr)
		return
	}
	if n != 8 {
		err = fmt.Errorf("decoder get uint64 failed, invalid content")
		return
	}
	v = binary.BigEndian.Uint64(p)
	return
}

func (decoder *Decoder) Bool() (v bool, err error) {
	p, readErr := decoder.Uint64()
	if readErr != nil {
		err = fmt.Errorf("decoder get bool failed, %v", readErr)
		return
	}
	v = p > 0
	return
}

func (decoder *Decoder) Time() (v time.Time, err error) {
	p, readErr := decoder.Uint64()
	if readErr != nil {
		err = fmt.Errorf("decoder get time failed, %v", readErr)
		return
	}
	v = time.Unix(0, int64(p))
	return
}

func (decoder *Decoder) Raw() (p []byte, err error) {
	buf := bytebufferpool.Get()
	for {
		b := make([]byte, 1024)
		n, readErr := decoder.reader.Read(b)
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			bytebufferpool.Put(buf)
			err = fmt.Errorf("decoder get raw failed, %v", readErr)
			return
		}
		if n > 0 {
			_, _ = buf.Write(b[0:n])
		}
	}
	p = buf.Bytes()
	bytebufferpool.Put(buf)
	return
}

func (decoder *Decoder) LengthFieldBasedFrame() (p []byte, err error) {
	length, lengthErr := decoder.Uint64()
	if lengthErr != nil {
		err = fmt.Errorf("decoder get length field frame failed, %v", lengthErr)
		return
	}
	if length == 0 {
		p = make([]byte, 0, 1)
		return
	}
	p = make([]byte, length)
	n, readErr := decoder.reader.Read(p)
	if readErr != nil {
		p = nil
		err = fmt.Errorf("decoder get length field frame failed, %v", readErr)
		return
	}
	if uint64(n) != length {
		p = nil
		err = fmt.Errorf("decoder get length field frame failed, invalid length")
		return
	}
	return
}

func (decoder *Decoder) LengthFieldBasedStringFrame() (s string, err error) {
	p, decodeErr := decoder.LengthFieldBasedFrame()
	if decodeErr != nil {
		err = decodeErr
		return
	}
	s = string(p)
	return
}
