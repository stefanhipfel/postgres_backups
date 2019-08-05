package writer

import "bufio"

type Writer interface {
	Write(f string, r *bufio.Reader) error
}
