package griak

type Reader interface {
	Read() (code byte, data []byte, err error)
}
