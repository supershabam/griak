package griak

type Writer interface {
	Write(code byte, data []byte) error
}
