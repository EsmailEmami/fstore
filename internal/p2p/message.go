package p2p

const (
	textMessage = 0x0
	lockMessage = 0x1
)

var (
	LockMessage = []byte{lockMessage}
	TextMessage = []byte{textMessage}
)

type RPC struct {
	From    string
	Payload []byte

	lock bool
}
