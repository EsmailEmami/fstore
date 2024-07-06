package p2p

const (
	StreamMessage = 0x0
	TextMessage   = 0x1
)

type RPC struct {
	From    string
	Payload []byte

	isStream bool
	isText   bool
}
