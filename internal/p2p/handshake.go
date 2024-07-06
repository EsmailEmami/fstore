package p2p

type HandshakeFunc func(Peer) error

func NOPHandshake(Peer) error { return nil }
