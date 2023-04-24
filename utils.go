package kafka

import "net"

type networkAddress struct {
	network string
	address string
}

func (a *networkAddress) Network() string { return a.network }

func (a *networkAddress) String() string { return a.address }

// tcp constructs an address with the network set to "tcp".
func tcp(address string) net.Addr { return makeAddr("tcp", address) }

func makeAddr(network, address string) net.Addr {
	host, port, _ := net.SplitHostPort(address)
	if port == "" {
		port = "9092"
	}
	if host == "" {
		host = address
	}
	return &networkAddress{
		network: network,
		address: net.JoinHostPort(host, port),
	}
}