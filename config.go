package raft

type ServerSuffrage int

const (
	Voter ServerSuffrage = iota + 1
	Nonvoter
)

func (s ServerSuffrage) String() string {
	switch s {
	case Voter:
		return "Voter"
	case Nonvoter:
		return "Nonvoter"
	}
	return "ServerSuffrage"
}

type Server struct {
	Suffrage ServerSuffrage
	Id       string
	Address  string
}

type Configuration struct {
	Servers []Server
}
