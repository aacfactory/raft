package raft

import (
	"errors"
	"fmt"
	"strings"
)

type Addresses interface {
	Local() (local string, err error)
	Members() (addresses []string, err error)
}

func DesignatedAddresses(local string, members ...string) Addresses {
	return &designatedAddresses{
		local:   strings.TrimSpace(local),
		members: members,
	}
}

type designatedAddresses struct {
	local   string
	members []string
}

func (addr *designatedAddresses) Local() (local string, err error) {
	if local == "" {
		err = errors.New("local address is required")
		return
	}
	return
}

func (addr *designatedAddresses) Members() (members []string, err error) {
	members = make([]string, 0, 1)
	for _, address := range addr.members {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		has := false
		for _, member := range members {
			if member == address {
				has = true
				break
			}
		}
		if has {
			continue
		}
		members = append(members, address)
	}
	if len(members) == 0 {
		err = fmt.Errorf("addresses are invalid")
		return
	}
	return
}
