package domain

import (
	pb "github.com/kgantsov/dlock/internal/proto"
	"google.golang.org/protobuf/proto"
)

// LockEntry is the canonical record of a lock in the store.
type LockEntry struct {
	Key          string
	Owner        string
	FencingToken uint64
	ExpireAt     int64
}

func (m *LockEntry) ToBytes() ([]byte, error) {
	msg := m.ToProto()
	return proto.Marshal(msg)
}

func (m *LockEntry) ToProto() *pb.LockEntry {
	return &pb.LockEntry{
		Key:          m.Key,
		Owner:        m.Owner,
		FencingToken: m.FencingToken,
		ExpireAt:     m.ExpireAt,
	}
}

func LockEntryFromBytes(data []byte) (*LockEntry, error) {
	var msg pb.LockEntry
	err := proto.Unmarshal(data, &msg)
	return &LockEntry{
		Key:          msg.Key,
		Owner:        msg.Owner,
		FencingToken: msg.FencingToken,
		ExpireAt:     msg.ExpireAt,
	}, err
}

func LockEntryProtoFromBytes(data []byte) (*pb.LockEntry, error) {
	var msg pb.LockEntry
	err := proto.Unmarshal(data, &msg)
	return &msg, err
}
