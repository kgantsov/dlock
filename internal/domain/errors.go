package domain

import "errors"

var (
	ErrFailedToJoinNode = errors.New("failed to join node")

	ErrInvalidKey          = errors.New("invalid key")
	ErrInvalidOwner        = errors.New("invalid owner")
	ErrInvalidTTL          = errors.New("invalid ttl")
	ErrInvalidFencingToken = errors.New("invalid fencing token")

	ErrLockNotFound         = errors.New("lock not found")
	ErrLockExpired          = errors.New("lock expired")
	ErrLockAlreadyAcquired  = errors.New("lock already acquired")
	ErrFencingTokenMismatch = errors.New("fencing token mismatch")
	ErrOwnerMismatch        = errors.New("owner mismatch")
)
