package domain

import "errors"

var (
	ErrFailedToJoinNode = errors.New("failed to join node")

	ErrLockNotFound         = errors.New("not found")
	ErrLockExpired          = errors.New("lock expired")
	ErrLockAlreadyAcquired  = errors.New("lock already acquired")
	ErrFencingTokenMismatch = errors.New("fencing token mismatch")
	ErrOwnerMismatch        = errors.New("owner mismatch")
)
