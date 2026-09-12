package router

// PermanentError marks a failure that retrying can never fix: a contract
// violation or an event for a caller nobody registered. The consumer
// dead-letters these and moves on; everything else is treated as transient
// and left uncommitted for redelivery.
type PermanentError struct {
	Reason string
}

func (e *PermanentError) Error() string { return e.Reason }
