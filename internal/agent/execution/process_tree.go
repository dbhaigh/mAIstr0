package execution

import (
	"errors"
	"time"
)

var errProcessTreeUnsupported = errors.New("process-tree termination is unavailable")

const processTreeTerminationWait = 5 * time.Second

type processTree interface {
	Kill() error
	Close() error
}
