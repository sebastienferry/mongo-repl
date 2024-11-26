package incr

import "context"

type Writer interface {
	StartWriter(context.Context)
	StopWriter(context.Context)
}
