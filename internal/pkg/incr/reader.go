package incr

import "context"

type Reader interface {
	StartReader(context.Context)
	StopReader(context.Context)
}
