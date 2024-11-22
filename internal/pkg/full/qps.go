package full

import "time"

type QpsLimit struct {
	Start  time.Time
	MaxQps uint
	hits   int
	curQps float64
	wait   time.Duration
}

func NewQpsLimit(maxQps uint) *QpsLimit {
	return &QpsLimit{
		MaxQps: maxQps,
	}
}

func (q *QpsLimit) Reset() {
	q.curQps = 0
	q.Start = time.Now()
}

func (q *QpsLimit) Incr(incr int) {

	// Increment the current QPS
	q.hits += incr

	// Compute the QPS from the start
	elapsed := time.Since(q.Start)
	q.curQps = float64(q.hits) / elapsed.Seconds()
	q.wait = time.Duration(float64(time.Microsecond) / q.curQps)
}

func (q *QpsLimit) Wait() {
	// Determine the time to wait
	if q.curQps > float64(q.MaxQps) {
		time.Sleep(q.wait)
	}
}
