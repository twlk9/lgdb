package wal

type walSyncQueue struct {
	buf  []*SyncRequest
	head int
}

func (q *walSyncQueue) put(r *SyncRequest) {
	q.buf = append(q.buf, r)
}

func (q *walSyncQueue) get() (*SyncRequest, bool) {
	if q.head >= len(q.buf) {
		return &SyncRequest{}, false
	}
	r := q.buf[q.head]
	q.head++

	// Reset when head gets large to avoid GC leaks and slice growth
	if q.head > 1024 && q.head*2 > len(q.buf) {
		copy(q.buf, q.buf[q.head:])
		q.buf = q.buf[:len(q.buf)-q.head]
		q.head = 0
	}
	return r, true
}

func (q *walSyncQueue) len() int {
	return len(q.buf) - q.head
}
