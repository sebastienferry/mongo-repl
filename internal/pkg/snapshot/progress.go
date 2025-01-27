package snapshot

type SyncProgress struct {
	Database   string
	Collection string
	total      uint64
	processed  uint64
}

func NewSyncProgress(database string, collection string) *SyncProgress {
	return &SyncProgress{
		Database:   database,
		Collection: collection,
	}
}

func (f *SyncProgress) SetTotal(total uint64) {
	f.total = total
}

func (f *SyncProgress) Increment(incr int) {
	f.processed += uint64(incr)
}

func (f *SyncProgress) Progress() float64 {
	return float64(f.processed) / float64(f.total)
}
