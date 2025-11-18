package memtable

// RefMemTableList takes the active memtable and list of current
// immutable memtables and returns a combined list after incrementing
// their references
func RefMemTableList(mt *MemTable, immutables []*MemTable) []*MemTable {
	mems := make([]*MemTable, 0, len(immutables)+1)
	mems = append(mems, mt)
	mems = append(mems, immutables...)

	for _, m := range mems {
		m.Ref()
	}
	return mems
}

// UnRefMemTableList decrements the references and that's it. Just a
// helper function
func UnRefMemTableList(mems []*MemTable) {
	for _, m := range mems {
		m.UnRef()
	}
}
