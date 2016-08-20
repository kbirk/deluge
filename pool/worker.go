package pool

// Worker represents a designated worker function to batch in a pool.
type Worker func(interface{}) (uint64, error)
