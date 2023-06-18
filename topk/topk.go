package topk

// @Author KHighness
// @Update 2023-06-18

// TopK item.
type Item struct {
	Key   string
	Count uint32
}

// TopK algorithm interface.
type TopK interface {

	// Add adds an item to the list of top k.
	// It returns two values:
	//	- The first return value represents if the item had been added successfully.
	//	- The second return value is the expelled item if any item was expelled.
	Add(item string, incr uint32) (string, bool)

	// Lists returns all the items in the top k.
	List() []Item

	// Total returns the total count of the items.
	Total() uint64

	// Expelled watches at the expelled items.
	Expelled() <-chan Item

	// Halve reduces count for the specified scene.
	Fading()
}
