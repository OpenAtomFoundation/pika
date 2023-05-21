package commands

type keySpec struct {
	// begin_search
	beginSearchType string
	// @index
	beginSearchIndex int
	// @keyword
	beginSearchKeyword   string
	beginSearchStartFrom int

	// find_keys
	findKeysType string
	// @range
	findKeysRangeLastKey int
	findKeysRangeKeyStep int
	findKeysRangeLimit   int
	// @keynum
	findKeysKeynumIndex    int
	findKeysKeynumFirstKey int
	findKeysKeynumKeyStep  int
}

type redisCommand struct {
	group   string
	keySpec []keySpec
}
