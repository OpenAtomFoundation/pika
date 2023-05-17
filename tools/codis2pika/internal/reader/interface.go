package reader

import "github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"

type Reader interface {
	StartRead() chan *entry.Entry
}
