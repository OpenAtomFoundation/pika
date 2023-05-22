package writer

import "github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"

type Writer interface {
	Write(entry *entry.Entry)
}
