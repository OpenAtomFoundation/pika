// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package unsafe2

import (
	"testing"

	"pika/codis/v2/pkg/utils/assert"
)

func TestCastString(t *testing.T) {
	var b = []byte("hello")
	var s = CastString(b)
	b[0] = 'w'
	assert.Must(s == "wello")
}
