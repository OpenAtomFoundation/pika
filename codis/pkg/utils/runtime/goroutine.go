/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gxruntime

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

// GoSafely wraps a `go func()` with recover()
func GoSafely(wg *sync.WaitGroup, ignoreRecover bool, handler func(), catchFunc func(r interface{})) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if !ignoreRecover {
					fmt.Fprintf(os.Stderr, "%s goroutine panic: %v\n%s\n",
						time.Now(), r, string(debug.Stack()))
				}
				if catchFunc != nil {
					if wg != nil {
						wg.Add(1)
					}
					go func() {
						defer func() {
							if p := recover(); p != nil {
								if !ignoreRecover {
									fmt.Fprintf(os.Stderr, "recover goroutine panic:%v\n%s\n",
										p, string(debug.Stack()))
								}
							}

							if wg != nil {
								wg.Done()
							}
						}()
						catchFunc(r)
					}()
				}
			}
			if wg != nil {
				wg.Done()
			}
		}()
		handler()
	}()
}

// GoUnterminated is used for which goroutine wanna long live as its process.
// @period: sleep time duration after panic to defeat @handle panic so frequently. if it is not positive,
//
//	the @handle will be invoked asap after panic.
func GoUnterminated(handle func(), wg *sync.WaitGroup, ignoreRecover bool, period time.Duration) {
	GoSafely(wg,
		ignoreRecover,
		handle,
		func(r interface{}) {
			if period > 0 {
				time.Sleep(period)
			}
			GoUnterminated(handle, wg, ignoreRecover, period)
		},
	)
}
