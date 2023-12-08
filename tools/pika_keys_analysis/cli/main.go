package main

import (
	"fmt"
	"os"
	"pika/tools/pika_keys_analysis"

	"github.com/desertbit/grumble"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: pika_keys_analysis <config file>")
		os.Exit(1)
	}
	err := pika_keys_analysis.Init(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Args = os.Args[0:1]
	grumble.Main(pika_keys_analysis.App)
}
