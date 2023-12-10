package pika_keys_analysis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"
)

var App = grumble.New(&grumble.Config{
	Name:                  "pika_keys_analysis",
	Description:           "A tool for analyzing keys in Pika",
	HistoryFile:           "/tmp/.pika_keys_analysis_history",
	Prompt:                "pika_keys_analysis > ",
	HistoryLimit:          100,
	ErrorColor:            color.New(color.FgRed, color.Bold, color.Faint),
	HelpHeadlineColor:     color.New(color.FgGreen),
	HelpHeadlineUnderline: false,
	HelpSubCommands:       true,
	PromptColor:           color.New(color.FgBlue, color.Bold),
	Flags:                 func(f *grumble.Flags) {},
})

func init() {
	App.OnInit(func(a *grumble.App, fm grumble.FlagMap) error {
		return nil
	})
	App.SetPrintASCIILogo(func(a *grumble.App) {
		fmt.Println(strings.Join([]string{`
   .............          ....     .....       .....           .....         
   #################      ####     #####      #####           #######        
   ####         #####     ####     #####    #####            #########       
   ####          #####    ####     #####  #####             ####  #####      
   ####         #####     ####     ##### #####             ####    #####     
   ################       ####     ##### #####            ####      #####    
   ####                   ####     #####   #####         #################   
   ####                   ####     #####    ######      #####         #####  
   ####                   ####     #####      ######   #####           ##### 
`}, "\r\n"))
	})
	register(App)
}

func register(app *grumble.App) {
	app.AddCommand(&grumble.Command{
		Name:     "bigKey",
		Help:     "list the big keys",
		LongHelp: "list the big keys",
		Run: func(c *grumble.Context) error {
			listBigKeys, err := PikaInstance.ListBigKeysByScan(context.Background())
			if err != nil {
				return err
			}
			start := time.Now()
			for keyType, data := range listBigKeys {
				fmt.Printf("Type: %s, Head: %d\n", keyType, Head)
				if len(data.GetTopN(Head)) == 0 {
					fmt.Println("No big key found")
				}
				for _, v := range data.GetTopN(Head) {
					fmt.Printf("Key : %s, Size: %d, From: %s\n", v.Key, v.UsedSize, v.Client)
				}
			}
			end := time.Now()
			if PrintKeyNum {
				fmt.Println("Total Key Number:", PikaInstance.GetTotalKeyNumber())
			}
			fmt.Println("Cost Time:", end.Sub(start))
			return nil
		},
	})

	app.AddCommand(&grumble.Command{
		Name:     "apply",
		Help:     "Apply the settings to Pika",
		LongHelp: "Apply the settings to Pika",
		Args: func(a *grumble.Args) {
			a.String("filename", "The configuration file")
		},
		Run: func(c *grumble.Context) error {
			filename := c.Args.String("filename")
			return Init(filename)
		},
	})

	app.AddCommand(&grumble.Command{
		Name:     "compress",
		Help:     "Compress the big keys",
		LongHelp: "Compress the big keys and store them to pika",
		Args: func(a *grumble.Args) {
			a.String("key", "The key to compress")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			return PikaInstance.CompressKey(context.Background(), key)
		},
	})

	app.AddCommand(&grumble.Command{
		Name:     "decompress",
		Help:     "Decompress the big keys",
		LongHelp: "Decompress the big keys and store them to pika",
		Args: func(a *grumble.Args) {
			a.String("key", "The key to decompress")
		},
		Flags: func(f *grumble.Flags) {
			f.Bool("s", "save", false, "Save the decompressed value to pika")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			save := c.Flags.Bool("save")
			decompressKey, err := PikaInstance.DecompressKey(context.Background(), key, save)
			if err != nil {
				return err
			}
			fmt.Printf("Key: %s, Decompress: %s\n", key, decompressKey)
			return nil
		},
	})

	app.AddCommand(&grumble.Command{
		Name:     "recover",
		Help:     "Recover the big keys",
		LongHelp: "Recover the big keys and store them to pika",
		Args: func(a *grumble.Args) {
			a.String("key", "The key to recover")
			a.String("newKey", "The new key to store the recovered value")
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			newKey := c.Args.String("newKey")
			return PikaInstance.RecoverKey(context.Background(), key, newKey)
		},
	})
}
