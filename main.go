package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/anaminus/but"
	"github.com/jessevdk/go-flags"
)

var Main, CancelMain = context.WithCancel(context.Background())

var FlagOptions struct{}
var FlagParser = flags.NewParser(&FlagOptions, flags.Default)

func init() {
	log.SetFlags(0)
}

func main() {
	// _, err := FlagParser.Parse()
	// if err != nil {
	// 	return
	// }

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		for {
			select {
			case <-sig:
				CancelMain()
				return
			}
		}
	}()

	config := &Config{}
	{
		f, err := os.Open("config.json")
		if err == nil {
			but.IfFatal(json.NewDecoder(f).Decode(config), "read config")
			f.Close()
		}
	}

	db, err := sql.Open("sqlite3", "test.db")
	but.IfFatal(err, "open database")

	action := Action{Context: Main}
	but.IfFatal(action.Init(db), "initialize database")

	fetcher := NewFetcher(nil, 32, config.RateLimit)

	if len(os.Args) < 2 {
		return
	}
	switch os.Args[1] {
	case "merge-files":
		newFiles, err := action.MergeFiles(db, config.BuildFiles)
		but.IfFatal(err, "merge files")
		log.Printf("merged %d new files\n", newFiles)
	case "merge-servers":
		newServers, err := action.MergeServers(db, config.Servers)
		but.IfFatal(err, "merge servers")
		log.Printf("merged %d new servers\n", newServers)
	case "fetch-builds":
		file := config.DeployHistory
		if file == "" {
			file = "DeployHistory.txt"
		}
		but.IfFatal(action.FetchBuilds(db, fetcher, file), "fetch builds")
	case "generate-files":
		newFiles, err := action.GenerateFiles(db)
		but.IfFatal(err, "generate files")
		log.Printf("generated %d new files\n", newFiles)
	case "fetch-headers":
		but.IfFatal(action.FetchContent(db, fetcher, "", false, 4096), "fetch headers")
	case "fetch-files":
		but.IfFatal(action.FetchContent(db, fetcher, config.ObjectsPath, false, 256), "fetch files")
	default:
		but.Fatalf("unknown command %q", os.Args[1])
	}
}
