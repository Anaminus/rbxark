package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/anaminus/but"
	"github.com/anaminus/rbxark/objects"
	"github.com/anaminus/rbxark/pkgman"
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

	db, err := sql.Open("sqlite3", "ark.db")
	but.IfFatal(err, "open database")

	action := Action{Context: Main}
	but.IfFatal(action.Init(db), "initialize database")

	fetcher := NewFetcher(nil, 32, config.RateLimit)

	if len(os.Args) < 2 {
		return
	}
	switch os.Args[1] {
	case "merge-filenames":
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
		stats := Stats{}
		err := action.FetchContent(db, fetcher, "", false, 4096, stats)
		but.IfError(err, "fetch headers")
		but.Log(stats.String())
	case "fetch-files":
		stats := Stats{}
		err := action.FetchContent(db, fetcher, config.ObjectsPath, false, 64, stats)
		but.IfError(err, "fetch files")
		but.Fatal(stats.String())
	case "find-filenames":
		if config.ObjectsPath == "" {
			but.Fatal("unspecified objects path")
		}

		names, err := action.GetFilenames(db)
		but.IfFatal(err, "get filenames")

		filenames := map[string]struct{}{}
		for _, name := range names {
			filenames[name] = struct{}{}
		}

		manifests, err := action.FindManifests(db)
		but.IfFatal(err, "find manifests")

		for _, hash := range manifests {
			path := objects.Path(config.ObjectsPath, hash)
			if path == "" {
				but.IfError(fmt.Errorf("%s: file does not exist", hash))
				continue
			}
			man, err := os.Open(path)
			if err != nil {
				but.IfError(fmt.Errorf("%s: %w", hash, err))
				continue
			}
			entries, err := pkgman.Decode(man)
			if err != nil {
				but.IfError(fmt.Errorf("%s: %w", hash, err))
				continue
			}
			for _, entry := range entries {
				if _, ok := filenames[entry.Name]; ok {
					continue
				}
				log.Println(entry.Name)
				filenames[entry.Name] = struct{}{}
			}
		}

	default:
		but.Fatalf("unknown command %q", os.Args[1])
	}
}
