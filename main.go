package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/jessevdk/go-flags"
)

var Main, CancelMain = context.WithCancel(context.Background())

var FlagOptions struct {
	Config string `short:"c" long:"config" description:"Path to configuration file. Defaults to the database file path appended with '.json'."`
}
var FlagParser = flags.NewParser(&FlagOptions, flags.Default)

func init() {
	log.SetFlags(0)
}

// Gets a database path from a list of arguments and opens the database. Returns
// the database and the directory of the database.
func OpenDatabase(args []string) (db *sql.DB, dir string, err error) {
	if len(args) == 0 {
		return nil, "", fmt.Errorf("expected database file")
	}
	if db, err = sql.Open("sqlite3", args[0]); err != nil {
		return nil, "", err
	}
	return db, args[0] + ".json", nil
}

func LoadConfig(path string) (config *Config, err error) {
	if FlagOptions.Config != "" {
		path = FlagOptions.Config
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	config = &Config{}
	err = json.NewDecoder(f).Decode(config)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return config, nil
}

func MonitorSignals(cancel context.CancelFunc) {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		for {
			select {
			case <-sig:
				cancel()
				return
			}
		}
	}()
}

type OptionTags map[string]*flags.Option

func (tags OptionTags) AddTo(cmd *flags.Command, err error) (*flags.Command, error) {
	if cmd == nil {
		return cmd, err
	}
	for name, info := range tags {
		opt := cmd.FindOptionByLongName(name)
		if opt == nil {
			continue
		}
		opt.Description = info.Description
		opt.ValueName = info.ValueName
		opt.Default = info.Default
	}
	return cmd, err
}

func main() {
	MonitorSignals(CancelMain)
	FlagParser.Parse()
}
