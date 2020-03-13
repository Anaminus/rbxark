package main

import (
	"github.com/anaminus/rbxark/fetch"
	"github.com/jessevdk/go-flags"
)

func init() {
	OptionTags{
		"workers": &flags.Option{
			Description: "The number of worker threads used when downloading files.",
			Default:     []string{"32"},
		},
	}.AddTo(FlagParser.AddCommand(
		"fetch-builds",
		"Discover new builds from each server.",
		`Downloads and scans the DeployHistory file from each server in the
		database. Any found builds that are new are inserted into the
		database.`,
		&CmdFetchBuilds{},
	))
}

type CmdFetchBuilds struct {
	Workers int `long:"workers"`
}

func (cmd *CmdFetchBuilds) Execute(args []string) error {
	db, cfgdir, err := OpenDatabase(args)
	if err != nil {
		return err
	}
	defer db.Close()

	config, err := LoadConfig(cfgdir)
	if err != nil {
		return err
	}

	action := Action{Context: Main}
	if err := action.Init(db); err != nil {
		return err
	}

	fetcher := fetch.NewFetcher(nil, cmd.Workers, config.RateLimit)

	file := config.DeployHistory
	if file == "" {
		file = "DeployHistory.txt"
	}
	return action.FetchBuilds(db, fetcher, file)
}
