package main

import (
	"log"

	"github.com/anaminus/rbxark/fetch"
	"github.com/jessevdk/go-flags"
)

func init() {
	OptionTags{
		"workers": &flags.Option{
			Description: "The number of worker threads used when downloading files.",
			Default:     []string{"32"},
		},
		"recheck": &flags.Option{
			Description: "Include files with the NotFound flag.",
		},
		"rate-limit": &flags.Option{
			Description: "Allowed requests per second. A negative value means unlimited.",
			Default:     []string{"-1"},
		},
		"batch-size": &flags.Option{
			ShortName:   'b',
			Description: "Number of files to fetch before committing them to the database",
			Default:     []string{"64"},
		},
	}.AddTo(FlagParser.AddCommand(
		"fetch-files",
		"Download content of unchecked files.",
		`Scans for Unchecked files and downloads their content to the configured
		objects path. A hit writes the file to the objects path, and adds the
		response's headers to the database. A miss sets the NotFound flag.

		Prints the aggregation of each response status code.`,
		&CmdFetchFiles{},
	))
}

type CmdFetchFiles struct {
	Workers   int  `long:"workers"`
	Recheck   bool `long:"recheck"`
	BatchSize int  `long:"batch-size"`
}

func (cmd *CmdFetchFiles) Execute(args []string) error {
	db, cfgdir, err := OpenDatabase(args)
	if err != nil {
		return err
	}
	defer db.Close()

	config, err := LoadConfig(cfgdir)
	if err != nil {
		return err
	}

	query, err := LoadFilter(config.Filters, "files")
	if err != nil {
		return err
	}

	action := Action{Context: Main}
	if err := action.Init(db); err != nil {
		return err
	}

	fetcher := fetch.NewFetcher(nil, cmd.Workers, config.RateLimit)

	stats := Stats{}
	err = action.FetchContent(db, fetcher, config.ObjectsPath, query, cmd.Recheck, cmd.BatchSize, stats)
	log.Println(stats)
	return err
}
