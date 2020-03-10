package main

import (
	"log"

	"github.com/jessevdk/go-flags"
)

func init() {
	OptionTags{
		"workers": &flags.Option{
			Description: "The number of worker threads used when downloading files.",
			Default:     []string{"32"},
		},
		"recheck": &flags.Option{
			Description: "Include files with the Missing status.",
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
		`Scans for files with the Unchecked status and downloads their content
		to the configured objects path. A hit writes the file to the objects
		path, adds the response's headers to the database, and sets the file's
		status to Complete. A miss sets the file's status to Missing.

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

	fetcher := NewFetcher(nil, cmd.Workers, config.RateLimit)

	stats := Stats{}
	err = action.FetchContent(db, fetcher, config.ObjectsPath, query, cmd.Recheck, cmd.BatchSize, stats)
	log.Println(stats)
	return err
}
