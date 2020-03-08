package main

import (
	"log"
)

func init() {
	FlagParser.AddCommand(
		"merge-servers",
		"Merge new servers into the database.",
		`Reads configured server URLs. Servers that aren't present in the
		database are inserted.`,
		&CmdMergeServers{},
	)
}

type CmdMergeServers struct{}

func (cmd *CmdMergeServers) Execute(args []string) error {
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

	newServers, err := action.MergeServers(db, config.Servers)
	if err != nil {
		return err
	}

	log.Printf("merged %d new servers\n", newServers)
	return nil
}
