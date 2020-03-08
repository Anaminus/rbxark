package main

import (
	"log"
)

func init() {
	FlagParser.AddCommand(
		"merge-filenames",
		"Merge new file names into the database.",
		`Reads configured file names. Names that aren't present in the database
		are inserted.`,
		&CmdMergeFilenames{},
	)
}

type CmdMergeFilenames struct{}

func (cmd *CmdMergeFilenames) Execute(args []string) error {
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

	newFiles, err := action.MergeFiles(db, config.BuildFiles)
	if err != nil {
		return err
	}

	log.Printf("merged %d new files\n", newFiles)
	return nil
}
