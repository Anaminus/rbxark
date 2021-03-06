package main

import (
	"log"
)

func init() {
	FlagParser.AddCommand(
		"generate-files",
		"Generate combinations of possible files.",
		`Inserts into the database combinations of build hashes and file names
		that aren't already present.`,
		&CmdGenerateFiles{},
	)
}

type CmdGenerateFiles struct{}

func (cmd *CmdGenerateFiles) Execute(args []string) error {
	db, _, err := OpenDatabase(args)
	if err != nil {
		return err
	}
	defer db.Close()

	action := Action{Context: Main}
	if err := action.Init(db); err != nil {
		return err
	}

	newFiles, err := action.GenerateFiles(db)
	if err != nil {
		return err
	}

	log.Printf("merged %d new files\n", newFiles)
	return nil
}
