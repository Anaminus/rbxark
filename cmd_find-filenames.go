package main

import (
	"fmt"
	"log"
	"os"

	"github.com/anaminus/but"
	"github.com/anaminus/rbxark/objects"
	"github.com/anaminus/rbxark/pkgman"
)

func init() {
	FlagParser.AddCommand(
		"find-filenames",
		"Find file names from rbxPkgManifest files.",
		`Scans downloaded rbxPkgManifest files for file names that have not been
		added to the database. The results are printed, but are not added to the
		database.`,
		&CmdFindFilenames{},
	)
}

type CmdFindFilenames struct{}

func (cmd *CmdFindFilenames) Execute(args []string) error {
	db, cfgdir, err := OpenDatabase(args)
	if err != nil {
		return err
	}
	defer db.Close()

	config, err := LoadConfig(cfgdir)
	if err != nil {
		return err
	}
	if config.ObjectsPath == "" {
		return fmt.Errorf("unconfigured objects path")
	}

	action := Action{Context: Main}
	if err := action.Init(db); err != nil {
		return err
	}

	names, err := action.GetFilenames(db)
	if err != nil {
		return err
	}

	filenames := map[string]struct{}{}
	for _, name := range names {
		filenames[name] = struct{}{}
	}

	manifests, err := action.FindManifests(db)
	if err != nil {
		return err
	}

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

	return nil
}
