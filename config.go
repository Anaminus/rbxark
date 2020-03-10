package main

type Config struct {
	// Location of object files.
	ObjectsPath string `json:"objects_path"`
	// File on server from which builds are scanned.
	DeployHistory string `json:"deploy_history"`
	// Allowed requests per second.
	RateLimit float64 `json:"rate_limit"`
	// List of deployment servers.
	Servers []string `json:"servers"`
	// List of files on server that have a constant location.
	DeployFiles []string `json:"deploy_files"`
	// List of potential files per version hash.
	BuildFiles []string `json:"build_files"`
	// List of filters to apply when selecting files.
	Filters []string `json:"filters"`
}
