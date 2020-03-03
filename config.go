package main

type Config struct {
	// Location of object files.
	ObjectsPath string `json:"objects_path"`
	// File on server from which builds are scanned.
	DeployHistory string `json:"deploy_history"`
	// Throttle HTTP requests.
	RequestsPerMinute int `json:"requests_per_minute"`
	// List of deployment servers.
	Servers []string `json:"servers"`
	// List of files on server that have a constant location.
	DeployFiles []string `json:"deploy_files"`
	// List of potential files per version hash.
	BuildFiles []string `json:"build_files"`
}

func (c *Config) GetDeployHistory() string {
	if c.DeployHistory == "" {
		return "DeployHistory.txt"
	}
	return c.DeployHistory
}
