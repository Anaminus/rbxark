# rbxark
**rbxark** is a program for archiving Roblox build files.

rbxark operates on a database (e.g. `ark.db`). A database has an associated
config file, which either matches the name of the database (e.g. `ark.db.json`)
or is specified explicitly with a command line option. The
[config_sample.json](config_sample.json) file provides a sample configuration
file, with commentary.

Complete process for updating a database:

```bash
# Merge new servers from config.
rbxark merge-servers ark.db
# Merge new filenames from config.
rbxark merge-filenames ark.db
# Scan DeployHistory of servers for new builds.
rbxark fetch-builds ark.db
# Generate new combinations of build hashes and filenames.
rbxark generate-files ark.db
# Fetch just the headers of generated files.
rbxark fetch-headers ark.db
# Fetch the full content of generated files.
rbxark fetch-files ark.db
```

## Installation
rbxark depends on [go-sqlite3][go-sqlite3], which requires cgo and gcc. Check
`go env` to make sure `CGO_ENABLED` is set.

### Windows
On Windows, rbxark can be compiled with [MSYS2][MSYS2]. This guide assumes the
MINGW64 environment is used.

Make sure MSYS2 is up to date:
```bash
pacman -Syu
```

Install GCC and Go:
```bash
pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-go
```

Install rbxark:
```bash
go install github.com/anaminus/rbxark
```

[MSYS2]: https://www.msys2.org/
[go-sqlite3]: https://github.com/mattn/go-sqlite3
