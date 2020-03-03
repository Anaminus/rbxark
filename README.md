# rbxark
**rbxark** is a program for archiving Roblox build files.

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
