# trie-fs

A Go library that implements an in-memory trie-based file system. It stores
files and directories in a compressed trie structure, providing path-based
operations like listing, lookup, and deletion. The trie is safe for concurrent
use.

## Install

```
go get github.com/kalambet/trie-fs
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	triefs "github.com/kalambet/trie-fs"
)

func main() {
	// Create a new trie.
	t := triefs.NewTrie()

	// Add files.
	now := time.Now()
	t.AddFile(triefs.NewEntry("/docs/readme.txt", "cid1", 128, triefs.MIMEOctetStream, now))
	t.AddFile(triefs.NewEntry("/docs/notes.txt", "cid2", 64, triefs.MIMEOctetStream, now))

	// List a directory.
	for _, c := range t.Ls("/docs") {
		fmt.Println(c.Name, c.Type)
	}

	// Look up a single file.
	content, err := t.File("/docs/readme.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(content.Name, content.Size)

	// Delete a file.
	t.Delete("/docs/notes.txt")
}
```

## License

MIT
