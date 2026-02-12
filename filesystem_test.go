package filesystem_test

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"imploy/files/filesystem"
	"imploy/lib/env"
	"imploy/lib/zlog"

	fuzz "github.com/google/gofuzz"
	"gotest.tools/assert"
)

func init() {
	zlog.NewSilentLogger()
}

func createRandomFiles(trie *filesystem.Trie, n int) []string {
	f := fuzz.New()
	fileTypes := []string{filesystem.MIMEDriveEntry, filesystem.MIMEOctetStream}
	var paths []string
	createdAt := time.Now()
	for j := 0; j < n; j++ {
		var path string
		var cid string
		var size int64
		var fileTypeIndex int
		f.Fuzz(&path)
		f.Fuzz(&cid)
		f.Fuzz(&size)

		paths = append(paths, path)

		// generate filetype randomly
		index := fileTypeIndex % (len(fileTypes))
		fileType := fileTypes[int64(math.Abs(float64(index)))]
		e := filesystem.NewEntry(path, cid, size, fileType, createdAt)
		_, _ = trie.AddFile(e)
	}
	return paths
}

func TestHash(t *testing.T) {
	for i := 1; i <= 100; i++ {
		trie := filesystem.NewTrie()
		ehash, err := trie.Hash()
		if err != nil {
			t.Errorf("Got an err %v", err)
		}

		createRandomFiles(trie, i)
		// check if hash matches
		hash1, err := trie.Hash()
		if err != nil {
			t.Errorf("Got an err %v", err)
		}

		if ehash == hash1 {
			t.Errorf("hashes should be different")
		}

		hash2, err := trie.Hash()
		if err != nil {
			t.Errorf("Got an err %v", err)
		}

		if hash1 != hash2 {
			t.Errorf("hashes should be same")
		}

		// Add more files
		createRandomFiles(trie, 10)
		hash3, err := trie.Hash()
		if err != nil {
			t.Errorf("Got an err %v", err)
		}

		if hash1 == hash3 {
			t.Errorf("hashes should be different")
		}
	}
}

func TestAddFile(t *testing.T) {
	t.Parallel()

	now := time.Now()

	cases := []struct {
		name  string
		dirs  []*filesystem.Entry
		added [][]*filesystem.Entry
		root  *filesystem.Entry
		err   error
	}{
		{
			name: "add a nil",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{nil},
		},
		{
			name: "add a directory",
			err:  filesystem.ErrCantAddDirectory,
			dirs: []*filesystem.Entry{filesystem.NewEntry("/new_dir", "", 0, filesystem.MIMEDriveDirectory, now)},
		},
		{
			name: "add empty path file",
			err:  filesystem.ErrEmptyPath,
			dirs: []*filesystem.Entry{filesystem.NewEntry("", "test_cid", 512, filesystem.MIMEOctetStream, now)},
		},
		{
			name: "add illegal directory",
			err:  filesystem.ErrIllegalPathChars,
			dirs: []*filesystem.Entry{filesystem.NewEntry("/some:dir", "", 0, filesystem.MIMEDriveEntry, now)},
		},
		{
			name: "add root directory",
			err:  filesystem.ErrEmptyName,
			dirs: []*filesystem.Entry{filesystem.NewEntry("/", "", 0, filesystem.MIMEDriveEntry, now)},
		},
		{
			name: "add single directory",
			err:  nil,
			dirs: []*filesystem.Entry{filesystem.NewEntry("/folder1", "", 0, filesystem.MIMEDriveEntry, now)},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1",
				Content: filesystem.Content{
					Name:      "",
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "basic directory first",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, "", now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/file",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "file",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "directory already exist",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/file",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "file",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "conflict add",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "file",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add file that is also sub-path",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "f",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "add dir that is also sub-path",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/f", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("f", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "divergence in first level",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aba/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aba"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aba/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("aca", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aca"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aca/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aca/file/file"},
				},
			},
			root: &filesystem.Entry{
				Path: "/a",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "aa/bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "ba/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "ca/file/file",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "file",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add file that is also sub-path, new order",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/f"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: "ile/file",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "f",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "add dir that is also sub-path, new order",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/f", "", 0, filesystem.MIMEDriveEntry, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("f", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/f"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple files in dir",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file/test", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa"},
					&filesystem.Entry{Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&filesystem.Entry{Content: filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&filesystem.Entry{Content: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("test", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/aaa/file/test"},
				},
			},
			root: &filesystem.Entry{
				Path: "/aaa/",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "bbb/f",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: "file",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "test",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "test",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "add files with appending same last char -  Issue #2630",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile1", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile11", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile111", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile1", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile1"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile11", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile111", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile111"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile1",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "1",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "myfile11",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "1",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "myfile111",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "add dir with appending same last char",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/folder1/folder2/myfile11", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/folder1/folder2/myfile111", "", 0, filesystem.MIMEDriveEntry, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile1"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile11", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile111", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile111"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "1",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "1",
								Content: filesystem.Content{
									Type:      filesystem.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
								Entries: []*filesystem.Entry{
									{
										Path: ":",
										Content: filesystem.Content{
											Type:      filesystem.MIMEDriveEntry,
											CreatedAt: now.Unix(),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "add files with cutting same last char",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile111", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile11", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile1", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile111", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile111"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile11", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile1", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile1"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "1",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*filesystem.Entry{
							{
								Path: "1",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "myfile111",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: ":",
								Content: filesystem.Content{
									Type:      filesystem.MIMEOctetStream,
									Name:      "myfile11",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
						},
					},
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile1",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add files in a reverse order",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile2", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile2"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "3",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile3",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "2",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile2",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add conflicting fields",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: "3",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile3",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add conflicting dirs",
			err:  filesystem.ErrConflict,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/folder1/folder2/myfile", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/folder1/folder2/myfile", "", 0, filesystem.MIMEDriveEntry, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1"},
					&filesystem.Entry{Content: filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&filesystem.Entry{Content: filesystem.NewContent("myfile3", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&filesystem.Entry{Content: filesystem.NewContent("myfile", "", 0, filesystem.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &filesystem.Entry{
				Path: "/folder1/folder2/myfile",
				Content: filesystem.Content{
					Type:      filesystem.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*filesystem.Entry{
					{
						Path: ":",
						Content: filesystem.Content{
							Type:      filesystem.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "3",
						Content: filesystem.Content{
							Type:      filesystem.MIMEOctetStream,
							Name:      "myfile3",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "add files with sharp in name",
			err:  nil,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/mambo #5", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			added: [][]*filesystem.Entry{
				{
					&filesystem.Entry{Content: filesystem.NewContent("mambo #5", "test_cid", 512, filesystem.MIMEOctetStream, now), Path: "/mambo #5"},
				},
			},
			root: &filesystem.Entry{
				Path: "/mambo #5",
				Content: filesystem.Content{
					Type:      filesystem.MIMEOctetStream,
					Name:      "mambo #5",
					Size:      512,
					CID:       "test_cid",
					Version:   1,
					CreatedAt: now.Unix(),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for i, e := range tc.dirs {
				entries, err := trie.AddFile(e)
				if err != nil {
					assert.Equal(t, err, tc.err)
				} else {
					assert.DeepEqual(t, tc.added[i], entries)
				}
			}
			assert.DeepEqual(t, trie.Root, tc.root)
		})
	}
}

func TestFuzzyAddFile(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()
		createRandomFiles(trie, 20)
	}
}

func TestLs(t *testing.T) {
	t.Parallel()
	now := time.Now()

	cases := []struct {
		name    string
		path    string
		dirs    []*filesystem.Entry
		content []filesystem.Content
		err     error
	}{
		{
			name:    "ls on nil",
			err:     nil,
			path:    "/",
			content: []filesystem.Content{},
		},
		{
			name: "simple list 1",
			err:  nil,
			path: "/folder1/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{},
		},
		{
			name: "ls root simple",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls with semicolon",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "ls from a slash child",
			err:  nil,
			path: "/priom",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/priom", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/priom/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "ls from a similar child",
			err:  nil,
			path: "/priom",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/priompriom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/priom", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/priom/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aba/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aca/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aca", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aba/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aca/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aca", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs 2",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/folder1", "test_cid", 512, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/folder2", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/folder3", "test_cid", 512, filesystem.MIMEDriveEntry, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("folder3", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed 2",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/fiee", "", 0, filesystem.MIMEDriveEntry, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed 3",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/test", "", 0, filesystem.MIMEDriveEntry, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls file",
			err:  nil,
			path: "/aaa/fbb/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{},
		},
		{
			name: "ls entry (as a trie node)",
			err:  nil,
			path: "/aaa/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{},
		},
		{
			name: "ls non existent entry",
			err:  nil,
			path: "/aaa/test",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{},
		},
		{
			name: "ls second layer",
			err:  nil,
			path: "/aaa/fbb",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			content: []filesystem.Content{
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "ls reference entry",
			err:  nil,
			path: "/aaa/fbb/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 0, filesystem.MIMEReference, now),
			},
			content: []filesystem.Content{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt := trie.Ls(tc.path)

			assert.Equal(t, len(cnt), len(tc.content))
			for i, c := range tc.content {
				assert.DeepEqual(t, c, *cnt[i])
			}
		})
	}
}

func TestFuzzyLs(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()

	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_ = trie.Ls(path)
		} else {
			// randomly generate path to be deleted
			var path string
			f.Fuzz(&path)
			_ = trie.Ls(path)
		}
	}
}

func TestLsRecursive(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name   string
		path   string
		dirs   []*filesystem.Entry
		rdirs  []filesystem.Content
		rpaths []string
		err    error
	}{
		{
			name:   "ls on nil",
			err:    nil,
			path:   "/",
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "ls on file",
			err:  nil,
			path: "/folder/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "simple list 1",
			err:  nil,
			path: "/folder1/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder1/folder2/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "ls root simple",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/aaa",
				"/aaa/f",
			},
		},
		{
			name: "ls with semicolon",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/file",
				"/file1",
			},
		},
		{
			name: "ls with no slash at the path",
			err:  nil,
			path: "aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("file1", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/file",
				"/file1",
			},
		},
		{
			name: "ls from a slash child",
			err:  nil,
			path: "/priom",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/priom", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/priom/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/priom.txt",
			},
		},
		{
			name: "ls from a similar child",
			err:  nil,
			path: "/priom",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/priompriom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/priom", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/priom/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/priom.txt",
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aba/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aca/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aca", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/aaa",
				"/aaa/bbb",
				"/aaa/bbb/f",
				"/aba",
				"/aba/file",
				"/aca",
				"/aca/file",
				"/aca/file/file",
			},
		},
		{
			name: "ls first level dirs",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
			},
			rpaths: []string{
				"/fbb",
				"/fbb/f",
				"/fiee",
				"/fiee/file",
				"/file",
			},
		},
		{
			name: "ls first level mixed",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/fbb",
				"/fbb/f",
				"/fiee",
				"/fiee/file",
				"/fieeolder_emtpty",
				"/file",
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aba/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aca/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("aca", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/aaa",
				"/aaa/bbb",
				"/aaa/bbb/f",
				"/aba",
				"/aba/file",
				"/aca",
				"/aca/file",
				"/aca/file/file",
			},
		},
		{
			name: "ls first level mixed 2",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/fiee", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/fbb",
				"/fbb/f",
				"/fiee",
				"/fiee/fiee",
				"/fiee/file",
				"/fieeolder_emtpty",
				"/file",
			},
		},
		{
			name: "ls first level mixed 3",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/test", "", 0, filesystem.MIMEDriveEntry, now),
			},

			rdirs: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("test", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/fbb",
				"/fbb/f",
				"/fiee",
				"/fiee/file",
				"/fiee/test",
				"/fieeolder_emtpty",
				"/file",
			},
		},
		{
			name: "ls file",
			err:  nil,
			path: "/aaa/fbb/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "ls entry (as a trie node)",
			err:  nil,
			path: "/aaa/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "ls non existent entry",
			err:  nil,
			path: "/aaa/test",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs:  []filesystem.Content{},
			rpaths: []string{},
		},
		{
			name: "ls second layer",
			err:  nil,
			path: "/aaa/fbb",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []filesystem.Content{
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rpaths: []string{"/f"},
		},
		{
			name: "ls on reference entry",
			err:  nil,
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fieeolder_emtpty", "", 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/fiee/test", "", 0, filesystem.MIMEDriveEntry, now),
			},

			rdirs: []filesystem.Content{
				filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("fiee", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewContent("test", "", 0, filesystem.MIMEDriveDirectory, now),
				filesystem.NewContent("fieeolder_emtpty", "", 0, filesystem.MIMEReference, now),
				filesystem.NewContent("file", "test_cid", 0, filesystem.MIMEReference, now),
			},
			rpaths: []string{
				"/fbb",
				"/fbb/f",
				"/fiee",
				"/fiee/file",
				"/fiee/test",
				"/fieeolder_emtpty",
				"/file",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			entries := trie.LsRecursive(tc.path)

			assert.Equal(t, len(entries), len(tc.rdirs))
			for i, c := range tc.rdirs {
				assert.DeepEqual(t, c, entries[i].Content)
				assert.Equal(t, tc.rpaths[i], entries[i].Path)
			}
		})
	}
}

func TestFuzzyLsRecursive(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()

	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_ = trie.LsRecursive(path)
		} else {
			// randomly generate path to be deleted
			var path string
			f.Fuzz(&path)
			_ = trie.LsRecursive(path)
		}
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name  string
		path  string
		dirs  []*filesystem.Entry
		rdirs []*filesystem.Entry
		err   error
	}{
		{
			name:  "delete empty",
			path:  "",
			rdirs: []*filesystem.Entry{},
			err:   filesystem.ErrEmptyPath,
		},
		{
			name:  "delete crash test",
			path:  "/aaa/bbb/file",
			dirs:  []*filesystem.Entry{},
			rdirs: []*filesystem.Entry{},
		},
		{
			name: "simple delete file",
			path: "/aaa/bbb/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
		{
			name: "delete first level file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete root",
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level file",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level dir",
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete first level dir",
			path: "/aaa/fbb",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete trie node",
			path: "/aaa/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level file",
			path: "/aca",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete entry with semicolon",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "delete empty dir",
			path: "/aaa/dir1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir2", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir2", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
		{
			name: "issue 735",
			path: "/folder/f1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder/f1", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
		{
			name: "issue 735 regression after fix",
			path: "/folder/f1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/folder/f1", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			rtrie := filesystem.NewTrie()
			for _, d := range tc.rdirs {
				_, err := rtrie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			err := trie.Delete(tc.path)
			if err != nil {
				assert.Equal(t, err, tc.err)
			}

			assert.DeepEqual(t, trie.Root, rtrie.Root)
		})
	}
}

func TestFuzzyDeleteFile(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()

	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_ = trie.Delete(path)
		} else {
			// randomly generate path to be deleted
			var path string
			f.Fuzz(&path)
			_ = trie.Delete(path)
		}
	}
}

func TestFile(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name string
		path string
		dirs []*filesystem.Entry
		file filesystem.Content
		err  error
	}{
		{
			name: "empty test",
			path: "",
			err:  filesystem.ErrEmptyPath,
		},
		{
			name: "crash test",
			path: "/aaa/bbb/file",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{},
			file: filesystem.Content{},
		},
		{
			name: "simple get file",
			path: "/aaa/bbb/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get first level file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get root",
			path: "/",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get top level file",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get top level dir",
			path: "/aaa",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get first level dir",
			path: "/aaa/fbb",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get trie node",
			path: "/aaa/f",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get top level file",
			path: "/aca",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/dir",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir2", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("dir", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir",
			path: "/aaa/fdir1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fdir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fdir1", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fdir12", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir2", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fdir12", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get reference entry",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 0, filesystem.MIMEReference, now),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt, err := trie.File(tc.path)
			if err != nil {
				assert.Equal(t, err, tc.err)
			}

			if len(tc.file.Name) != 0 && cnt == nil {
				t.Fatalf("nil returned instaen of %#v", tc.file)
			}

			if cnt != nil {
				assert.DeepEqual(t, tc.file, *cnt)
			}
		})
	}
}

func TestFuzzyFile(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()

	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_, _ = trie.File(path)
		} else {
			// randomly generate path to get the metadata
			var path string
			f.Fuzz(&path)
			_, _ = trie.File(path)
		}
	}
}

func TestStat(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name string
		path string
		dirs []*filesystem.Entry
		file filesystem.Content
		err  error
	}{
		{
			name: "empty test",
			path: "",
			err:  filesystem.ErrEmptyPath,
		},
		{
			name: "crash test",
			path: "/aaa/bbb/file",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{},
			file: filesystem.Content{},
		},
		{
			name: "simple get file",
			path: "/aaa/bbb/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get first level file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get root",
			path: "/",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get top level file",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get top level dir",
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get first level dir",
			path: "/aaa/fbb",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fbb", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get trie node",
			path: "/aaa/f",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "get top level file",
			path: "/aca",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file - empty directory",
			path: "/aaa/dir",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir2", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("dir", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/dir",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir2/file2", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir/file2", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("dir", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir",
			path: "/aaa/fdir1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fdir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fdir1", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fdir12", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir2", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fdir12", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get info on non empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fdir12/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("fdir12", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "get reference entry",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("file", "test_cid", 0, filesystem.MIMEReference, now),
		},
		{
			name: "complex scenario #1",
			path: "/a",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "complex scenario #2",
			path: "/abca",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "complex scenario #3",
			path: "/abcab",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("abcab", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #4",
			path: "/akcab1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/akcab/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("akcab1", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #5",
			path: "/akcab",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/akcab/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			file: filesystem.NewContent("akcab", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #6",
			path: "/adcac/fdir",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/adcac/fdir3/file", "", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "complex scenario #7",
			path: "/adcac/fdir12",
			err:  filesystem.ErrFileNotExist,
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/adcac/fdir3/file", "", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.Content{},
		},
		{
			name: "complex scenario #8",
			path: "/adcac/fdir3",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/adcac/fdir3/file", "", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("fdir3", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #9",
			path: "/adcac/fdir2/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/abcab/folder1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/adcac/fdir2/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/afcad/fdir1/file", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/akcab1/file/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/adcac/fdir3/file", "", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("file", "", 0, filesystem.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #10",
			path: "/a/b/c",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/a/b/c/d/e", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/a/b/c/f/g", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/a/b/f/g/e", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/a/b/f/k/g", "", 0, filesystem.MIMEDriveEntry, now),
			},
			file: filesystem.NewContent("c", "", 0, filesystem.MIMEDriveDirectory, now),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt, err := trie.Stat(tc.path)
			if tc.err != nil {
				assert.Equal(t, err, tc.err)
			}
			if len(tc.file.Name) != 0 && cnt == nil {
				t.Fatalf("nil returned instaed of %#v", tc.file)
			}

			if cnt != nil {
				assert.DeepEqual(t, tc.file, *cnt)
			}
		})
	}
}

func TestFuzzyStat(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()

	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_, _ = trie.Stat(path)
		} else {
			// randomly generate path to get the metadata
			var path string
			f.Fuzz(&path)
			_, _ = trie.Stat(path)
		}
	}
}

func TestRoutine(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()
	dirs := []*filesystem.Entry{
		filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder/folder", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/folder/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder1", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder1/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder/folder1", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/folder1/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
	}

	rtrie := &filesystem.Trie{
		Root: &filesystem.Entry{
			Path: "/f",
			Content: filesystem.Content{
				Type:      filesystem.MIMEDriveEntry,
				CreatedAt: now.Unix(),
			},
			Entries: []*filesystem.Entry{
				{
					Path: "ile",
					Content: filesystem.Content{
						Type:      filesystem.MIMEOctetStream,
						Name:      "file",
						Size:      512,
						CID:       "test_cid",
						Version:   1,
						CreatedAt: now.Unix(),
					},
				},
				{
					Path: "older",
					Content: filesystem.Content{
						Type:      filesystem.MIMEDriveEntry,
						CreatedAt: now.Unix(),
					},
					Entries: []*filesystem.Entry{
						{
							Path: "/f",
							Content: filesystem.Content{
								Type:      filesystem.MIMEDriveEntry,
								CreatedAt: now.Unix(),
							},
							Entries: []*filesystem.Entry{
								{
									Path: "ile",
									Content: filesystem.Content{
										Type:      filesystem.MIMEOctetStream,
										Name:      "file",
										Size:      512,
										CID:       "test_cid",
										Version:   1,
										CreatedAt: now.Unix(),
									},
								},
								{
									Path: "older",
									Content: filesystem.Content{
										Type:      filesystem.MIMEDriveEntry,
										CreatedAt: now.Unix(),
									},
									Entries: []*filesystem.Entry{
										{
											Path: "/file",
											Content: filesystem.Content{
												Type:      filesystem.MIMEOctetStream,
												Name:      "file",
												Size:      512,
												CID:       "test_cid",
												Version:   1,
												CreatedAt: now.Unix(),
											},
										},
										{
											Path: "1/file",
											Content: filesystem.Content{
												Type:      filesystem.MIMEOctetStream,
												Name:      "file",
												Size:      512,
												CID:       "test_cid",
												Version:   1,
												CreatedAt: now.Unix(),
											},
										},
									},
								},
							},
						},
						{
							Path: "1/file",
							Content: filesystem.Content{
								Type:      filesystem.MIMEOctetStream,
								Name:      "file",
								Size:      512,
								CID:       "test_cid",
								Version:   1,
								CreatedAt: now.Unix(),
							},
						},
					},
				},
			},
		},
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	assert.DeepEqual(t, *trie.Root, *rtrie.Root)
}

func TestRoutine2(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()
	dirs := []*filesystem.Entry{
		filesystem.NewEntry("/folder1/folder2/testfile1", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder1/testfile2", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder1/folder3", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder1/folder2/testfile1-copy", "test_cid", 512, filesystem.MIMEOctetStream, now),
	}

	for _, d := range dirs {
		pth := d.Path
		_, err := trie.AddFile(d)
		if err != nil {
			t.Fatal(err)
		}

		f, err := trie.File(pth)
		if err != nil {
			t.Fatal(err)
		}

		if f == nil {
			t.Fatalf("file '%s' not found", pth)
		}
	}

	mv1 := filesystem.NewEntry("/folder1/folder3/testfile1", "test_cid", 512, filesystem.MIMEOctetStream, now)
	_, err := trie.AddFile(mv1)
	if err != nil {
		t.Fatal(err)
	}

	err = trie.Delete("/folder1/folder2/testfile1-copy")
	if err != nil {
		t.Fatal(err)
	}

	f, err := trie.File("/folder1/folder2/testfile1")
	if err != nil {
		t.Fatal(err)
	}

	if f == nil {
		t.Fatalf("file '%s' not found", "/folder1/folder2/testfile1")
	}
}

func TestRoutine3(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()
	dirs := []*filesystem.Entry{
		filesystem.NewEntry("/folder1", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder2", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder123", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder2/myfile.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
		filesystem.NewEntry("/folder123/priom.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
	}

	res := []filesystem.Content{
		filesystem.NewContent("folder1", "", 0, filesystem.MIMEDriveDirectory, now),
		filesystem.NewContent("folder123", "", 0, filesystem.MIMEDriveDirectory, now),
		filesystem.NewContent("folder2", "", 0, filesystem.MIMEDriveDirectory, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		assert.NilError(t, err)
	}

	list := trie.Ls("/")
	assert.Equal(t, len(list), 3)

	for i, l := range list {
		assert.DeepEqual(t, *l, res[i])
	}

	err := trie.Delete("/folder123/priom.txt")
	assert.NilError(t, err)

	list = trie.Ls("/")
	assert.Equal(t, len(list), 3)

	for i, l := range list {
		assert.DeepEqual(t, *l, res[i])
	}
}

func TestTreeRecursive(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name           string
		dirs           []*filesystem.Entry
		expectedResult *filesystem.Entry
		err            error
	}{
		{
			name: "Empty fs",
			dirs: []*filesystem.Entry{},
			expectedResult: &filesystem.Entry{
				Content: filesystem.Content{Name: "/", Type: filesystem.MIMEDriveDirectory, CreatedAt: now.Unix()},
				Path:    "/",
			},
		},
		{
			name: "single file on root",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/file.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{},
			},
		},
		{
			name: "one level directory",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{},
					},
				},
			},
		},
		{
			name: "one level directories",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aab", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aba", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/abb", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/baa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bab", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bba", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bbb", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("aab", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aab",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aba",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("abb", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/abb",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("baa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/baa",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("bab", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bab",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("bba", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bba",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*filesystem.Entry{},
					},
				},
			},
		},
		{
			name: "one level directories with files",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/file1.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file2.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/bbb/file1.txt", "test_cid", 0, filesystem.MIMEOctetStream, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{},
					},
					{
						Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*filesystem.Entry{},
					},
				},
			},
		},
		{
			name: "two level directory",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{
							{
								Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*filesystem.Entry{},
							},
						},
					},
				},
			},
		},
		{
			name: "two level directories",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/bba", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/bab", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/baa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bbb/aaa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bbb/aab", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bbb/aba", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/bbb/abb", "test_cid", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{
							{
								Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("bba", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bba",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("bab", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bab",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("baa", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/baa",
								Entries: []*filesystem.Entry{},
							},
						},
					},
					{
						Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*filesystem.Entry{
							{
								Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/bbb/aaa",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("aab", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/bbb/aab",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("aba", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/bbb/aba",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("abb", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/bbb/abb",
								Entries: []*filesystem.Entry{},
							},
						},
					},
				},
			},
		},
		{
			name: "two level directories with files",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa", "test_cid", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/bbb/file1.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/bba/file2.txt", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/bbb/aaa/file1.txt", "test_cid", 0, filesystem.MIMEOctetStream, now),
			},
			expectedResult: &filesystem.Entry{
				Content: filesystem.NewContent("/", "", 0, filesystem.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*filesystem.Entry{
					{
						Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*filesystem.Entry{
							{
								Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*filesystem.Entry{},
							},
							{
								Content: filesystem.NewContent("bba", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/aaa/bba",
								Entries: []*filesystem.Entry{},
							},
						},
					},
					{
						Content: filesystem.NewContent("bbb", "", 0, filesystem.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*filesystem.Entry{
							{
								Content: filesystem.NewContent("aaa", "", 0, filesystem.MIMEDriveDirectory, now),
								Path:    "/bbb/aaa",
								Entries: []*filesystem.Entry{},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			ds := trie.Tree("/")

			assert.DeepEqual(t, ds, tc.expectedResult)
		})
	}
}

func TestRecursiveDelete(t *testing.T) {
	// Issues 735, PR 746
	trie := filesystem.NewTrie()
	now := time.Now()
	dirs := []*filesystem.Entry{
		filesystem.NewEntry("/folder/f1/f2/f3/f4", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f2/f3/f4", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f/f3/f4", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f/f/f4", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f/f/f", "", 0, filesystem.MIMEDriveEntry, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		assert.NilError(t, err)
	}

	entries := trie.LsRecursive("/folder")
	assert.Equal(t, len(entries), 14)

	for idx := len(entries) - 1; idx >= 0; idx-- {
		err := trie.Delete(filesystem.JoinPath("/folder" + entries[idx].Path))
		assert.NilError(t, err)
	}
	err := trie.Delete("/folder")
	assert.NilError(t, err)

	list := trie.Ls("/")
	assert.NilError(t, err)
	assert.Equal(t, len(list), 0)
}

func TestDeleteCornerCase(t *testing.T) {
	// Issues 504, PR 1040
	trie := filesystem.NewTrie()
	now := time.Now()
	_, err := trie.AddFile(filesystem.NewEntry("/logo.png", "", 0, "image/png", now))
	assert.NilError(t, err)

	_, err = trie.AddFile(filesystem.NewEntry("/logo.png(1)", "", 0, "image/png", now))
	assert.NilError(t, err)

	err = trie.Delete("/logo.png(1)")
	assert.NilError(t, err)

	_, err = trie.AddFile(filesystem.NewEntry("/logo.png(1)", "", 0, "image/png", now))
	assert.NilError(t, err)
}

func TestDotLsRecursive(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()
	dirs := []*filesystem.Entry{
		filesystem.NewEntry("/.", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/./Test.txt", "fake_cid", 512, filesystem.MIMEOctetStream, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		assert.NilError(t, err)
	}

	entries := trie.Ls("/")
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name, ".")

	entries = trie.Ls("/.")
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name, "Test.txt")
}

func TestMkDir(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()
	srcPath := "Test.txt"
	file := filesystem.NewEntry(srcPath, "fake_cid", 512, filesystem.MIMEOctetStream, now)
	_, err := trie.AddFile(file)
	assert.NilError(t, err)

	destPath := "/."
	dir := filesystem.NewEntry(destPath, "", 0, filesystem.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir)
	assert.NilError(t, err)

	info, err := trie.File(srcPath)
	assert.NilError(t, err)
	newFile := filesystem.NewEntry(filesystem.JoinPath(destPath, info.Name), info.CID, info.Size, info.Type, now)
	_, err = trie.AddFile(newFile)
	assert.NilError(t, err)

	err = trie.Delete(srcPath)
	assert.NilError(t, err)

	entries := trie.Ls("/")
	assert.NilError(t, err)
	assert.Equal(t, len(entries), 1)

	// Both directories starting with unicode char-issue #2355
	trie = filesystem.NewTrie()
	dir1 := filesystem.NewEntry("/Șfolder1", "", 0, filesystem.MIMEDriveEntry, now)
	dir2 := filesystem.NewEntry("/Țfolder2", "", 0, filesystem.MIMEDriveEntry, now)
	dir3 := filesystem.NewEntry("folder3 😋", "", 0, filesystem.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir1)
	assert.NilError(t, err)
	_, err = trie.AddFile(dir2)
	assert.NilError(t, err)
	_, err = trie.AddFile(dir3)
	assert.NilError(t, err)

	data, err := json.Marshal(trie)
	if err != nil {
		t.Fatal(err)
	}

	trie2 := filesystem.Trie{}
	err = json.Unmarshal(data, &trie2)
	if err != nil {
		t.Fatal(err)
	}

	cnts := trie2.Ls("/")
	assert.Equal(t, len(cnts), 3)
	assert.Equal(t, cnts[0].Name, "Șfolder1")
	assert.Equal(t, cnts[1].Name, "Țfolder2")
	assert.Equal(t, cnts[2].Name, "folder3 😋")
}

func TestConflictMv(t *testing.T) {
	trie := filesystem.NewTrie()
	now := time.Now()

	srcPath := "Test.txt"
	file := filesystem.NewEntry(srcPath, "fake_cid", 512, filesystem.MIMEOctetStream, now)
	_, err := trie.AddFile(file)
	assert.NilError(t, err)

	destPath := "/."
	dir := filesystem.NewEntry(destPath, "", 0, filesystem.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir)
	assert.NilError(t, err)

	info, err := trie.File(srcPath)
	assert.NilError(t, err)
	tmpFile := filesystem.NewEntry(srcPath, "fake_cid", 512, filesystem.MIMEOctetStream, now)
	newPath := strings.Replace(tmpFile.Path, srcPath, destPath, 1)
	newFile := filesystem.NewEntry(newPath, info.CID, info.Size, info.Type, now)
	_, err = trie.AddFile(newFile)
	assert.Equal(t, err, filesystem.ErrConflict)
}

func TestStrangeMvCases(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name            string
		newName         string
		oldName         string
		dirs            []*filesystem.Entry
		paths           []string
		expectedError   error
		expectedEntries int
	}{
		{
			name:    "Move folders with spaces",
			oldName: "/test",
			newName: "/test rename",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/test", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/test/some folder", "", 0, filesystem.MIMEDriveEntry, now),
			},
			paths:           []string{"/test rename", "/test rename/some folder"},
			expectedEntries: 2,
		},
		{
			name:    "Move folders with spaces",
			oldName: "/test",
			newName: "/test rename",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/test", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/test/some file", "fake_cid", 512, filesystem.MIMEOctetStream, now),
			},
			paths:           []string{"/test rename", "/test rename/some file"},
			expectedEntries: 2,
		},
		{
			name:    "Move from dot folder to 'dot' folder",
			oldName: "/.",
			newName: "/dot",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/.", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/./test.txt", "fake_cid", 512, filesystem.MIMEOctetStream, now),
			},
			paths:           []string{"/dot", "/dot/test.txt"},
			expectedEntries: 2,
		},
		{
			name:    "Move with tricky names",
			oldName: "/.<,?!%^%!@#+_*&",
			newName: "/&^^#%@+_)!)($&%)_)(*$*(&%",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/.<,?!%^%!@#+_*&", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/.<,?!%^%!@#+_*&/////&$*@#((<>}{{{}", "fake_cid", 512, filesystem.MIMEOctetStream, now),
			},
			paths:           []string{"/&^^#%@+_)!)($&%)_)(*$*(&%", "/&^^#%@+_)!)($&%)_)(*$*(&%/&$*@#((<>}{{{}"},
			expectedEntries: 2,
		},
		{
			name:    "Move file to root",
			oldName: "/Test.txt",
			newName: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/Test.txt", "fake_cid", 512, filesystem.MIMEOctetStream, now),
			},
			expectedError:   filesystem.ErrIllegalNameChars,
			expectedEntries: 1,
		},
		{
			name:    "Move file to double root",
			oldName: "/Test.txt",
			newName: "//",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/Test.txt", "fake_cid", 512, filesystem.MIMEOctetStream, now),
			},
			expectedError:   filesystem.ErrIllegalNameChars,
			expectedEntries: 1,
		},
		{
			name:    "Move folder to root",
			oldName: "/Dir2",
			newName: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/Dir2", "", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedError:   filesystem.ErrEmptyName,
			expectedEntries: 1,
		},
		{
			name:    "Move folder to double root",
			oldName: "/Dir1",
			newName: "//",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/Dir1", "", 0, filesystem.MIMEDriveEntry, now),
			},
			expectedError:   filesystem.ErrEmptyName,
			expectedEntries: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				assert.NilError(tt, err)
			}

			entries := trie.LsRecursive("/")
			assert.Equal(tt, len(entries), tc.expectedEntries)

			for i, e := range entries {
				typ := e.Type
				if typ == filesystem.MIMEDriveDirectory {
					typ = filesystem.MIMEDriveEntry
				}
				_, err := trie.AddFile(
					filesystem.NewEntry(
						strings.Replace(entries[i].Path, tc.oldName, tc.newName, 1), e.CID, e.Size, typ, now))
				if tc.expectedError != nil {
					assert.Equal(tt, err, tc.expectedError)
					return
				}
				assert.NilError(tt, err)
			}

			for idx := len(entries) - 1; idx >= 0; idx-- {
				err := trie.Delete(entries[idx].Path)
				assert.NilError(tt, err)
			}

			entries = trie.LsRecursive("/")
			assert.Equal(tt, len(entries), 2)

			for idx, p := range entries {
				assert.Equal(tt, p.Path, tc.paths[idx])
			}
		})
	}
}

func TestTrieConflict(t *testing.T) {
	now := time.Now()
	entries := []*filesystem.Entry{
		filesystem.NewEntry("/folder/f1/f2", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f2", "", 0, filesystem.MIMEDriveEntry, now),
		filesystem.NewEntry("/folder/f/f", "", 0, filesystem.MIMEDriveEntry, now),
	}
	trie := filesystem.NewTrie()
	for i := range entries {
		_, err := trie.AddFile(entries[i])
		assert.NilError(t, err)
	}

	_, err := trie.AddFile(filesystem.NewEntry("/folder/f", "", 0, filesystem.MIMEDriveEntry, now))
	assert.Equal(t, err, filesystem.ErrConflict)
}

func TestCreateRef(t *testing.T) {
	t.Parallel()
	now := time.Now()
	bucketID := "YUsvjhduiwiuZBIYUFSVGEUYDI"
	cases := []struct {
		name  string
		path  string
		dirs  []*filesystem.Entry
		rdirs []*filesystem.Entry
		res   []*filesystem.Entry
		err   error
	}{
		{
			name:  "createRef with empty path",
			path:  "",
			rdirs: []*filesystem.Entry{},
			err:   filesystem.ErrEmptyPath,
		},
		{
			name:  "createRef crash test",
			path:  "/aaa/bbb/file",
			dirs:  []*filesystem.Entry{},
			rdirs: []*filesystem.Entry{},
			err:   filesystem.ErrFileNotExist,
		},
		{
			name: "simple createRef for a file",
			path: "/aaa/bbb/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/bbb/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on first level file",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on root",
			path: "/",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			err: filesystem.ErrCantCreateRef,
		},
		{
			name: "createRef on top level file",
			path: "/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", bucketID, 0, filesystem.MIMEReference, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on top level dir with one entries",
			path: "/user",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/user/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/user", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/user/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on top level dir",
			path: "/aaa",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on first level dir",
			path: "/aaa/fbb",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fbb", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on trie node",
			path: "/aaa/f",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			err: filesystem.ErrFileNotExist,
		},
		{
			name: "createRef on top level file",
			path: "/aca",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aba/fbb/f", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/ada/fiee/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aca", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aca", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef entry containing semicolon",
			path: "/aaa/file",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file2", "test_cid", 512, filesystem.MIMEOctetStream, now),
				filesystem.NewEntry("/aaa/file", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/file", "test_cid", 512, filesystem.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on empty dir",
			path: "/aaa/dir1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir1", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir2", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir2", "", 0, filesystem.MIMEDriveEntry, now),
				filesystem.NewEntry("/aaa/dir1", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/aaa/dir1", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
		{
			name: "createRef on single child",
			path: "/folder/f1",
			dirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder/f1", "", 0, filesystem.MIMEDriveEntry, now),
			},
			rdirs: []*filesystem.Entry{
				filesystem.NewEntry("/folder/f1", bucketID, 0, filesystem.MIMEReference, now),
			},
			res: []*filesystem.Entry{
				filesystem.NewEntry("/folder/f1", "", 0, filesystem.MIMEDriveEntry, now),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			rtrie := filesystem.NewTrie()
			for _, d := range tc.rdirs {
				_, err := rtrie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			trieBucket2 := filesystem.NewTrie()
			for _, d := range tc.res {
				_, err := trieBucket2.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			rtrieBucket2 := filesystem.NewTrie()

			entries, err := trie.CreateRef(tc.path, bucketID, now)
			if err != nil {
				assert.Equal(t, err, tc.err)
			}
			for _, d := range entries {
				if d.Type == filesystem.MIMEDriveDirectory {
					d.Type = filesystem.MIMEDriveEntry
				}

				_, err := rtrieBucket2.AddFile(filesystem.NewEntry(d.Path, d.CID, d.Size, d.Content.Type, now))
				if err != nil {
					t.Fatal(err)
				}
			}
			ignoreCreatedAt(trie.Root, rtrie.Root)
			ignoreCreatedAt(trieBucket2.Root, trieBucket2.Root)

			assert.DeepEqual(t, trie.Root, rtrie.Root)
			assert.DeepEqual(t, trieBucket2.Root, rtrieBucket2.Root)
		})
	}
}

func TestReplace(t *testing.T) {
	trie := filesystem.NewTrie()
	oldEntry := filesystem.NewEntry("/home/test.txt", "cid1", 100, filesystem.MIMEOctetStream, time.Now())
	_, err := trie.AddFile(oldEntry)
	if err != nil {
		t.Fatal(err)
	}
	updatedContent := filesystem.NewContent("test.txt", "cid2", 1000, filesystem.MIMEOctetStream, time.Now())
	_, _, err = trie.Replace("/home/test.txt", &updatedContent)
	if err != nil {
		t.Fatal(err)
	}
	cnt, err := trie.File("/home/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, cnt.Size, updatedContent.Size)
	assert.Equal(t, cnt.CID, updatedContent.CID)
}

func TestFuzzyCreateRef(t *testing.T) {
	if env.SkipFuzzTest() {
		t.Skip()
	}
	t.Parallel()
	now := time.Now()
	f := fuzz.New()
	for i := 0; i < 50000; i++ {
		trie := filesystem.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			var dataIndex int
			f.Fuzz(&dataIndex)
			index := dataIndex % (len(paths))
			path := paths[int64(math.Abs(float64(index)))]
			_, _ = trie.CreateRef(path, "", now)
		} else {
			// randomly generate path to be deleted
			var path string
			f.Fuzz(&path)
			_, _ = trie.CreateRef(path, "", now)
		}
	}
}

func TestCleanPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "full path with trailing separator",
			path:     "/a/b/c/",
			expected: "/a/b/c",
		},
		{
			name:     "full path without trailing separator",
			path:     "/a/b/c",
			expected: "/a/b/c",
		},
		{
			name:     "single character with trailing separator",
			path:     "/a/",
			expected: "/a",
		},
		{
			name:     "single character without trailing separator",
			path:     "/a",
			expected: "/a",
		},
		{
			name:     "single character without leading separator",
			path:     "a/",
			expected: "/a",
		},
		{
			name:     "single character without leading and trailing separator",
			path:     "a",
			expected: "/a",
		},
		{
			name:     "path with double separators",
			path:     "/a/b///c/d//",
			expected: "/a/b/c/d",
		},
		{
			name:     "root path",
			path:     "/",
			expected: "/",
		},
		{
			name:     "root path with double separators",
			path:     "///",
			expected: "/",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := filesystem.CleanPath(tc.path)
			assert.Equal(t, result, tc.expected)
		})
	}
}
func ignoreCreatedAt(e1 *filesystem.Entry, e2 *filesystem.Entry) {
	if e1 == nil || e2 == nil {
		return
	}
	e1.Content.CreatedAt = e2.Content.CreatedAt
	for i := 0; i < len(e1.Entries); i++ {
		ignoreCreatedAt(e1.Entries[i], e2.Entries[i])
	}
}

// https://github.com/ChainSafe/files-api/issues/2477
func TestIssue2477(t *testing.T) {
	trie := filesystem.NewTrie()
	dirs := []string{
		"/さだまさし",
		"/さだまさし/さだ丼～新自分風土記III～",
		"/さだまさし/存在理由～Raison d’etre～",
		"/さだまさし/新自分風土記I～望郷篇～",
		"/さだまさし/孤悲",
	}
	files := []string{
		"/さだまさし/孤悲/01. 孤悲.m4a",
		"/さだまさし/孤悲/02. 抱擁.m4a",
	}
	for _, dir := range dirs {
		_, err := trie.AddFile(filesystem.NewEntry(dir, "test-cid", 100, filesystem.MIMEDriveEntry, time.Now()))
		if err != nil {
			t.Error(err)
		}
	}
	for _, file := range files {
		_, err := trie.AddFile(filesystem.NewEntry(file, "test-cid", 100, filesystem.MIMEOctetStream, time.Now()))
		if err != nil {
			t.Error(err)
		}
	}

	entries := trie.LsRecursive("/")
	assert.Equal(t, len(files)+len(dirs), len(entries))
}

func TestEntryCount(t *testing.T) {
	files := map[string][]string{
		"/folder1":                 {"file1", "test", "test.txt"},
		"/folder1/folder2":         {"file2", "test(1)", "test(1).txt"},
		"/folder1/folder2/folder3": {"file3", "new file", "new file 1"},
		"/folder1(1)":              {"file1", "new file (1)", "folder1"},
	}

	paths := []string{
		"/folder1(1)",
		"/folder1",
		"/folder1/folder2",
		"/folder1/folder2/folder3",
	}

	trie := filesystem.NewTrie()
	total := 0
	for _, path := range paths {
		for _, e := range files[path] {
			entry := filesystem.NewEntry(filesystem.JoinPath(path, e), "", 0, filesystem.MIMEOctetStream, time.Now())
			added, err := trie.AddFile(entry)
			if err != nil {
				t.Fatal(err)
			}
			total += len(added)
		}
	}
	assert.Equal(t, total, 16)
}

func TestConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	trie := filesystem.NewTrie()
	now := time.Now()

	// Seed the trie with initial data
	_, err := trie.AddFile(filesystem.NewEntry("/seed/file", "cid0", 100, filesystem.MIMEOctetStream, now))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const writers = 5
	const readers = 10
	const iterations = 50

	// Concurrent writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				path := filesystem.JoinPath("/concurrent", strings.Repeat("a", id+1), strings.Repeat("b", i+1))
				entry := filesystem.NewEntry(path, "cid", 64, filesystem.MIMEOctetStream, now)
				trie.AddFile(entry)
			}
		}(w)
	}

	// Concurrent readers
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				trie.Ls("/")
				trie.File("/seed/file")
				trie.Stat("/seed/file")
				trie.LsRecursive("/")
				trie.Tree("/")
				trie.Hash()
			}
		}()
	}

	wg.Wait()
}

func TestDeepCopyOnAdd(t *testing.T) {
	t.Parallel()

	now := time.Now()
	trie := filesystem.NewTrie()

	// Add a folder with a child
	folder := filesystem.NewEntry("/docs", "", 0, filesystem.MIMEDriveEntry, now)
	_, err := trie.AddFile(folder)
	if err != nil {
		t.Fatal(err)
	}

	child := filesystem.NewEntry("/docs/readme", "cid1", 100, filesystem.MIMEOctetStream, now)
	_, err = trie.AddFile(child)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot the trie state before mutation
	fileBefore, err := trie.File("/docs/readme")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, fileBefore.CID, "cid1")

	// Mutate the original entry that was passed to AddFile
	child.Content.CID = "corrupted"
	child.Path = "/totally/different"

	// Trie must be unaffected
	fileAfter, err := trie.File("/docs/readme")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, fileAfter.CID, "cid1")
}

func TestDeepCopyOnCopy(t *testing.T) {
	t.Parallel()

	now := time.Now()

	original := &filesystem.Entry{
		Content: filesystem.Content{
			Name:      "file",
			CID:       "cid1",
			Type:      filesystem.MIMEOctetStream,
			Size:      100,
			Version:   1,
			CreatedAt: now.Unix(),
		},
		Path: "/dir/file",
		Entries: []*filesystem.Entry{
			{
				Content: filesystem.Content{
					Name:      "child",
					CID:       "cid2",
					Type:      filesystem.MIMEOctetStream,
					Size:      50,
					Version:   1,
					CreatedAt: now.Unix(),
				},
				Path: "/dir/file/child",
			},
		},
	}

	// Use the exported Copy method
	copied := &filesystem.Entry{}
	copied.Copy(original)

	// Verify the copy matches
	assert.Equal(t, copied.Content.CID, "cid1")
	assert.Equal(t, copied.Entries[0].Content.CID, "cid2")

	// Mutate the original's child
	original.Entries[0].Content.CID = "corrupted"
	original.Entries = append(original.Entries, &filesystem.Entry{
		Path: "/extra",
	})

	// The copy must be unaffected
	assert.Equal(t, copied.Entries[0].Content.CID, "cid2")
	assert.Equal(t, len(copied.Entries), 1)
}

func TestCommonPrefixSplit(t *testing.T) {
	t.Parallel()

	now := time.Now()

	cases := []struct {
		name    string
		paths   []string
		lsPath  string
		wantLen int
	}{
		{
			name:    "short common prefix",
			paths:   []string{"/file", "/folder/doc"},
			lsPath:  "/",
			wantLen: 2,
		},
		{
			name:    "long common prefix diverging mid-segment",
			paths:   []string{"/documents/report_final", "/documents/report_draft"},
			lsPath:  "/documents",
			wantLen: 2,
		},
		{
			name:    "no common prefix beyond root",
			paths:   []string{"/alpha", "/beta"},
			lsPath:  "/",
			wantLen: 2,
		},
		{
			name:    "one path is prefix of another segment",
			paths:   []string{"/abc", "/abcdef"},
			lsPath:  "/",
			wantLen: 2,
		},
		{
			name:    "emoji names diverging on different emoji",
			paths:   []string{"/folder/\U0001F600file", "/folder/\U0001F601file"},
			lsPath:  "/folder",
			wantLen: 2,
		},
		{
			name:    "emoji shared then ascii diverges",
			paths:   []string{"/\U0001F600/abc", "/\U0001F600/xyz"},
			lsPath:  "/\U0001F600",
			wantLen: 2,
		},
		{
			name:    "multi-byte prefix with shared leading bytes",
			paths:   []string{"/dir/\U0001F600rest", "/dir/\U0001F601rest"},
			lsPath:  "/dir",
			wantLen: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := filesystem.NewTrie()
			for _, p := range tc.paths {
				entry := filesystem.NewEntry(p, "cid", 64, filesystem.MIMEOctetStream, now)
				_, err := trie.AddFile(entry)
				if err != nil {
					t.Fatal(err)
				}
			}

			// Verify all files are retrievable
			for _, p := range tc.paths {
				f, err := trie.File(p)
				if err != nil {
					t.Fatalf("File(%q) failed: %v", p, err)
				}
				assert.Equal(t, f.CID, "cid")
			}

			// Verify ls returns expected count
			contents := trie.Ls(tc.lsPath)
			assert.Equal(t, len(contents), tc.wantLen)
		})
	}
}

func TestEmojiRuneDispatch(t *testing.T) {
	t.Parallel()

	now := time.Now()
	trie := filesystem.NewTrie()

	// Create a parent directory so the trie has an internal node with children
	// whose paths start with multi-byte runes. This exercises the first-rune
	// comparison used to dispatch into the correct child entry.
	paths := []string{
		"/parent/\U0001F600-smile",    // 😀
		"/parent/\U0001F601-grin",     // 😁  (shares 3 of 4 UTF-8 bytes with 😀)
		"/parent/\U0001F680-rocket",   // 🚀  (different leading bytes than 😀/😁)
		"/parent/ascii-file",          // plain ASCII sibling
	}

	for _, p := range paths {
		entry := filesystem.NewEntry(p, "cid-"+p, 64, filesystem.MIMEOctetStream, now)
		_, err := trie.AddFile(entry)
		if err != nil {
			t.Fatalf("AddFile(%q) failed: %v", p, err)
		}
	}

	// Every file must be independently retrievable with correct CID
	for _, p := range paths {
		f, err := trie.File(p)
		if err != nil {
			t.Fatalf("File(%q) failed: %v", p, err)
		}
		assert.Equal(t, f.CID, "cid-"+p)
	}

	// Ls on parent must list all four children
	contents := trie.Ls("/parent")
	assert.Equal(t, len(contents), len(paths))
}
