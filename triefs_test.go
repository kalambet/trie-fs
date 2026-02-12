package triefs_test

import (
	"encoding/json"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	triefs "github.com/kalambet/trie-fs"
)

func randString(r *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-+@#!%^&()[] "
	n := r.Intn(20) + 1
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[r.Intn(len(chars))]
	}
	return string(b)
}

func createRandomFiles(trie *triefs.Trie, n int) []string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fileTypes := []string{triefs.MIMEDriveEntry, triefs.MIMEOctetStream}
	var paths []string
	createdAt := time.Now()
	for j := 0; j < n; j++ {
		path := "/" + randString(r)
		cid := randString(r)
		size := r.Int63()
		fileTypeIndex := r.Intn(len(fileTypes))

		paths = append(paths, path)

		// generate filetype randomly
		index := fileTypeIndex % (len(fileTypes))
		fileType := fileTypes[int64(math.Abs(float64(index)))]
		e := triefs.NewEntry(path, cid, size, fileType, createdAt)
		_, _ = trie.AddFile(e)
	}
	return paths
}

func TestHash(t *testing.T) {
	for i := 1; i <= 100; i++ {
		trie := triefs.NewTrie()
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
		dirs  []*triefs.Entry
		added [][]*triefs.Entry
		root  *triefs.Entry
		err   error
	}{
		{
			name: "add a nil",
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{nil},
		},
		{
			name: "add a directory",
			err:  triefs.ErrCantAddDirectory,
			dirs: []*triefs.Entry{triefs.NewEntry("/new_dir", "", 0, triefs.MIMEDriveDirectory, now)},
		},
		{
			name: "add empty path file",
			err:  triefs.ErrEmptyPath,
			dirs: []*triefs.Entry{triefs.NewEntry("", "test_cid", 512, triefs.MIMEOctetStream, now)},
		},
		{
			name: "add illegal directory",
			err:  triefs.ErrIllegalPathChars,
			dirs: []*triefs.Entry{triefs.NewEntry("/some:dir", "", 0, triefs.MIMEDriveEntry, now)},
		},
		{
			name: "add root directory",
			err:  triefs.ErrEmptyName,
			dirs: []*triefs.Entry{triefs.NewEntry("/", "", 0, triefs.MIMEDriveEntry, now)},
		},
		{
			name: "add single directory",
			err:  nil,
			dirs: []*triefs.Entry{triefs.NewEntry("/folder1", "", 0, triefs.MIMEDriveEntry, now)},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1",
				Content: triefs.Content{
					Name:      "",
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
				},
			},
		},
		{
			name: "basic directory first",
			err:  nil,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, "", now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/file",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/file",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
									Name:      "f",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/f", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("f", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aba/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aba"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aba/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("aca", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aca"},
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aca/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aca/file/file"},
				},
			},
			root: &triefs.Entry{
				Path: "/a",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "aa/bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "ba/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "ca/file/file",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/f", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/f"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: "ile/file",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/f", "", 0, triefs.MIMEDriveEntry, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("f", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/f"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "f",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "ile/file",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file/test", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa"},
					&triefs.Entry{Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/bbb"},
					&triefs.Entry{Content: triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/bbb/f"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now), Path: "/aaa/file"},
					&triefs.Entry{Content: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/file"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("test", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/aaa/file/test"},
				},
			},
			root: &triefs.Entry{
				Path: "/aaa/",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "bbb/f",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "f",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "file/",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: "file",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
									Name:      "file",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "test",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile1", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile11", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile111", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile1", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile1"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile11", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile111", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile111"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "myfile1",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "1",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
									Name:      "myfile11",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "1",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/folder1/folder2/myfile11", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/folder1/folder2/myfile111", "", 0, triefs.MIMEDriveEntry, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile1"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile11", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile111", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile111"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "1",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: "1",
								Content: triefs.Content{
									Type:      triefs.MIMEDriveEntry,
									CreatedAt: now.Unix(),
								},
								Entries: []*triefs.Entry{
									{
										Path: ":",
										Content: triefs.Content{
											Type:      triefs.MIMEDriveEntry,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile111", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile11", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile1", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile111", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile111"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile11", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile11"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile1", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile1"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile1",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "1",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
						Entries: []*triefs.Entry{
							{
								Path: "1",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
									Name:      "myfile111",
									Size:      512,
									CID:       "test_cid",
									Version:   1,
									CreatedAt: now.Unix(),
								},
							},
							{
								Path: ":",
								Content: triefs.Content{
									Type:      triefs.MIMEOctetStream,
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
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile3", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile2", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile2"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "3",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "myfile3",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "2",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "myfile2",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile3", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: "3",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
							Name:      "myfile3",
							Size:      512,
							CID:       "test_cid",
							Version:   1,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			err:  triefs.ErrConflict,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/myfile3", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/folder1/folder2/myfile", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/folder1/folder2/myfile", "", 0, triefs.MIMEDriveEntry, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1"},
					&triefs.Entry{Content: triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2"},
					&triefs.Entry{Content: triefs.NewContent("myfile3", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/folder1/folder2/myfile3"},
				},
				{
					&triefs.Entry{Content: triefs.NewContent("myfile", "", 0, triefs.MIMEDriveDirectory, now), Path: "/folder1/folder2/myfile"},
				},
			},
			root: &triefs.Entry{
				Path: "/folder1/folder2/myfile",
				Content: triefs.Content{
					Type:      triefs.MIMEDriveEntry,
					CreatedAt: now.Unix(),
				},
				Entries: []*triefs.Entry{
					{
						Path: ":",
						Content: triefs.Content{
							Type:      triefs.MIMEDriveEntry,
							CreatedAt: now.Unix(),
						},
					},
					{
						Path: "3",
						Content: triefs.Content{
							Type:      triefs.MIMEOctetStream,
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/mambo #5", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			added: [][]*triefs.Entry{
				{
					&triefs.Entry{Content: triefs.NewContent("mambo #5", "test_cid", 512, triefs.MIMEOctetStream, now), Path: "/mambo #5"},
				},
			},
			root: &triefs.Entry{
				Path: "/mambo #5",
				Content: triefs.Content{
					Type:      triefs.MIMEOctetStream,
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
			trie := triefs.NewTrie()
			for i, e := range tc.dirs {
				entries, err := trie.AddFile(e)
				if err != nil {
					if err != tc.err {
						t.Errorf("got %v, want %v", err, tc.err)
					}
				} else {
					if !reflect.DeepEqual(tc.added[i], entries) {
						t.Errorf("got %v, want %v", tc.added[i], entries)
					}
				}
			}
			if !reflect.DeepEqual(trie.Root, tc.root) {
				t.Errorf("got %v, want %v", trie.Root, tc.root)
			}
		})
	}
}

func TestFuzzyAddFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()
		createRandomFiles(trie, 20)
	}
}

func TestLs(t *testing.T) {
	t.Parallel()
	now := time.Now()

	cases := []struct {
		name    string
		path    string
		dirs    []*triefs.Entry
		content []triefs.Content
		err     error
	}{
		{
			name:    "ls on nil",
			err:     nil,
			path:    "/",
			content: []triefs.Content{},
		},
		{
			name: "simple list 1",
			err:  nil,
			path: "/folder1/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{},
		},
		{
			name: "ls root simple",
			err:  nil,
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls with semicolon",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file1", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("file1", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "ls from a slash child",
			err:  nil,
			path: "/priom",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/priom", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/priom/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "ls from a similar child",
			err:  nil,
			path: "/priom",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/priompriom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/priom", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/priom/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aba/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aca/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aca", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aba/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aca/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aca", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level dirs 2",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/folder1", "test_cid", 512, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/folder2", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/folder3", "test_cid", 512, triefs.MIMEDriveEntry, now),
			},
			content: []triefs.Content{
				triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("folder3", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed 2",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/fiee", "", 0, triefs.MIMEDriveEntry, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls first level mixed 3",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/test", "", 0, triefs.MIMEDriveEntry, now),
			},
			content: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
			},
		},
		{
			name: "ls file",
			err:  nil,
			path: "/aaa/fbb/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{},
		},
		{
			name: "ls entry (as a trie node)",
			err:  nil,
			path: "/aaa/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{},
		},
		{
			name: "ls non existent entry",
			err:  nil,
			path: "/aaa/test",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{},
		},
		{
			name: "ls second layer",
			err:  nil,
			path: "/aaa/fbb",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			content: []triefs.Content{
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "ls reference entry",
			err:  nil,
			path: "/aaa/fbb/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 0, triefs.MIMEReference, now),
			},
			content: []triefs.Content{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt := trie.Ls(tc.path)

			if len(cnt) != len(tc.content) {
				t.Errorf("got %v, want %v", len(cnt), len(tc.content))
			}
			for i, c := range tc.content {
				if !reflect.DeepEqual(c, *cnt[i]) {
					t.Errorf("got %v, want %v", c, *cnt[i])
				}
			}
		})
	}
}

func TestFuzzyLs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_ = trie.Ls(path)
		} else {
			// randomly generate path to be deleted
			path := randString(r)
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
		dirs   []*triefs.Entry
		rdirs  []triefs.Content
		rpaths []string
		err    error
	}{
		{
			name:   "ls on nil",
			err:    nil,
			path:   "/",
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "ls on file",
			err:  nil,
			path: "/folder/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "simple list 1",
			err:  nil,
			path: "/folder1/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder1/folder2/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "ls root simple",
			err:  nil,
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/f", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file1", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("file1", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file1", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("file1", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/priom", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/priom/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/priom.txt",
			},
		},
		{
			name: "ls from a similar child",
			err:  nil,
			path: "/priom",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/priompriom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/priom", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/priom/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rpaths: []string{
				"/priom.txt",
			},
		},
		{
			name: "ls root complex",
			err:  nil,
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aba/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aca/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aca", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aba/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aca/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("aca", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/fiee", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/test", "", 0, triefs.MIMEDriveEntry, now),
			},

			rdirs: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("test", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
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
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "ls entry (as a trie node)",
			err:  nil,
			path: "/aaa/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "ls non existent entry",
			err:  nil,
			path: "/aaa/test",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs:  []triefs.Content{},
			rpaths: []string{},
		},
		{
			name: "ls second layer",
			err:  nil,
			path: "/aaa/fbb",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []triefs.Content{
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rpaths: []string{"/f"},
		},
		{
			name: "ls on reference entry",
			err:  nil,
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fieeolder_emtpty", "", 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/fiee/test", "", 0, triefs.MIMEDriveEntry, now),
			},

			rdirs: []triefs.Content{
				triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("fiee", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewContent("test", "", 0, triefs.MIMEDriveDirectory, now),
				triefs.NewContent("fieeolder_emtpty", "", 0, triefs.MIMEReference, now),
				triefs.NewContent("file", "test_cid", 0, triefs.MIMEReference, now),
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
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			entries := trie.LsRecursive(tc.path)

			if len(entries) != len(tc.rdirs) {
				t.Errorf("got %v, want %v", len(entries), len(tc.rdirs))
			}
			for i, c := range tc.rdirs {
				if !reflect.DeepEqual(c, entries[i].Content) {
					t.Errorf("got %v, want %v", c, entries[i].Content)
				}
				if tc.rpaths[i] != entries[i].Path {
					t.Errorf("got %v, want %v", tc.rpaths[i], entries[i].Path)
				}
			}
		})
	}
}

func TestFuzzyLsRecursive(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_ = trie.LsRecursive(path)
		} else {
			// randomly generate path to be deleted
			path := randString(r)
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
		dirs  []*triefs.Entry
		rdirs []*triefs.Entry
		err   error
	}{
		{
			name:  "delete empty",
			path:  "",
			rdirs: []*triefs.Entry{},
			err:   triefs.ErrEmptyPath,
		},
		{
			name:  "delete crash test",
			path:  "/aaa/bbb/file",
			dirs:  []*triefs.Entry{},
			rdirs: []*triefs.Entry{},
		},
		{
			name: "simple delete file",
			path: "/aaa/bbb/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
		{
			name: "delete first level file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete root",
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level file",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level dir",
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete first level dir",
			path: "/aaa/fbb",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete trie node",
			path: "/aaa/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete top level file",
			path: "/aca",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete entry with semicolon",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "delete empty dir",
			path: "/aaa/dir1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir2", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir2", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
		{
			name: "issue 735",
			path: "/folder/f1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder/f1", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/folder", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
		{
			name: "issue 735 regression after fix",
			path: "/folder/f1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/folder/f1", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/folder", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			rtrie := triefs.NewTrie()
			for _, d := range tc.rdirs {
				_, err := rtrie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			err := trie.Delete(tc.path)
			if err != nil {
				if err != tc.err {
					t.Errorf("got %v, want %v", err, tc.err)
				}
			}

			if !reflect.DeepEqual(trie.Root, rtrie.Root) {
				t.Errorf("got %v, want %v", trie.Root, rtrie.Root)
			}
		})
	}
}

func TestFuzzyDeleteFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_ = trie.Delete(path)
		} else {
			// randomly generate path to be deleted
			path := randString(r)
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
		dirs []*triefs.Entry
		file triefs.Content
		err  error
	}{
		{
			name: "empty test",
			path: "",
			err:  triefs.ErrEmptyPath,
		},
		{
			name: "crash test",
			path: "/aaa/bbb/file",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{},
			file: triefs.Content{},
		},
		{
			name: "simple get file",
			path: "/aaa/bbb/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get first level file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get root",
			path: "/",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get top level file",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get top level dir",
			path: "/aaa",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get first level dir",
			path: "/aaa/fbb",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get trie node",
			path: "/aaa/f",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get top level file",
			path: "/aca",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("aca", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/dir",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir2", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("dir", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir",
			path: "/aaa/fdir1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fdir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fdir1", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fdir12", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir2", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fdir12", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get reference entry",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 0, triefs.MIMEReference, now),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt, err := trie.File(tc.path)
			if err != nil {
				if err != tc.err {
					t.Errorf("got %v, want %v", err, tc.err)
				}
			}

			if len(tc.file.Name) != 0 && cnt == nil {
				t.Fatalf("nil returned instaen of %#v", tc.file)
			}

			if cnt != nil {
				if !reflect.DeepEqual(tc.file, *cnt) {
					t.Errorf("got %v, want %v", tc.file, *cnt)
				}
			}
		})
	}
}

func TestFuzzyFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_, _ = trie.File(path)
		} else {
			// randomly generate path to get the metadata
			path := randString(r)
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
		dirs []*triefs.Entry
		file triefs.Content
		err  error
	}{
		{
			name: "empty test",
			path: "",
			err:  triefs.ErrEmptyPath,
		},
		{
			name: "crash test",
			path: "/aaa/bbb/file",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{},
			file: triefs.Content{},
		},
		{
			name: "simple get file",
			path: "/aaa/bbb/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get first level file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get root",
			path: "/",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get top level file",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get top level dir",
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get first level dir",
			path: "/aaa/fbb",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fbb", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get trie node",
			path: "/aaa/f",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "get top level file",
			path: "/aca",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("aca", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 512, triefs.MIMEOctetStream, now),
		},
		{
			name: "get semicolon file - empty directory",
			path: "/aaa/dir",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir2", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("dir", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get semicolon file",
			path: "/aaa/dir",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir2/file2", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir/file2", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("dir", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir",
			path: "/aaa/fdir1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fdir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fdir1", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get info on an empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fdir12", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir2", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fdir12", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get info on non empty dir in bunch of similar neighbors",
			path: "/aaa/fdir12",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fdir12/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("fdir12", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "get reference entry",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("file", "test_cid", 0, triefs.MIMEReference, now),
		},
		{
			name: "complex scenario #1",
			path: "/a",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "complex scenario #2",
			path: "/abca",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.Content{},
		},
		{
			name: "complex scenario #3",
			path: "/abcab",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("abcab", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #4",
			path: "/akcab1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/akcab/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("akcab1", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #5",
			path: "/akcab",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/akcab/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			file: triefs.NewContent("akcab", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #6",
			path: "/adcac/fdir",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/adcac/fdir3/file", "", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.Content{},
		},
		{
			name: "complex scenario #7",
			path: "/adcac/fdir12",
			err:  triefs.ErrFileNotExist,
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/adcac/fdir3/file", "", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.Content{},
		},
		{
			name: "complex scenario #8",
			path: "/adcac/fdir3",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/adcac/fdir3/file", "", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("fdir3", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #9",
			path: "/adcac/fdir2/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/abcab/folder1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/adcac/fdir2/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/afcad/fdir1/file", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/akcab1/file/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/adcac/fdir3/file", "", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("file", "", 0, triefs.MIMEDriveDirectory, now),
		},
		{
			name: "complex scenario #10",
			path: "/a/b/c",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/a/b/c/d/e", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/a/b/c/f/g", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/a/b/f/g/e", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/a/b/f/k/g", "", 0, triefs.MIMEDriveEntry, now),
			},
			file: triefs.NewContent("c", "", 0, triefs.MIMEDriveDirectory, now),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			cnt, err := trie.Stat(tc.path)
			if tc.err != nil {
				if err != tc.err {
					t.Errorf("got %v, want %v", err, tc.err)
				}
			}
			if len(tc.file.Name) != 0 && cnt == nil {
				t.Fatalf("nil returned instaed of %#v", tc.file)
			}

			if cnt != nil {
				if !reflect.DeepEqual(tc.file, *cnt) {
					t.Errorf("got %v, want %v", tc.file, *cnt)
				}
			}
		})
	}
}

func TestFuzzyStat(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_, _ = trie.Stat(path)
		} else {
			// randomly generate path to get the metadata
			path := randString(r)
			_, _ = trie.Stat(path)
		}
	}
}

func TestRoutine(t *testing.T) {
	trie := triefs.NewTrie()
	now := time.Now()
	dirs := []*triefs.Entry{
		triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/file", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder/folder", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/folder/file", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder1", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder1/file", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder/folder1", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/folder1/file", "test_cid", 512, triefs.MIMEOctetStream, now),
	}

	rtrie := &triefs.Trie{
		Root: &triefs.Entry{
			Path: "/f",
			Content: triefs.Content{
				Type:      triefs.MIMEDriveEntry,
				CreatedAt: now.Unix(),
			},
			Entries: []*triefs.Entry{
				{
					Path: "ile",
					Content: triefs.Content{
						Type:      triefs.MIMEOctetStream,
						Name:      "file",
						Size:      512,
						CID:       "test_cid",
						Version:   1,
						CreatedAt: now.Unix(),
					},
				},
				{
					Path: "older",
					Content: triefs.Content{
						Type:      triefs.MIMEDriveEntry,
						CreatedAt: now.Unix(),
					},
					Entries: []*triefs.Entry{
						{
							Path: "/f",
							Content: triefs.Content{
								Type:      triefs.MIMEDriveEntry,
								CreatedAt: now.Unix(),
							},
							Entries: []*triefs.Entry{
								{
									Path: "ile",
									Content: triefs.Content{
										Type:      triefs.MIMEOctetStream,
										Name:      "file",
										Size:      512,
										CID:       "test_cid",
										Version:   1,
										CreatedAt: now.Unix(),
									},
								},
								{
									Path: "older",
									Content: triefs.Content{
										Type:      triefs.MIMEDriveEntry,
										CreatedAt: now.Unix(),
									},
									Entries: []*triefs.Entry{
										{
											Path: "/file",
											Content: triefs.Content{
												Type:      triefs.MIMEOctetStream,
												Name:      "file",
												Size:      512,
												CID:       "test_cid",
												Version:   1,
												CreatedAt: now.Unix(),
											},
										},
										{
											Path: "1/file",
											Content: triefs.Content{
												Type:      triefs.MIMEOctetStream,
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
							Content: triefs.Content{
								Type:      triefs.MIMEOctetStream,
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

	if !reflect.DeepEqual(*trie.Root, *rtrie.Root) {
		t.Errorf("got %v, want %v", *trie.Root, *rtrie.Root)
	}
}

func TestRoutine2(t *testing.T) {
	trie := triefs.NewTrie()
	now := time.Now()
	dirs := []*triefs.Entry{
		triefs.NewEntry("/folder1/folder2/testfile1", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder1/testfile2", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder1/folder3", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder1/folder2/testfile1-copy", "test_cid", 512, triefs.MIMEOctetStream, now),
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

	mv1 := triefs.NewEntry("/folder1/folder3/testfile1", "test_cid", 512, triefs.MIMEOctetStream, now)
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
	trie := triefs.NewTrie()
	now := time.Now()
	dirs := []*triefs.Entry{
		triefs.NewEntry("/folder1", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder2", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder123", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder2/myfile.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
		triefs.NewEntry("/folder123/priom.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
	}

	res := []triefs.Content{
		triefs.NewContent("folder1", "", 0, triefs.MIMEDriveDirectory, now),
		triefs.NewContent("folder123", "", 0, triefs.MIMEDriveDirectory, now),
		triefs.NewContent("folder2", "", 0, triefs.MIMEDriveDirectory, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	list := trie.Ls("/")
	if len(list) != 3 {
		t.Errorf("got %v, want %v", len(list), 3)
	}

	for i, l := range list {
		if !reflect.DeepEqual(*l, res[i]) {
			t.Errorf("got %v, want %v", *l, res[i])
		}
	}

	err := trie.Delete("/folder123/priom.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list = trie.Ls("/")
	if len(list) != 3 {
		t.Errorf("got %v, want %v", len(list), 3)
	}

	for i, l := range list {
		if !reflect.DeepEqual(*l, res[i]) {
			t.Errorf("got %v, want %v", *l, res[i])
		}
	}
}

func TestTreeRecursive(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name           string
		dirs           []*triefs.Entry
		expectedResult *triefs.Entry
		err            error
	}{
		{
			name: "Empty fs",
			dirs: []*triefs.Entry{},
			expectedResult: &triefs.Entry{
				Content: triefs.Content{Name: "/", Type: triefs.MIMEDriveDirectory, CreatedAt: now.Unix()},
				Path:    "/",
			},
		},
		{
			name: "single file on root",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/file.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{},
			},
		},
		{
			name: "one level directory",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{},
					},
				},
			},
		},
		{
			name: "one level directories",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aab", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aba", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/abb", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/baa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bab", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bba", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bbb", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("aab", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aab",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aba",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("abb", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/abb",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("baa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/baa",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("bab", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bab",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("bba", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bba",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*triefs.Entry{},
					},
				},
			},
		},
		{
			name: "one level directories with files",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/file1.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file2.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/bbb/file1.txt", "test_cid", 0, triefs.MIMEOctetStream, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{},
					},
					{
						Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*triefs.Entry{},
					},
				},
			},
		},
		{
			name: "two level directory",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{
							{
								Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*triefs.Entry{},
							},
						},
					},
				},
			},
		},
		{
			name: "two level directories",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/bba", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/bab", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/baa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bbb/aaa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bbb/aab", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bbb/aba", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/bbb/abb", "test_cid", 0, triefs.MIMEDriveEntry, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{
							{
								Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("bba", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bba",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("bab", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bab",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("baa", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/baa",
								Entries: []*triefs.Entry{},
							},
						},
					},
					{
						Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*triefs.Entry{
							{
								Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/bbb/aaa",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("aab", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/bbb/aab",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("aba", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/bbb/aba",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("abb", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/bbb/abb",
								Entries: []*triefs.Entry{},
							},
						},
					},
				},
			},
		},
		{
			name: "two level directories with files",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa", "test_cid", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/bbb/file1.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/bba/file2.txt", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/bbb/aaa/file1.txt", "test_cid", 0, triefs.MIMEOctetStream, now),
			},
			expectedResult: &triefs.Entry{
				Content: triefs.NewContent("/", "", 0, triefs.MIMEDriveDirectory, now),
				Path:    "/",
				Entries: []*triefs.Entry{
					{
						Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/aaa",
						Entries: []*triefs.Entry{
							{
								Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bbb",
								Entries: []*triefs.Entry{},
							},
							{
								Content: triefs.NewContent("bba", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/aaa/bba",
								Entries: []*triefs.Entry{},
							},
						},
					},
					{
						Content: triefs.NewContent("bbb", "", 0, triefs.MIMEDriveDirectory, now),
						Path:    "/bbb",
						Entries: []*triefs.Entry{
							{
								Content: triefs.NewContent("aaa", "", 0, triefs.MIMEDriveDirectory, now),
								Path:    "/bbb/aaa",
								Entries: []*triefs.Entry{},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			ds := trie.Tree("/")

			if !reflect.DeepEqual(ds, tc.expectedResult) {
				t.Errorf("got %v, want %v", ds, tc.expectedResult)
			}
		})
	}
}

func TestRecursiveDelete(t *testing.T) {
	// Issues 735, PR 746
	trie := triefs.NewTrie()
	now := time.Now()
	dirs := []*triefs.Entry{
		triefs.NewEntry("/folder/f1/f2/f3/f4", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f2/f3/f4", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f/f3/f4", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f/f/f4", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f/f/f", "", 0, triefs.MIMEDriveEntry, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	entries := trie.LsRecursive("/folder")
	if len(entries) != 14 {
		t.Errorf("got %v, want %v", len(entries), 14)
	}

	for idx := len(entries) - 1; idx >= 0; idx-- {
		err := trie.Delete(triefs.JoinPath("/folder" + entries[idx].Path))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	err := trie.Delete("/folder")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list := trie.Ls("/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("got %v, want %v", len(list), 0)
	}
}

func TestDeleteCornerCase(t *testing.T) {
	// Issues 504, PR 1040
	trie := triefs.NewTrie()
	now := time.Now()
	_, err := trie.AddFile(triefs.NewEntry("/logo.png", "", 0, "image/png", now))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = trie.AddFile(triefs.NewEntry("/logo.png(1)", "", 0, "image/png", now))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = trie.Delete("/logo.png(1)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = trie.AddFile(triefs.NewEntry("/logo.png(1)", "", 0, "image/png", now))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDotLsRecursive(t *testing.T) {
	trie := triefs.NewTrie()
	now := time.Now()
	dirs := []*triefs.Entry{
		triefs.NewEntry("/.", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/./Test.txt", "fake_cid", 512, triefs.MIMEOctetStream, now),
	}

	for _, d := range dirs {
		_, err := trie.AddFile(d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	entries := trie.Ls("/")
	if len(entries) != 1 {
		t.Errorf("got %v, want %v", len(entries), 1)
	}
	if entries[0].Name != "." {
		t.Errorf("got %v, want %v", entries[0].Name, ".")
	}

	entries = trie.Ls("/.")
	if len(entries) != 1 {
		t.Errorf("got %v, want %v", len(entries), 1)
	}
	if entries[0].Name != "Test.txt" {
		t.Errorf("got %v, want %v", entries[0].Name, "Test.txt")
	}
}

func TestMkDir(t *testing.T) {
	trie := triefs.NewTrie()
	now := time.Now()
	srcPath := "Test.txt"
	file := triefs.NewEntry(srcPath, "fake_cid", 512, triefs.MIMEOctetStream, now)
	_, err := trie.AddFile(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	destPath := "/."
	dir := triefs.NewEntry(destPath, "", 0, triefs.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := trie.File(srcPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	newFile := triefs.NewEntry(triefs.JoinPath(destPath, info.Name), info.CID, info.Size, info.Type, now)
	_, err = trie.AddFile(newFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = trie.Delete(srcPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := trie.Ls("/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("got %v, want %v", len(entries), 1)
	}

	// Both directories starting with unicode char-issue #2355
	trie = triefs.NewTrie()
	dir1 := triefs.NewEntry("/Șfolder1", "", 0, triefs.MIMEDriveEntry, now)
	dir2 := triefs.NewEntry("/Țfolder2", "", 0, triefs.MIMEDriveEntry, now)
	dir3 := triefs.NewEntry("folder3 😋", "", 0, triefs.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = trie.AddFile(dir2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = trie.AddFile(dir3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(trie)
	if err != nil {
		t.Fatal(err)
	}

	trie2 := triefs.Trie{}
	err = json.Unmarshal(data, &trie2)
	if err != nil {
		t.Fatal(err)
	}

	cnts := trie2.Ls("/")
	if len(cnts) != 3 {
		t.Errorf("got %v, want %v", len(cnts), 3)
	}
	if cnts[0].Name != "Șfolder1" {
		t.Errorf("got %v, want %v", cnts[0].Name, "Șfolder1")
	}
	if cnts[1].Name != "Țfolder2" {
		t.Errorf("got %v, want %v", cnts[1].Name, "Țfolder2")
	}
	if cnts[2].Name != "folder3 😋" {
		t.Errorf("got %v, want %v", cnts[2].Name, "folder3 😋")
	}
}

func TestConflictMv(t *testing.T) {
	trie := triefs.NewTrie()
	now := time.Now()

	srcPath := "Test.txt"
	file := triefs.NewEntry(srcPath, "fake_cid", 512, triefs.MIMEOctetStream, now)
	_, err := trie.AddFile(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	destPath := "/."
	dir := triefs.NewEntry(destPath, "", 0, triefs.MIMEDriveEntry, now)
	_, err = trie.AddFile(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := trie.File(srcPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tmpFile := triefs.NewEntry(srcPath, "fake_cid", 512, triefs.MIMEOctetStream, now)
	newPath := strings.Replace(tmpFile.Path, srcPath, destPath, 1)
	newFile := triefs.NewEntry(newPath, info.CID, info.Size, info.Type, now)
	_, err = trie.AddFile(newFile)
	if err != triefs.ErrConflict {
		t.Errorf("got %v, want %v", err, triefs.ErrConflict)
	}
}

func TestStrangeMvCases(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name            string
		newName         string
		oldName         string
		dirs            []*triefs.Entry
		paths           []string
		expectedError   error
		expectedEntries int
	}{
		{
			name:    "Move folders with spaces",
			oldName: "/test",
			newName: "/test rename",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/test", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/test/some folder", "", 0, triefs.MIMEDriveEntry, now),
			},
			paths:           []string{"/test rename", "/test rename/some folder"},
			expectedEntries: 2,
		},
		{
			name:    "Move folders with spaces",
			oldName: "/test",
			newName: "/test rename",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/test", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/test/some file", "fake_cid", 512, triefs.MIMEOctetStream, now),
			},
			paths:           []string{"/test rename", "/test rename/some file"},
			expectedEntries: 2,
		},
		{
			name:    "Move from dot folder to 'dot' folder",
			oldName: "/.",
			newName: "/dot",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/.", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/./test.txt", "fake_cid", 512, triefs.MIMEOctetStream, now),
			},
			paths:           []string{"/dot", "/dot/test.txt"},
			expectedEntries: 2,
		},
		{
			name:    "Move with tricky names",
			oldName: "/.<,?!%^%!@#+_*&",
			newName: "/&^^#%@+_)!)($&%)_)(*$*(&%",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/.<,?!%^%!@#+_*&", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/.<,?!%^%!@#+_*&/////&$*@#((<>}{{{}", "fake_cid", 512, triefs.MIMEOctetStream, now),
			},
			paths:           []string{"/&^^#%@+_)!)($&%)_)(*$*(&%", "/&^^#%@+_)!)($&%)_)(*$*(&%/&$*@#((<>}{{{}"},
			expectedEntries: 2,
		},
		{
			name:    "Move file to root",
			oldName: "/Test.txt",
			newName: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/Test.txt", "fake_cid", 512, triefs.MIMEOctetStream, now),
			},
			expectedError:   triefs.ErrIllegalNameChars,
			expectedEntries: 1,
		},
		{
			name:    "Move file to double root",
			oldName: "/Test.txt",
			newName: "//",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/Test.txt", "fake_cid", 512, triefs.MIMEOctetStream, now),
			},
			expectedError:   triefs.ErrIllegalNameChars,
			expectedEntries: 1,
		},
		{
			name:    "Move folder to root",
			oldName: "/Dir2",
			newName: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/Dir2", "", 0, triefs.MIMEDriveEntry, now),
			},
			expectedError:   triefs.ErrEmptyName,
			expectedEntries: 1,
		},
		{
			name:    "Move folder to double root",
			oldName: "/Dir1",
			newName: "//",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/Dir1", "", 0, triefs.MIMEDriveEntry, now),
			},
			expectedError:   triefs.ErrEmptyName,
			expectedEntries: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					tt.Fatalf("unexpected error: %v", err)
				}
			}

			entries := trie.LsRecursive("/")
			if len(entries) != tc.expectedEntries {
				tt.Errorf("got %v, want %v", len(entries), tc.expectedEntries)
			}

			for i, e := range entries {
				typ := e.Type
				if typ == triefs.MIMEDriveDirectory {
					typ = triefs.MIMEDriveEntry
				}
				_, err := trie.AddFile(
					triefs.NewEntry(
						strings.Replace(entries[i].Path, tc.oldName, tc.newName, 1), e.CID, e.Size, typ, now))
				if tc.expectedError != nil {
					if err != tc.expectedError {
						tt.Errorf("got %v, want %v", err, tc.expectedError)
					}
					return
				}
				if err != nil {
					tt.Fatalf("unexpected error: %v", err)
				}
			}

			for idx := len(entries) - 1; idx >= 0; idx-- {
				err := trie.Delete(entries[idx].Path)
				if err != nil {
					tt.Fatalf("unexpected error: %v", err)
				}
			}

			entries = trie.LsRecursive("/")
			if len(entries) != 2 {
				tt.Errorf("got %v, want %v", len(entries), 2)
			}

			for idx, p := range entries {
				if p.Path != tc.paths[idx] {
					tt.Errorf("got %v, want %v", p.Path, tc.paths[idx])
				}
			}
		})
	}
}

func TestTrieConflict(t *testing.T) {
	now := time.Now()
	entries := []*triefs.Entry{
		triefs.NewEntry("/folder/f1/f2", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f2", "", 0, triefs.MIMEDriveEntry, now),
		triefs.NewEntry("/folder/f/f", "", 0, triefs.MIMEDriveEntry, now),
	}
	trie := triefs.NewTrie()
	for i := range entries {
		_, err := trie.AddFile(entries[i])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	_, err := trie.AddFile(triefs.NewEntry("/folder/f", "", 0, triefs.MIMEDriveEntry, now))
	if err != triefs.ErrConflict {
		t.Errorf("got %v, want %v", err, triefs.ErrConflict)
	}
}

func TestCreateRef(t *testing.T) {
	t.Parallel()
	now := time.Now()
	bucketID := "YUsvjhduiwiuZBIYUFSVGEUYDI"
	cases := []struct {
		name  string
		path  string
		dirs  []*triefs.Entry
		rdirs []*triefs.Entry
		res   []*triefs.Entry
		err   error
	}{
		{
			name:  "createRef with empty path",
			path:  "",
			rdirs: []*triefs.Entry{},
			err:   triefs.ErrEmptyPath,
		},
		{
			name:  "createRef crash test",
			path:  "/aaa/bbb/file",
			dirs:  []*triefs.Entry{},
			rdirs: []*triefs.Entry{},
			err:   triefs.ErrFileNotExist,
		},
		{
			name: "simple createRef for a file",
			path: "/aaa/bbb/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/bbb/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on first level file",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on root",
			path: "/",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			err: triefs.ErrCantCreateRef,
		},
		{
			name: "createRef on top level file",
			path: "/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", bucketID, 0, triefs.MIMEReference, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on top level dir with one entries",
			path: "/user",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/user/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/user", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/user/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on top level dir",
			path: "/aaa",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on first level dir",
			path: "/aaa/fbb",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fbb", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on trie node",
			path: "/aaa/f",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			err: triefs.ErrFileNotExist,
		},
		{
			name: "createRef on top level file",
			path: "/aca",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aba/fbb/f", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/ada/fiee/file", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aca", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aca", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef entry containing semicolon",
			path: "/aaa/file",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/file2", "test_cid", 512, triefs.MIMEOctetStream, now),
				triefs.NewEntry("/aaa/file", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/file", "test_cid", 512, triefs.MIMEOctetStream, now),
			},
		},
		{
			name: "createRef on empty dir",
			path: "/aaa/dir1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir1", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir2", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir2", "", 0, triefs.MIMEDriveEntry, now),
				triefs.NewEntry("/aaa/dir1", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/aaa/dir1", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
		{
			name: "createRef on single child",
			path: "/folder/f1",
			dirs: []*triefs.Entry{
				triefs.NewEntry("/folder/f1", "", 0, triefs.MIMEDriveEntry, now),
			},
			rdirs: []*triefs.Entry{
				triefs.NewEntry("/folder/f1", bucketID, 0, triefs.MIMEReference, now),
			},
			res: []*triefs.Entry{
				triefs.NewEntry("/folder/f1", "", 0, triefs.MIMEDriveEntry, now),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trie := triefs.NewTrie()
			for _, d := range tc.dirs {
				_, err := trie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			rtrie := triefs.NewTrie()
			for _, d := range tc.rdirs {
				_, err := rtrie.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}

			trieBucket2 := triefs.NewTrie()
			for _, d := range tc.res {
				_, err := trieBucket2.AddFile(d)
				if err != nil {
					t.Fatal(err)
				}
			}
			rtrieBucket2 := triefs.NewTrie()

			entries, err := trie.CreateRef(tc.path, bucketID, now)
			if err != nil {
				if err != tc.err {
					t.Errorf("got %v, want %v", err, tc.err)
				}
			}
			for _, d := range entries {
				if d.Type == triefs.MIMEDriveDirectory {
					d.Type = triefs.MIMEDriveEntry
				}

				_, err := rtrieBucket2.AddFile(triefs.NewEntry(d.Path, d.CID, d.Size, d.Content.Type, now))
				if err != nil {
					t.Fatal(err)
				}
			}
			ignoreCreatedAt(trie.Root, rtrie.Root)
			ignoreCreatedAt(trieBucket2.Root, trieBucket2.Root)

			if !reflect.DeepEqual(trie.Root, rtrie.Root) {
				t.Errorf("got %v, want %v", trie.Root, rtrie.Root)
			}
			if !reflect.DeepEqual(trieBucket2.Root, rtrieBucket2.Root) {
				t.Errorf("got %v, want %v", trieBucket2.Root, rtrieBucket2.Root)
			}
		})
	}
}

func TestReplace(t *testing.T) {
	trie := triefs.NewTrie()
	oldEntry := triefs.NewEntry("/home/test.txt", "cid1", 100, triefs.MIMEOctetStream, time.Now())
	_, err := trie.AddFile(oldEntry)
	if err != nil {
		t.Fatal(err)
	}
	updatedContent := triefs.NewContent("test.txt", "cid2", 1000, triefs.MIMEOctetStream, time.Now())
	_, _, err = trie.Replace("/home/test.txt", &updatedContent)
	if err != nil {
		t.Fatal(err)
	}
	cnt, err := trie.File("/home/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if cnt.Size != updatedContent.Size {
		t.Errorf("got %v, want %v", cnt.Size, updatedContent.Size)
	}
	if cnt.CID != updatedContent.CID {
		t.Errorf("got %v, want %v", cnt.CID, updatedContent.CID)
	}
}

func TestFuzzyCreateRef(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	now := time.Now()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 50000; i++ {
		trie := triefs.NewTrie()

		// paths that are stored in the trie
		paths := createRandomFiles(trie, 10)

		if i%2 == 0 {
			// pick from the inserted data
			dataIndex := r.Intn(len(paths))
			path := paths[dataIndex]
			_, _ = trie.CreateRef(path, "", now)
		} else {
			// randomly generate path to be deleted
			path := randString(r)
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
			result := triefs.CleanPath(tc.path)
			if result != tc.expected {
				t.Errorf("got %v, want %v", result, tc.expected)
			}
		})
	}
}
func ignoreCreatedAt(e1 *triefs.Entry, e2 *triefs.Entry) {
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
	trie := triefs.NewTrie()
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
		_, err := trie.AddFile(triefs.NewEntry(dir, "test-cid", 100, triefs.MIMEDriveEntry, time.Now()))
		if err != nil {
			t.Error(err)
		}
	}
	for _, file := range files {
		_, err := trie.AddFile(triefs.NewEntry(file, "test-cid", 100, triefs.MIMEOctetStream, time.Now()))
		if err != nil {
			t.Error(err)
		}
	}

	entries := trie.LsRecursive("/")
	if len(files)+len(dirs) != len(entries) {
		t.Errorf("got %v, want %v", len(files)+len(dirs), len(entries))
	}
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

	trie := triefs.NewTrie()
	total := 0
	for _, path := range paths {
		for _, e := range files[path] {
			entry := triefs.NewEntry(triefs.JoinPath(path, e), "", 0, triefs.MIMEOctetStream, time.Now())
			added, err := trie.AddFile(entry)
			if err != nil {
				t.Fatal(err)
			}
			total += len(added)
		}
	}
	if total != 16 {
		t.Errorf("got %v, want %v", total, 16)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	trie := triefs.NewTrie()
	now := time.Now()

	// Seed the trie with initial data
	_, err := trie.AddFile(triefs.NewEntry("/seed/file", "cid0", 100, triefs.MIMEOctetStream, now))
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
				path := triefs.JoinPath("/concurrent", strings.Repeat("a", id+1), strings.Repeat("b", i+1))
				entry := triefs.NewEntry(path, "cid", 64, triefs.MIMEOctetStream, now)
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
	trie := triefs.NewTrie()

	// Add a folder with a child
	folder := triefs.NewEntry("/docs", "", 0, triefs.MIMEDriveEntry, now)
	_, err := trie.AddFile(folder)
	if err != nil {
		t.Fatal(err)
	}

	child := triefs.NewEntry("/docs/readme", "cid1", 100, triefs.MIMEOctetStream, now)
	_, err = trie.AddFile(child)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot the trie state before mutation
	fileBefore, err := trie.File("/docs/readme")
	if err != nil {
		t.Fatal(err)
	}
	if fileBefore.CID != "cid1" {
		t.Errorf("got %v, want %v", fileBefore.CID, "cid1")
	}

	// Mutate the original entry that was passed to AddFile
	child.Content.CID = "corrupted"
	child.Path = "/totally/different"

	// Trie must be unaffected
	fileAfter, err := trie.File("/docs/readme")
	if err != nil {
		t.Fatal(err)
	}
	if fileAfter.CID != "cid1" {
		t.Errorf("got %v, want %v", fileAfter.CID, "cid1")
	}
}

func TestDeepCopyOnCopy(t *testing.T) {
	t.Parallel()

	now := time.Now()

	original := &triefs.Entry{
		Content: triefs.Content{
			Name:      "file",
			CID:       "cid1",
			Type:      triefs.MIMEOctetStream,
			Size:      100,
			Version:   1,
			CreatedAt: now.Unix(),
		},
		Path: "/dir/file",
		Entries: []*triefs.Entry{
			{
				Content: triefs.Content{
					Name:      "child",
					CID:       "cid2",
					Type:      triefs.MIMEOctetStream,
					Size:      50,
					Version:   1,
					CreatedAt: now.Unix(),
				},
				Path: "/dir/file/child",
			},
		},
	}

	// Use the exported Copy method
	copied := &triefs.Entry{}
	copied.Copy(original)

	// Verify the copy matches
	if copied.Content.CID != "cid1" {
		t.Errorf("got %v, want %v", copied.Content.CID, "cid1")
	}
	if copied.Entries[0].Content.CID != "cid2" {
		t.Errorf("got %v, want %v", copied.Entries[0].Content.CID, "cid2")
	}

	// Mutate the original's child
	original.Entries[0].Content.CID = "corrupted"
	original.Entries = append(original.Entries, &triefs.Entry{
		Path: "/extra",
	})

	// The copy must be unaffected
	if copied.Entries[0].Content.CID != "cid2" {
		t.Errorf("got %v, want %v", copied.Entries[0].Content.CID, "cid2")
	}
	if len(copied.Entries) != 1 {
		t.Errorf("got %v, want %v", len(copied.Entries), 1)
	}
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
			trie := triefs.NewTrie()
			for _, p := range tc.paths {
				entry := triefs.NewEntry(p, "cid", 64, triefs.MIMEOctetStream, now)
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
				if f.CID != "cid" {
					t.Errorf("got %v, want %v", f.CID, "cid")
				}
			}

			// Verify ls returns expected count
			contents := trie.Ls(tc.lsPath)
			if len(contents) != tc.wantLen {
				t.Errorf("got %v, want %v", len(contents), tc.wantLen)
			}
		})
	}
}

func TestEmojiRuneDispatch(t *testing.T) {
	t.Parallel()

	now := time.Now()
	trie := triefs.NewTrie()

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
		entry := triefs.NewEntry(p, "cid-"+p, 64, triefs.MIMEOctetStream, now)
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
		if f.CID != "cid-"+p {
			t.Errorf("got %v, want %v", f.CID, "cid-"+p)
		}
	}

	// Ls on parent must list all four children
	contents := trie.Ls("/parent")
	if len(contents) != len(paths) {
		t.Errorf("got %v, want %v", len(contents), len(paths))
	}
}

func TestUTF8Paths(t *testing.T) {
	t.Parallel()
	now := time.Now()

	t.Run("CJK folder and file names", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/中文/文件.txt",
			"/中文/子目录/深层文件.dat",
			"/日本語/ファイル.txt",
			"/한국어/파일.txt",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 100, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		// File retrieval
		for _, p := range paths {
			f, err := trie.File(p)
			if err != nil {
				t.Fatalf("File(%q) failed: %v", p, err)
			}
			if f.CID != "cid-"+p {
				t.Errorf("File(%q).CID = %q, want %q", p, f.CID, "cid-"+p)
			}
		}

		// Ls root should list the three top-level CJK directories
		rootContents := trie.Ls("/")
		if len(rootContents) != 3 {
			t.Errorf("Ls(\"/\") returned %d entries, want 3", len(rootContents))
		}

		// Ls into CJK directory
		zhContents := trie.Ls("/中文")
		if len(zhContents) != 2 {
			t.Errorf("Ls(\"/中文\") returned %d entries, want 2", len(zhContents))
		}

		// Stat on CJK path
		st, err := trie.Stat("/日本語/ファイル.txt")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if st.CID != "cid-/日本語/ファイル.txt" {
			t.Errorf("Stat CID = %q, want %q", st.CID, "cid-/日本語/ファイル.txt")
		}
	})

	t.Run("Emoji paths including ZWJ sequences", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/emoji/😀.txt",
			"/emoji/😁.txt",
			"/emoji/👨\u200d👩\u200d👧.txt",
			"/emoji/🚀rocket.log",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 50, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		for _, p := range paths {
			f, err := trie.File(p)
			if err != nil {
				t.Fatalf("File(%q) failed: %v", p, err)
			}
			if f.CID != "cid-"+p {
				t.Errorf("File(%q).CID = %q, want %q", p, f.CID, "cid-"+p)
			}
		}

		contents := trie.Ls("/emoji")
		if len(contents) != 4 {
			t.Errorf("Ls(\"/emoji\") returned %d entries, want 4", len(contents))
		}
	})

	t.Run("Mixed scripts in single path", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/docs/café-résumé.pdf",
			"/docs/日本-report-🚀.txt",
			"/data/München/Ölpreis.csv",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 200, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		for _, p := range paths {
			f, err := trie.File(p)
			if err != nil {
				t.Fatalf("File(%q) failed: %v", p, err)
			}
			if f.CID != "cid-"+p {
				t.Errorf("File(%q).CID = %q, want %q", p, f.CID, "cid-"+p)
			}
		}
	})

	t.Run("Delete UTF-8 paths", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		entry1 := triefs.NewEntry("/フォルダ/ファイル.txt", "cid1", 10, triefs.MIMEOctetStream, now)
		_, err := trie.AddFile(entry1)
		if err != nil {
			t.Fatalf("AddFile failed: %v", err)
		}

		entry2 := triefs.NewEntry("/フォルダ/別のファイル.txt", "cid2", 20, triefs.MIMEOctetStream, now)
		_, err = trie.AddFile(entry2)
		if err != nil {
			t.Fatalf("AddFile failed: %v", err)
		}

		err = trie.Delete("/フォルダ/ファイル.txt")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = trie.File("/フォルダ/ファイル.txt")
		if err == nil {
			t.Error("File should return error after deletion")
		}

		f, err := trie.File("/フォルダ/別のファイル.txt")
		if err != nil {
			t.Fatalf("File for remaining entry failed: %v", err)
		}
		if f.CID != "cid2" {
			t.Errorf("remaining file CID = %q, want %q", f.CID, "cid2")
		}
	})

	t.Run("LsRecursive with UTF-8", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/根目录/子目录/文件.txt",
			"/根目录/子目录/另一个.dat",
			"/根目录/直接文件.log",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 30, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		entries := trie.LsRecursive("/根目录")
		// Should include: 子目录 (dir), 文件.txt, 另一个.dat, 直接文件.log
		if len(entries) < 3 {
			t.Errorf("LsRecursive returned %d entries, want at least 3", len(entries))
		}
	})

	t.Run("Tree with UTF-8", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/ルート/サブ/ファイル.txt",
			"/ルート/別.dat",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 40, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		tree := trie.Tree("/")
		if tree == nil {
			t.Fatal("Tree returned nil")
		}
		if len(tree.Entries) == 0 {
			t.Error("Tree root has no entries")
		}
	})

	t.Run("JSON marshal/unmarshal roundtrip", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		paths := []string{
			"/données/café.txt",
			"/数据/文件.bin",
			"/🎉/party.txt",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 60, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		data, err := json.Marshal(trie)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		trie2 := triefs.NewTrie()
		err = json.Unmarshal(data, trie2)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		for _, p := range paths {
			f, err := trie2.File(p)
			if err != nil {
				t.Fatalf("After roundtrip, File(%q) failed: %v", p, err)
			}
			if f.CID != "cid-"+p {
				t.Errorf("After roundtrip, File(%q).CID = %q, want %q", p, f.CID, "cid-"+p)
			}
		}
	})

	t.Run("Trie splitting at multi-byte rune boundaries", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		// These paths share a common prefix that includes multi-byte chars,
		// forcing commonPrefix to split correctly at rune boundaries.
		paths := []string{
			"/共有プレフィックス/ブランチA.txt",
			"/共有プレフィックス/ブランチB.txt",
		}

		for _, p := range paths {
			entry := triefs.NewEntry(p, "cid-"+p, 70, triefs.MIMEOctetStream, now)
			_, err := trie.AddFile(entry)
			if err != nil {
				t.Fatalf("AddFile(%q) failed: %v", p, err)
			}
		}

		for _, p := range paths {
			f, err := trie.File(p)
			if err != nil {
				t.Fatalf("File(%q) failed: %v", p, err)
			}
			if f.CID != "cid-"+p {
				t.Errorf("File(%q).CID = %q, want %q", p, f.CID, "cid-"+p)
			}
		}

		// Both should appear under Ls of shared prefix dir
		contents := trie.Ls("/共有プレフィックス")
		if len(contents) != 2 {
			t.Errorf("Ls returned %d entries, want 2", len(contents))
		}
	})

	t.Run("Invalid UTF-8 rejected by Validate", func(t *testing.T) {
		t.Parallel()
		trie := triefs.NewTrie()

		// Create an entry with invalid UTF-8 bytes in the path
		entry := triefs.NewEntry("/bad/\xff\xfe.txt", "cid", 10, triefs.MIMEOctetStream, now)
		_, err := trie.AddFile(entry)
		if err == nil {
			t.Fatal("expected error for invalid UTF-8 path, got nil")
		}
	})
}
