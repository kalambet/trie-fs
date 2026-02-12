// Package filesystem contains implementation of helper methods and types to work with user file system
package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	protov1 "imploy/lib/files/proto/v1"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	// MIMEDriveDirectory mime type used only for return values, directory is completely ephemeral
	MIMEDriveDirectory = "application/chainsafe-files-directory"
	// MIMEDriveEntry mime type that describes internal trie node
	MIMEDriveEntry = "application/chainsafe-files-entry"
	// MIMEOctetStream is a copy of mime type from Echo so we do not need Echo dependency for this Package
	MIMEOctetStream = "application/octet-stream"
	// MIMEReference is mime type from that is referenced to another filesystem
	MIMEReference = "application/chainsafe-files-reference"

	// SpecialPathSymbol the only symbol you can't use in paths or names
	SpecialPathSymbol = ":"
	// Separator is a separator for filesystem
	Separator = "/"
	// SeparatorRune is a rune that contains the separator for filesystem
	SeparatorRune = '/'
	// DoubleSeparator is a constant that represents filesystem
	// separator double symbol and used to clean up the paths
	DoubleSeparator = "//"
)

var (
	// ErrConflict error the means such file or dir already exist
	ErrConflict = errors.New("conflict, entry can't be added")
	// ErrEmptyPath mean the passed path is empty
	ErrEmptyPath = errors.New("paths can't be empty")
	// ErrEmptyName mean the passed name is empty
	ErrEmptyName = errors.New("names can't be empty")
	// ErrIllegalPathChars means that passed path has illegal characters
	ErrIllegalPathChars = errors.New("semicolon and multiple consequent slashes in path are not allowed")
	// ErrIllegalNameChars means that passed name has illegal characters
	ErrIllegalNameChars = errors.New("semicolon or slashes in name are not allowed")
	// ErrCantAddDirectory means that someone tried to add directory directly which is not possible
	// use MIMEDriveEntry placeholder instead
	ErrCantAddDirectory = errors.New("directories are ephemeral for placeholder use Entity content type")
	// ErrFileNotExist returned in case requested file is not exist
	ErrFileNotExist = errors.New("file doesn't exist")
	// ErrCantCreateRef returned if provided path for createRef is root
	ErrCantCreateRef = errors.New("cannot create reference on root")
)

// Entry describes the trie node structure, if Entries length slice is zero - it's a leaf
type Entry struct {
	Content
	Path    string   `json:"path"`
	Entries []*Entry `json:"entries"`
	Meta    *Meta    `json:"meta,omitempty"`
}

// FromProto converts protobuf to Entry
func (entry *Entry) FromProto(protoTree *protov1.FilesystemEntry) {
	if protoTree == nil {
		return
	}

	entry.Content = Content{
		Name:      protoTree.Name,
		CID:       protoTree.Cid,
		Type:      protoTree.ContentType,
		Size:      protoTree.Size,
		CreatedAt: protoTree.CreatedAt,
	}
	if len(protoTree.Version) > 0 {
		entry.Content.Version = protoTree.Version[0]
	}
	entry.Path = protoTree.Path

	entry.Entries = make([]*Entry, len(protoTree.Entries))
	for i, protoEntry := range protoTree.Entries {
		entry.Entries[i] = &Entry{}
		entry.Entries[i].FromProto(protoEntry)
	}
}

// ToProto converts Entry to protobuf
func (entry *Entry) ToProto() *protov1.FilesystemEntry {
	protoTree := &protov1.FilesystemEntry{
		Name:        entry.Name,
		Cid:         entry.CID,
		ContentType: entry.Type,
		Size:        entry.Size,
		CreatedAt:   entry.CreatedAt,
		Version:     []byte{entry.Version},
		Path:        entry.Path,
	}

	protoTree.Entries = make([]*protov1.FilesystemEntry, len(entry.Entries))
	for i, en := range entry.Entries {
		protoTree.Entries[i] = en.ToProto()
	}
	return protoTree
}

// Meta holds some extra fields for entry
type Meta struct {
	FailureCode     int    `json:"failure_code"`
	FailedMessage   string `json:"failed_message"`
	SuggestedAction string `json:"suggested_action"`
}

// NewEntry creates new instance of Entry
func NewEntry(path string, cid string, size int64, contentType string, createdAt time.Time) *Entry {
	me := &Entry{
		Path:    path,
		Content: NewContent(filepath.Base(path), cid, size, contentType, createdAt),
	}

	if contentType == MIMEDriveEntry {
		me.Name = ""
		me.Path = SpecialPathSymbol
		me.Version = 0

		return &Entry{
			Content: Content{
				Type:      MIMEDriveEntry,
				CreatedAt: createdAt.Unix(),
			},
			Path:    path,
			Entries: []*Entry{me},
		}
	}
	return me
}

// AddMeta adds meta field to entry
func (entry *Entry) AddMeta(failureCode int, failedMsg, suggestion string) {
	entry.Meta = &Meta{
		FailureCode:     failureCode,
		FailedMessage:   failedMsg,
		SuggestedAction: suggestion,
	}
}

// SetCreatedAt sets CreatedAt field of content of a entry
func (entry *Entry) SetCreatedAt(t int64) {
	entry.Content.CreatedAt = t
}

// Validate preforms validate on Entry so it ready for traversal algos
func (entry *Entry) Validate() error {
	if entry.Content.Type == MIMEDriveDirectory {
		return ErrCantAddDirectory
	}

	// Because filepath.Base() is used, empty path leads to '.' value in method result
	if entry.Name == "." && len(entry.Path) == 0 {
		return ErrEmptyPath
	}
	if strings.Contains(entry.Path, SpecialPathSymbol) {
		return ErrIllegalPathChars
	}
	if entry.IsEmptyFolder() && len(CleanPath(entry.Path)) == 0 {
		return ErrEmptyName
	}
	return entry.Content.Validate()
}

// IsEmptyFolder checks if provided Entry is a placeholder for empty folder
func (entry *Entry) IsEmptyFolder() bool {
	return entry.Type == MIMEDriveEntry && len(entry.Entries) == 1 && entry.Entries[0].Path == SpecialPathSymbol
}

// Copy creates a deep copy of m into entry
func (entry *Entry) Copy(m *Entry) {
	entry.Content = m.Content
	entry.Path = m.Path
	entry.Meta = m.Meta.copy()
	entry.Entries = copyEntries(m.Entries)
}

func (entry *Entry) copy() *Entry {
	return &Entry{
		Content: entry.Content,
		Path:    entry.Path,
		Meta:    entry.Meta.copy(),
		Entries: copyEntries(entry.Entries),
	}
}

func copyEntries(entries []*Entry) []*Entry {
	if entries == nil {
		return nil
	}
	cp := make([]*Entry, len(entries))
	for i, e := range entries {
		cp[i] = e.copy()
	}
	return cp
}

func (m *Meta) copy() *Meta {
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}

func (entry *Entry) trimPrefix(prefix string) string {
	entry.Path = strings.TrimPrefix(entry.Path, prefix)
	if len(entry.Path) == 0 {
		entry.Path = SpecialPathSymbol
	}
	return entry.Path
}

// Content describes metadata content that going to be associated with the user's file
type Content struct {
	Name    string `json:"name"`
	CID     string `json:"cid"`
	Type    string `json:"content_type"`
	Size    int64  `json:"size"`
	Version byte   `json:"version"`
	// Behaves like "ModifiedAt" for now but the name
	// was preserved for backward compatibility
	CreatedAt int64 `json:"created_at"`
}

// NewContent creates new instance of a content, in case of Directory
// it omits everything except Name and Type
func NewContent(name string, cid string, size int64, t string, createdAt time.Time) Content {
	if t == MIMEDriveEntry {
		return Content{
			Type:      t,
			CreatedAt: createdAt.Unix(),
		}
	}

	var version byte = 1
	if t == MIMEDriveDirectory {
		version = 0
	}

	return Content{
		Name:      name,
		CID:       cid,
		Type:      t,
		Size:      size,
		Version:   version,
		CreatedAt: createdAt.Unix(),
	}
}

// Validate checks if existing Content eligible to be part of trie node
func (c *Content) Validate() error {
	if strings.Contains(c.Name, Separator) || strings.Contains(c.Name, SpecialPathSymbol) {
		return ErrIllegalNameChars
	}

	if len(c.Type) == 0 {
		c.Type = MIMEOctetStream
	}

	return nil
}

// IsDirectory returns whether a Content is directory or not
func (c *Content) IsDirectory() bool {
	return c.Type == MIMEDriveDirectory || c.Type == MIMEDriveEntry
}

func directoriesFromContents(path string, contents []*Content) []*Entry {
	dirs := make([]*Entry, 0)
	for _, content := range contents {
		if content.Type == MIMEDriveDirectory || content.Type == MIMEDriveEntry {
			dirs = append(dirs, NewEntry(JoinPath(path, content.Name), "", content.Size, content.Type, time.Unix(content.CreatedAt, 0)))
		}
	}
	return dirs
}

func (c *Content) copy() *Content {
	return &Content{
		Name:      c.Name,
		CID:       c.CID,
		Type:      c.Type,
		Size:      c.Size,
		Version:   c.Version,
		CreatedAt: c.CreatedAt,
	}
}

// Trie is the structure behind
type Trie struct {
	Root    *Entry          `json:"root"`
	Context context.Context `json:"-"`
	lock    sync.RWMutex
}

// FromProto converts protobuf representation to Trie
func (mt *Trie) FromProto(protoTrie *protov1.Trie) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	mt.Root = &Entry{}
	mt.Root.FromProto(protoTrie.Root)
}

// ToProto converts Trie to protobuf representation
func (mt *Trie) ToProto() protov1.Trie {
	if mt == nil {
		return protov1.Trie{}
	}

	mt.lock.RLock()
	defer mt.lock.RUnlock()

	if mt.Root == nil {
		return protov1.Trie{}
	}

	return protov1.Trie{
		Root: mt.Root.ToProto(),
	}
}

// NewTrie creates new instance of user's file system trie
func NewTrie() *Trie {
	return &Trie{
		lock: sync.RWMutex{},
	}
}

// Hash return the hash for the filesystem
func (mt *Trie) Hash() (string, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(mt)
	if err != nil {
		return "", err
	}

	hashFunc := sha256.New()
	hashFunc.Write(buf.Bytes())
	return fmt.Sprintf("%x", hashFunc.Sum(nil)), nil
}

// NewTrieWithContext creates new instance of user's file system trie with context
func NewTrieWithContext(ctx context.Context) *Trie {
	return &Trie{
		Context: ctx,
		lock:    sync.RWMutex{},
	}
}

// AddFile add new node to the tire
func (mt *Trie) AddFile(m *Entry) ([]*Entry, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	stop := trace(mt.Context, "filesystem.add")
	defer stop()

	if m == nil {
		return nil, ErrConflict
	}

	err := m.Validate()
	if err != nil {
		return nil, err
	}

	m.Path = CleanPath(m.Path)
	if mt.Root == nil {
		mt.Root = m
		return mt.lsRecursive("/"), nil
	}
	return addTo(mt.Root, m.copy())
}

// Ls lists passed directory paths. All returned directories are ephemeral
// they are not part of the trie
func (mt *Trie) Ls(path string) []*Content {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	stop := trace(mt.Context, "filesystem.ls")
	defer stop()

	if mt.Root == nil {
		return []*Content{}
	}

	p := CleanPath(path)
	return list(p, mt.Root)
}

// Tree returns the complete directory structure of trie.
func (mt *Trie) Tree(path string) *Entry {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	stop := trace(mt.Context, "filesystem.tree")
	defer stop()

	p := CleanPath(path)
	var t *Entry
	if p == "" {
		t = NewEntry("/", "", 0, MIMEDriveDirectory, time.Now())
	} else {
		t = NewEntry(p, "", 0, MIMEDriveDirectory, time.Now())
	}

	if mt.Root == nil {
		return t
	}

	return tree(t, p, mt.Root)
}

// LsRecursive lists passed directory and sub directory paths.
// Returned lists contains directories first and then their sub-dir/files.
// For adding entry from this list traverse it from first to last and for
// deletion traverse from last to first
func (mt *Trie) LsRecursive(path string) []*Entry {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	return mt.lsRecursive(path)
}

// lsRecursive is the lock-free core of LsRecursive.
// Callers must hold at least a read lock.
func (mt *Trie) lsRecursive(path string) []*Entry {
	stop := trace(mt.Context, "filesystem.ls-recursive")
	defer stop()

	if mt.Root == nil {
		return []*Entry{}
	}

	p := CleanPath(path)
	entries := listRecursive(p, p, mt.Root)
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	return entries
}

// File gets associated metadata by path
// returns data for existing file and empty folder
func (mt *Trie) File(path string) (*Content, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	stop := trace(mt.Context, "filesystem.file")
	defer stop()

	if len(path) == 0 {
		return nil, ErrEmptyPath
	}

	if mt.Root == nil {
		return nil, ErrFileNotExist
	}

	p := CleanPath(path)
	f := find(p, mt.Root)
	if f == nil {
		return nil, ErrFileNotExist
	}

	return f.copy(), nil
}

// Stat is similar to File. In addition, it also  returns non-empty directory
func (mt *Trie) Stat(path string) (*Content, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	stop := trace(mt.Context, "filesystem.stat")
	defer stop()

	if len(path) == 0 {
		return nil, ErrEmptyPath
	}

	if mt.Root == nil || path == Separator {
		return nil, ErrFileNotExist
	}

	p := CleanPath(path)
	f := stat(p, mt.Root)
	if f == nil {
		return nil, ErrFileNotExist
	}

	name := filepath.Base(path)
	cnt := f.copy()
	cnt.Name = name
	return cnt, nil
}

// Replace replaces contents of a path.
func (mt *Trie) Replace(path string, cnt *Content) (*Content, *Content, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	stop := trace(mt.Context, "filesystem.replace")
	defer stop()

	if len(path) == 0 {
		return nil, nil, ErrEmptyPath
	}

	if mt.Root == nil {
		return nil, nil, ErrFileNotExist
	}

	p := CleanPath(path)
	f := find(p, mt.Root)
	if f == nil {
		return nil, nil, ErrFileNotExist
	}
	old := f.copy()
	f.CID = cnt.CID
	f.Size = cnt.Size
	f.CreatedAt = cnt.CreatedAt
	return cnt.copy(), old.copy(), nil
}

// Delete deletes associated file system entry by path
func (mt *Trie) Delete(path string) error {
	stop := trace(mt.Context, "filesystem.delete")
	defer stop()

	mt.lock.Lock()
	defer mt.lock.Unlock()

	if len(path) == 0 {
		return ErrEmptyPath
	}

	if mt.Root == nil {
		return nil
	}

	p := CleanPath(path)
	item := rm(p, mt.Root)
	if item != nil {
		mt.Root = nil
	}

	return nil
}

// CreateRef creates ref for file
func (mt *Trie) CreateRef(path string, bucketID string, createdAt time.Time) ([]*Entry, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	stop := trace(mt.Context, "filesystem.create-ref")
	defer stop()

	if len(path) == 0 {
		return nil, ErrEmptyPath
	}
	if path == Separator {
		return nil, ErrCantCreateRef
	}
	if mt.Root == nil {
		return nil, ErrFileNotExist
	}

	p := CleanPath(path)
	entries, err := createRef(p, bucketID, mt, createdAt)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func createRef(path string, bucketID string, trie *Trie, createdAt time.Time) ([]*Entry, error) {
	entries := make([]*Entry, 0)
	// check the path if it is file
	f := find(path, trie.Root)
	if f != nil {
		entries = append(entries, &Entry{
			Content: *f.copy(),
			Path:    "",
			Entries: nil,
		})
	} else {
		e := listRecursive(path, path, trie.Root)
		if len(e) == 0 {
			return nil, ErrFileNotExist
		}
		entries = append(entries, &Entry{
			Content: NewContent("", "", 0, MIMEDriveEntry, createdAt),
			Path:    "",
		})
		entries = append(entries, e...)
	}

	if len(entries) == 0 {
		return nil, ErrFileNotExist
	}

	// remove entries from filesystem
	for i := len(entries) - 1; i >= 0; i-- {
		entries[i].Path = JoinPath(path, entries[i].Path)
		res := rm(entries[i].Path, trie.Root)
		if res != nil {
			trie.Root = nil
		}
	}
	refEntry := NewEntry(path, bucketID, 0, MIMEReference, createdAt)
	err := refEntry.Validate()
	if err != nil {
		return nil, err
	}
	if trie.Root == nil {
		trie.Root = refEntry
		return entries, nil
	}
	// add a reference entry to this filesystem
	_, err = addTo(trie.Root, refEntry)
	return entries, err
}

// CleanPath remove duplicate Separator symbols,
// adds missing in front, ignores last
func CleanPath(path string) string {
	pl := len(path)
	if pl == 0 {
		return path
	}

	for {
		if strings.Contains(path, DoubleSeparator) {
			path = strings.ReplaceAll(path, DoubleSeparator, Separator)
		} else {
			break
		}
	}

	if path[0] != SeparatorRune {
		path = Separator + path
	}

	pl = len(path)
	if pl > 1 && path[pl-1] == SeparatorRune {
		path = path[:pl-1]
	}

	return path
}

// JoinPath joins provided path segments into one path
func JoinPath(paths ...string) string {
	return CleanPath(strings.Join(paths, Separator))
}

// commonPrefix returns the longest common prefix of a and b,
// always cutting at a valid UTF-8 rune boundary so multi-byte
// characters (e.g. emoji) are never split.
func commonPrefix(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	// If we stopped in the middle of a multi-byte rune,
	// back up to its leading byte. Since a[:i] excludes a[i],
	// this drops the entire partially-matched rune.
	for i > 0 && i < len(a) && !utf8.RuneStart(a[i]) {
		i--
	}
	return a[:i]
}

func addTo(subtrie *Entry, what *Entry) ([]*Entry, error) {
	if strings.HasPrefix(subtrie.Path, what.Path) {
		suf := strings.TrimPrefix(subtrie.Path, what.Path)
		if len(suf) == 0 {
			return fixEntries([]*Entry{what.copy()}, ""), extend(subtrie, what)
		} else if suf[0] == SeparatorRune {
			return nil, ErrConflict
		}
		return fixEntries([]*Entry{what.copy()}, ""), split(what.Path, subtrie, what, true)
	}

	if strings.HasPrefix(what.Path, subtrie.Path) {
		what.trimPrefix(subtrie.Path)
		// If leaf
		if len(subtrie.Entries) == 0 {
			if len(what.Path) == 0 || what.Path[0] == SeparatorRune {
				if subtrie.Content.Type == MIMEDriveEntry {
					subtrie.Path += what.Path
					subtrie.Content = what.Content
					if len(what.Path) == 0 {
						return fixEntries([]*Entry{what.copy()}, subtrie.Path), nil
					}
					return fixEntries(splitEntry(what), subtrie.Path), nil
				}
				return nil, ErrConflict
			}
			// what's path is already trimmed.
			return fixEntries(splitEntry(what), subtrie.Path), split(subtrie.Path, subtrie, what, false)
		}

		whatRune, _ := utf8.DecodeRuneInString(what.Path)
		for _, me := range subtrie.Entries {
			meRune, _ := utf8.DecodeRuneInString(me.Path)
			if meRune != whatRune {
				continue
			}
			entries, err := addTo(me, what)
			return fixEntries(entries, subtrie.Path), err
		}
		return fixEntries(splitEntry(what), subtrie.Path), add(subtrie, what)
	}
	subprefix := commonPrefix(subtrie.Path, what.Path)
	temp := &Entry{Content: what.Content, Path: strings.TrimPrefix(what.Path, subprefix)}
	entries := splitEntry(temp)
	if temp.Path[0] == SeparatorRune {
		entries = append(entries, NewEntry("", "", 0, MIMEDriveDirectory, time.Now()))
	}
	return fixEntries(entries, subprefix), split(subprefix, subtrie, what, true)
}

// nolint:unparam
// to keep consistency with extend, add and others function return error though it always returns nil
func split(subprefix string, me *Entry, what *Entry, trimPath bool) error {
	newEntry := Entry{
		Entries: me.Entries,
		Path:    me.trimPrefix(subprefix),
		Content: me.Content,
	}

	if what.IsEmptyFolder() && what.Path == subprefix {
		me.Copy(what)
		me.Entries = append(me.Entries, &newEntry)
		return nil
	}

	if trimPath {
		what.trimPrefix(subprefix)
	}
	me.Content = NewContent("", "", 0, MIMEDriveEntry, time.Unix(me.Content.CreatedAt, 0))
	me.Path = subprefix
	me.Entries = []*Entry{
		&newEntry,
		what,
	}

	return nil
}

func extend(subtrie *Entry, what *Entry) error {
	if subtrie.Content.Type != MIMEDriveEntry {
		return ErrConflict
	}

	for _, me := range subtrie.Entries {
		if me.Path == SpecialPathSymbol || me.Path[0] == SeparatorRune {
			return ErrConflict
		}
	}

	what.Path = SpecialPathSymbol
	subtrie.Entries = append(subtrie.Entries, what)
	return nil
}

func add(subtrie *Entry, what *Entry) error {
	if subtrie.Content.Type != MIMEDriveEntry {
		return ErrConflict
	}

	// if it's child of a directory replace otherwise just add it to trie
	if what.Path[0] == SeparatorRune {
		if subtrie.IsEmptyFolder() {
			subtrie.Path += what.Path
			subtrie.Content = what.Content
			if what.Type != MIMEDriveEntry {
				subtrie.Entries = nil
			}
			return nil
		}

		for _, me := range subtrie.Entries {
			if me.Path == SpecialPathSymbol {
				if me.Content.Type != MIMEDriveEntry {
					return ErrConflict
				}
				me.Copy(what)
				return nil
			}
		}
	}

	subtrie.Entries = append(subtrie.Entries, what)
	return nil
}

func list(path string, subtrie *Entry) []*Content {
	res := make([]*Content, 0)

	if strings.HasPrefix(subtrie.Path, path) && subtrie.Path != path {
		suffix := strings.TrimPrefix(subtrie.Path, path)
		if len(suffix) != 0 && suffix[0] == SeparatorRune {
			return collect(path, "", subtrie)
		}
		return nil
	}

	if strings.HasPrefix(path, subtrie.Path) {
		if len(subtrie.Entries) == 0 {
			return nil
		}

		suffix := strings.TrimPrefix(path, subtrie.Path)
		for _, me := range subtrie.Entries {
			res = append(res, list(suffix, me)...)
		}
	}

	return res
}

func tree(dir *Entry, _path string, subtrie *Entry) *Entry {
	entries := list(_path, subtrie)
	if len(dir.Entries) == 0 {
		dir.Entries = make([]*Entry, 0)
	}
	dir.Entries = append(dir.Entries, directoriesFromContents(_path, entries)...)
	for i, c := range dir.Entries {
		if c.Type == MIMEDriveDirectory || c.Type == MIMEDriveEntry {
			tree(dir.Entries[i], JoinPath(_path, c.Name), subtrie)
		}
	}
	return dir
}

func listRecursive(_path string, fixedPath string, subtrie *Entry) []*Entry {
	entries := list(_path, subtrie)
	res := make([]*Entry, 0)
	for _, c := range entries {
		res = append(res, &Entry{Content: *c, Path: strings.TrimPrefix(JoinPath(_path, c.Name), fixedPath)})
		if c.Type == MIMEDriveDirectory {
			chldrn := listRecursive(JoinPath(_path, c.Name), fixedPath, subtrie)
			res = append(res, chldrn...)
		}
	}
	return res
}

func collect(prefix string, fullname string, subtrie *Entry) []*Content {
	if subtrie.Path == SpecialPathSymbol {
		if subtrie.Type == MIMEDriveEntry {
			cnt := NewContent(fullname, "", 0, MIMEDriveDirectory, time.Unix(subtrie.Content.CreatedAt, 0))
			return []*Content{&cnt}
		}
		return []*Content{&subtrie.Content}
	}

	suffix := subtrie.Path
	if len(prefix) != 0 {
		suffix = strings.TrimPrefix(suffix, prefix)
	}

	if len(suffix) != 0 && suffix[0] == SeparatorRune {
		suffix = suffix[1:]
	}

	slashIdx := strings.Index(suffix, Separator)
	if slashIdx > 0 {
		fullname += suffix[0:slashIdx]
		cnt := NewContent(fullname, "", 0, MIMEDriveDirectory, time.Unix(subtrie.Content.CreatedAt, 0))
		return []*Content{&cnt}
	}

	if slashIdx == 0 {
		return nil
	}

	fullname += suffix
	if len(subtrie.Entries) == 0 {
		if subtrie.Content.Type == MIMEDriveEntry {
			name := fullname + subtrie.Content.Name
			if subtrie.Content.Name == fullname {
				name = subtrie.Content.Name
			}
			cnt := NewContent(name, "", 0, MIMEDriveDirectory, time.Unix(subtrie.Content.CreatedAt, 0))
			return []*Content{&cnt}
		}
		return []*Content{&subtrie.Content}
	}

	res := make([]*Content, 0)
	for _, me := range subtrie.Entries {
		if len(me.Path) != 0 && me.Path[0] == SeparatorRune {
			// If child starts with slash then is means that we
			// are reached the end of parent directory name
			cnt := NewContent(fullname, "", 0, MIMEDriveDirectory, time.Unix(me.CreatedAt, 0))
			res = append(res, &cnt)
			continue
		}
		res = append(res, collect("", fullname, me)...)
	}

	return res
}

func rm(subprefix string, subtrie *Entry) *Entry {
	subprefix = strings.TrimPrefix(subprefix, subtrie.Path)

	if len(subprefix) == 0 {
		if subtrie.Content.Type != MIMEDriveEntry || subtrie.IsEmptyFolder() {
			return deleteOrConvert(subtrie)
		}
	}

	for i, me := range subtrie.Entries {
		if len(subprefix) == 0 {
			if me.Path == SpecialPathSymbol {
				return removeAndMerge(subtrie, i)
			}
			continue
		}
		if strings.HasPrefix(subprefix, me.Path) {
			found := rm(subprefix, me)
			if found != nil {
				return removeAndMerge(subtrie, i)
			}
			return nil
		}
		if strings.HasPrefix(me.Path, subprefix) {
			return nil
		}
	}
	return nil
}

func removeAndMerge(subtrie *Entry, idx int) *Entry {
	if len(subtrie.Entries) <= 1 {
		return subtrie
	}
	if idx == (len(subtrie.Entries) - 1) {
		subtrie.Entries = subtrie.Entries[:idx]
	} else {
		subtrie.Entries = append(subtrie.Entries[:idx], subtrie.Entries[idx+1:]...)
	}

	if len(subtrie.Entries) == 1 {
		// We need to merge
		subtrie.Content = subtrie.Entries[0].Content
		if subtrie.Entries[0].Path != SpecialPathSymbol {
			subtrie.Path += subtrie.Entries[0].Path
			subtrie.Entries = subtrie.Entries[0].Entries
		} else if subtrie.Type != MIMEDriveEntry {
			subtrie.Entries = nil
		}
	}
	return nil
}

func deleteOrConvert(md *Entry) *Entry {
	idx := strings.LastIndex(md.Path, Separator)
	if idx <= 0 {
		return md
	}

	md.Copy(NewEntry(md.Path[:idx], "", 0, MIMEDriveEntry, time.Unix(md.CreatedAt, 0)))
	return nil
}

func find(subprefix string, subtrie *Entry) *Content {
	subprefix = strings.TrimPrefix(subprefix, subtrie.Path)

	if len(subprefix) == 0 {
		if subtrie.Content.Type != MIMEDriveEntry {
			return &subtrie.Content
		}
		if subtrie.IsEmptyFolder() {
			cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
			return updateFolderEntry(&cnt, subtrie)
		}
	}

	for _, me := range subtrie.Entries {
		if len(subprefix) == 0 {
			if me.Path == SpecialPathSymbol {
				if me.Type != MIMEDriveEntry {
					return &me.Content
				}

				cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
				return updateFolderEntry(&cnt, subtrie)
			}
			continue
		}
		if strings.HasPrefix(subprefix, me.Path) {
			item := find(subprefix, me)
			if item != nil {
				return updateFolderEntry(item, subtrie)
			}
			return nil
		}
		if strings.HasPrefix(me.Path, subprefix) {
			return nil
		}
	}

	return nil
}

func stat(subprefix string, subtrie *Entry) *Content {
	if strings.HasPrefix(subprefix, subtrie.Path) {
		subprefix = strings.TrimPrefix(subprefix, subtrie.Path)
		if len(subprefix) == 0 {
			if subtrie.Content.Type != MIMEDriveEntry {
				return &subtrie.Content
			}
			if subtrie.IsEmptyFolder() {
				cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
				return &cnt
			}
		}
		for _, me := range subtrie.Entries {
			if len(subprefix) == 0 {
				if me.Path == SpecialPathSymbol {
					if me.Type != MIMEDriveEntry {
						return &me.Content
					}
					cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
					return &cnt
				}
				if me.Path[0] == SeparatorRune {
					cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
					return &cnt
				}
				continue
			}
			item := stat(subprefix, me)
			if item != nil {
				return item
			}
		}
	} else if strings.HasPrefix(subtrie.Path, subprefix) {
		subprefix = strings.TrimPrefix(subtrie.Path, subprefix)
		if len(subprefix) == 0 {
			if subtrie.Content.Type != MIMEDriveEntry {
				return &subtrie.Content
			}
			if subtrie.IsEmptyFolder() {
				cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
				return &cnt
			}
		}
		if subprefix[0] == SeparatorRune {
			cnt := NewContent("", "", 0, MIMEDriveDirectory, time.Unix(subtrie.CreatedAt, 0))
			return &cnt
		}
	}
	return nil
}

func updateFolderEntry(cnt *Content, me *Entry) *Content {
	if cnt.Type != MIMEDriveDirectory {
		return cnt
	}

	idx := strings.LastIndex(me.Path, Separator)
	if idx < 0 {
		cnt.Name = me.Path + cnt.Name
	} else {
		cnt.Name = me.Path[idx+1:] + cnt.Name
	}

	return cnt
}

func splitEntry(entry *Entry) []*Entry {
	path := entry.Path
	paths := strings.Split(path, Separator)
	currentPath := ""
	entries := make([]*Entry, 0)
	for i, p := range paths {
		if p == "" {
			continue
		}
		if i == 0 {
			currentPath = p
		} else {
			currentPath = currentPath + Separator + p
		}
		var cnt Content
		if entry.IsDirectory() || i != len(paths)-1 {
			cnt = NewContent(filepath.Base(currentPath), "", 0, MIMEDriveDirectory, time.Unix(entry.CreatedAt, 0))
		} else {
			cnt = NewContent(filepath.Base(currentPath), entry.CID, entry.Size, entry.Type, time.Unix(entry.CreatedAt, 0))
		}
		entries = append(entries, &Entry{
			Content: cnt,
			Path:    currentPath,
		})
	}
	return entries
}
func fixEntries(entries []*Entry, prefix string) []*Entry {
	for _, entry := range entries {
		entry.Path = prefix + entry.Path
		entry.Name = filepath.Base(entry.Path)
		if entry.Type == MIMEDriveEntry {
			entry.Type = MIMEDriveDirectory
		}
		entry.Entries = nil
	}
	return entries
}

func trace(ctx context.Context, name string) (stop func()) {
	span, _ := tracer.StartSpanFromContext(ctx, name,
		tracer.ServiceName("cs-filesystem"),
		tracer.SpanType("filesystem"),
		tracer.AnalyticsRate(1))
	return func() {
		span.Finish()
	}
}
