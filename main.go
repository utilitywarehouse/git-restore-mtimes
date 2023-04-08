package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// git does not record or look to restore mtimes when a repository is cloned. This tool provides a means to restore
// file and directory mtimes in a git directory, based on the author commit time. This can prove useful for a variety
// of use cases. In the partner-mono case specifically, our tests suffer from a slowdown each time they are run in CI -
// this is because golang pays attention to any files accessed by tests, and if the mtime of those files has changed,
// there will be a cache miss, and the tests will be run again.
//
// Note that https://github.com/MestreLion/git-tools/blob/main/git-restore-mtime is commonly used for this purpose,
// however there is an issue which impacts us - https://github.com/MestreLion/git-tools/issues/47 - and makes that
// script ineffective. This re-implementation does not suffer from the same problem.
//
// Also note that it's possible to run `git log --pretty=format:%at -1 master -- <file>` against each file, but this is
// prohibitively slow.
func main() {
	if err := run(context.Background()); err != nil {
		// log.Fatal(err)
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	gitLog, err := readLog(ctx)
	if err != nil {
		return err
	}

	fs := &filesystem{
		root: &file{
			isDir:    true,
			children: make(map[string]*file),
		},
	}
	err = parseLog(ctx, bytes.NewReader(gitLog), func(t time.Time, ch change) error {
		var err error
		switch ch.action {
		case actionAdd:
			err = fs.create(ch.to, t)
		case actionModify:
			err = fs.touch(ch.to, t)
		case actionDelete:
			err = fs.remove(ch.to, t)
		case actionRename:
			err = fs.rename(ch.from, ch.to, t)
		}
		return err
	})
	if err != nil {
		return err
	}

	var updated int
	err = fs.walk(func(f *file, path string) error {
		updated++
		return os.Chtimes(path, time.Now(), f.mtime)
	})
	if err != nil {
		return err
	}

	log.Printf("%d mtimes updated", updated)
	return nil
}

func readLog(ctx context.Context) ([]byte, error) {
	gitBin, err := exec.LookPath("git")
	if err != nil {
		return nil, fmt.Errorf("failed to find git binary: %w", err)
	}
	cmd := exec.CommandContext(
		ctx,
		gitBin,
		"-c",
		"diff.renameLimit=10000",
		"log",
		"--raw",
		"--first-parent",
		"--pretty=%at",
		"--reverse",
	)
	return cmd.Output()
}

func parseLog(ctx context.Context, r io.Reader, cb func(time.Time, change) error) error {
	var ts time.Time
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := sc.Text()
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, ":") {
			ch, err := parseRawLine(line)
			if err != nil {
				return err
			}
			if err := cb(ts, ch); err != nil {
				return err
			}
		} else {
			rawTS, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return fmt.Errorf("unrecognised line, expected timestamp, received %q: %w", line, err)
			}
			ts = time.Unix(rawTS, 0)
		}
	}
	return nil
}

func parseRawLine(line string) (change, error) {
	parts := strings.Split(line, "\t")
	commitParts := strings.Split(parts[0], " ")
	if len(commitParts) != 5 {
		return change{}, fmt.Errorf("unhandled line format %q", line)
	}
	act, err := parseAction(commitParts[4])
	if err != nil {
		return change{}, err
	}
	var from, to string
	switch len(parts) {
	case 2:
		to = parts[1]
	case 3:
		from = parts[1]
		to = parts[2]
	default:
		return change{}, fmt.Errorf("unhandled line format: %q", line)
	}
	return change{
		action: act,
		from:   from,
		to:     to,
	}, nil
}

func parseAction(a string) (act action, err error) {
	switch {
	case a == "A":
		act = actionAdd
	case a == "D":
		act = actionDelete
	case a == "M":
		act = actionModify
	case strings.HasPrefix(a, "R"):
		act = actionRename
	default:
		err = fmt.Errorf("unrecognised action: %q", a)
	}
	return
}

type action int

const (
	actionAdd action = iota + 1
	actionDelete
	actionModify
	actionRename
)

type change struct {
	action action
	from   string
	to     string
}

// filesystem provides a very loose vfs style abstraction that purely caters for this usecase..  specifically:
// - it does not store any file content
// - file creation and removal times are supplied up front
// - missing directories are created on file creation
// - empty dirs are not permitted (akin to how this works out with git)
type filesystem struct {
	root *file
}

type file struct {
	name     string
	isDir    bool
	mtime    time.Time
	children map[string]*file
}

func (fs *filesystem) create(path string, at time.Time) error {
	dir := fs.root
	for _, part := range dirParts(path) {
		d, found := dir.children[part]
		if !found {
			// dir does not exist, so create
			d = &file{
				name:     part,
				isDir:    true,
				mtime:    at,
				children: make(map[string]*file),
			}
			dir.children[part] = d
			dir.mtime = at // dir creation updates mtime of parent dir
		}
		dir = d
	}
	if !dir.isDir {
		return errors.New("parent is not a directory")
	}
	name := filepath.Base(path)
	if _, found := dir.children[name]; found {
		return os.ErrExist
	}
	dir.children[name] = &file{
		name:  name,
		mtime: at,
	}
	dir.mtime = at // file creation updates mtime of parent dir
	return nil
}

func (fs *filesystem) rename(from, to string, at time.Time) error {
	f, err := fs.get(from)
	if err != nil {
		return err
	}
	// source file mtime is preserved
	if err := fs.create(to, f.mtime); err != nil {
		return err
	}
	return fs.remove(from, at)
}

func (fs *filesystem) get(path string) (*file, error) {
	parts := strings.Split(path, string(filepath.Separator))
	f := fs.root
	for _, part := range parts {
		var found bool
		f, found = f.children[part]
		if !found {
			return nil, os.ErrNotExist
		}
	}
	return f, nil
}

func (fs *filesystem) touch(path string, at time.Time) error {
	f, err := fs.get(path)
	if err != nil {
		return err
	}
	f.mtime = at
	return nil
}

func (fs *filesystem) remove(path string, at time.Time) error {
	dir := fs.root
	parents := []*file{dir}
	for _, part := range dirParts(path) {
		var found bool
		dir, found = dir.children[part]
		if !found {
			return os.ErrNotExist
		}
		parents = append(parents, dir)
	}
	name := filepath.Base(path)
	if _, found := dir.children[name]; !found {
		return os.ErrNotExist
	}
	delete(dir.children, name)
	dir.mtime = at // file removal updates parent dir mtime
	for i := len(parents) - 1; i > 0; i-- {
		dir := parents[i]
		if len(dir.children) > 0 {
			break
		}
		// parent directory is empty, remove
		prev := parents[i-1]
		delete(prev.children, dir.name)
		prev.mtime = at
	}
	return nil
}

func (fs *filesystem) walk(cb func(f *file, path string) error) error {
	return walk(fs.root, "", cb)
}

func walk(f *file, path string, cb func(f *file, path string) error) error {
	for _, child := range f.children {
		childPath := filepath.Join(path, child.name)
		if err := cb(child, childPath); err != nil {
			return err
		}
		if child.isDir {
			if err := walk(child, childPath, cb); err != nil {
				return err
			}
		}
	}
	return nil
}

func dirParts(path string) []string {
	dir := filepath.Dir(path)
	if dir == "." {
		return nil
	}
	return strings.Split(dir, string(filepath.Separator))
}
