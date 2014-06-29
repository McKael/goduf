
// +build plan9 windows

package main

import "os"

// ByInode is a FileObjList type with a sort interface
type ByInode FileObjList

func (a ByInode) Len() int      { return len(a) }
func (a ByInode) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByInode) Less(i, j int) bool {
	return a[i].FilePath < a[j].FilePath
}

// OSHasInodes returns true iff the O.S. uses inodes for its filesystems.
func OSHasInodes() bool {
	return false
}

// GetDevIno returns the device and inode IDs of a given file.
// This is not supported on Windows and Plan9.
func GetDevIno(fi os.FileInfo) (uint64, uint64) {
	return 0, 0 // Not supported
}
