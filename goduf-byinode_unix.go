
// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package main

import "os"
import "syscall"

// ByInode is a FileObjList type with a sort interface
type ByInode FileObjList

func (a ByInode) Len() int      { return len(a) }
func (a ByInode) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByInode) Less(i, j int) bool {
	// Sort by device id first
	iDevice := a[i].Sys().(*syscall.Stat_t).Dev
	jDevice := a[j].Sys().(*syscall.Stat_t).Dev
	switch {
	case iDevice < jDevice:
		return true
	case iDevice > jDevice:
		return false
	}
	iInode := a[i].Sys().(*syscall.Stat_t).Ino
	jInode := a[j].Sys().(*syscall.Stat_t).Ino
	return iInode < jInode
}

// OSHasInodes returns true iff the O.S. uses inodes for its filesystems.
func OSHasInodes() bool {
	return true
}

// GetDevIno returns the device and inode IDs of a given file.
func GetDevIno(fi os.FileInfo) (uint64, uint64) {
	dev := fi.Sys().(*syscall.Stat_t).Dev
	ino := fi.Sys().(*syscall.Stat_t).Ino
	return uint64(dev), uint64(ino)
}
