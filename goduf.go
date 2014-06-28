/*
 * Copyright (C) 2014 Mikael Berthe <mikael@lilotux.net>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 */

// This program (Goduf) is a fast duplicate file finder.
// Use goduf --help to get the list of available options.

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

const medsumBytes = 128
const minSizePartialChecksum = 49152 // Should be > 3*medsumBytes

type sumType int

const (
	fullChecksum sumType = iota
	partialChecksum
)

type fileObj struct {
	//Unique   bool
	FilePath string
	os.FileInfo
	PartialHash []byte
	Hash        []byte
}

type FileObjList []*fileObj

type sizeClass struct {
	files    FileObjList
	medsums  map[string]FileObjList
	fullsums map[string]FileObjList
}

type dataT struct {
	totalSize   uint64
	cmpt        uint
	sizeGroups  map[int64]*sizeClass
	emptyFiles  FileObjList
	ignoreCount int
}

var data dataT

type myLogT struct {
	verbosity int
}

var myLog myLogT

func (l *myLogT) Printf(level int, format string, args ...interface{}) {
	if level > l.verbosity {
		return
	}
	if level >= 0 {
		log.Printf(format, args...)
		return
	}
	// Error message without timestamp
	fmt.Fprintf(os.Stderr, format, args...)
}

func (l *myLogT) Println(level int, args ...interface{}) {
	if level > l.verbosity {
		return
	}
	if level >= 0 {
		log.Println(args...)
		return
	}
	// Error message without timestamp
	fmt.Fprintln(os.Stderr, args...)
}

func visit(path string, f os.FileInfo, err error) error {
	if err != nil {
		if f == nil {
			return err
		}
		if f.IsDir() {
			myLog.Println(-1, "Warning: cannot process directory:",
				path)
			return filepath.SkipDir
		}

		myLog.Println(-1, "Ignoring ", path, " - ", err)
		data.ignoreCount++
		return nil
	}
	if f.IsDir() {
		return nil
	}

	if mode := f.Mode(); mode&os.ModeType != 0 {
		if mode&os.ModeSymlink != 0 {
			myLog.Println(5, "Ignoring symbolic link", path)
		} else {
			myLog.Println(0, "Ignoring special file", path)
		}
		data.ignoreCount++
		return nil
	}

	data.cmpt++
	data.totalSize += uint64(f.Size())
	fo := &fileObj{FilePath: path, FileInfo: f}
	if _, ok := data.sizeGroups[f.Size()]; !ok {
		data.sizeGroups[f.Size()] = &sizeClass{}
	}
	data.sizeGroups[f.Size()].files =
		append(data.sizeGroups[f.Size()].files, fo)
	return nil
}

func (fo *fileObj) CheckSum() error {
	file, err := os.Open(fo.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha1.New()
	if size, err := io.Copy(hash, file); size != fo.Size() || err != nil {
		if err == nil {
			return errors.New("failed to read the whole file: " +
				fo.FilePath)
		}
		return err
	}

	fo.Hash = hash.Sum(nil)

	return nil
}

func (fo *fileObj) MedSum() error {
	file, err := os.Open(fo.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha1.New()
	// XXX: duplicated code!
	// BOF
	if _, err := io.CopyN(hash, file, medsumBytes); err != nil {
		if err == nil {
			return errors.New("failed to read bytes from file:" +
				fo.FilePath)
		}
		return err
	}
	/*
		// MOF
		file.Seek((fo.Size()-medsumBytes)/2, 0)
		if _, err := io.CopyN(hash, file, medsumBytes); err != nil {
			if err == nil {
				return errors.New("failed to read bytes from file:" +
					fo.FilePath)
			}
			return err
		}
	*/
	// EOF
	file.Seek(0-medsumBytes, 2)
	if _, err := io.CopyN(hash, file, medsumBytes); err != nil {
		if err == nil {
			return errors.New("failed to read bytes from file:" +
				fo.FilePath)
		}
		return err
	}

	fo.PartialHash = hash.Sum(nil)

	return nil
}

func (fo *fileObj) Sum(sType sumType) error {
	if sType == partialChecksum {
		return fo.MedSum()
	} else if sType == fullChecksum {
		return fo.CheckSum()
	}
	panic("Internal error: Invalid sType")
}

func (data *dataT) dispCount() {
	if myLog.verbosity < 4 {
		return
	}
	var c1, c2, c3, c4 int
	var s4 string
	for _, sc := range data.sizeGroups {
		c1 += len(sc.files)
		for _, fol := range sc.medsums {
			c2 += len(fol)
		}
		for _, fol := range sc.fullsums {
			c3 += len(fol)
		}
	}
	c4 = len(data.emptyFiles)
	if c4 > 0 {
		s4 = fmt.Sprintf("+%d", c4)
	}
	myLog.Printf(4, "  Current countdown: %d  [%d%s/%d/%d]\n",
		c1+c2+c3+c4, c1, s4, c2, c3)
}

func (data *dataT) filesWithSameHash() (hgroups []FileObjList) {
	for _, sizeGroup := range data.sizeGroups {
		for _, l := range sizeGroup.fullsums {
			hgroups = append(hgroups, l)
		}
	}
	return
}

func createHashFromSum(sType sumType, folist FileObjList, hashes *map[string]FileObjList) {
	for _, fo := range folist {
		var hash string
		var hbytes []byte
		if sType == partialChecksum {
			hbytes = fo.PartialHash
		} else if sType == fullChecksum {
			hbytes = fo.Hash
		} else {
			panic("Internal error: Invalid sType")
		}
		if hbytes == nil {
			// Could happen if the file was not readable,
			// We skip it (already shown as dropped)
			continue
		}
		hash = hex.EncodeToString(hbytes)

		(*hashes)[hash] = append((*hashes)[hash], fo)
	}
}

func (data *dataT) calculateMedSums() {
	var bigList FileObjList
	var droppedGroups int
	var droppedFiles int

	// Create a unique list of files to be partially checksummed
	for size, sizeGroup := range data.sizeGroups {
		if size < minSizePartialChecksum {
			// We do not use MedSums for small files
			continue
		}
		bigList = append(bigList, sizeGroup.files...)
	}

	// Sort the list for better efficiency -- that's the whole point of
	// the unique big list.
	sort.Sort(ByInode(bigList))

	// Compute partial checksums
	for _, fo := range bigList {
		if err := fo.Sum(partialChecksum); err != nil {
			myLog.Println(0, "Error:", err)
			droppedFiles++
			// The hash part will be nil and the file will
			// be dropped in createHashFromSum() below.
		}
	}

	// Reparse size-grouped hashes and use the new hash information
	// to build lists of files with the same partial checksum.
	for size, sizeGroup := range data.sizeGroups {
		if size < minSizePartialChecksum {
			// We do not use MedSums for small files
			continue
		}
		hashes := make(map[string]FileObjList)
		createHashFromSum(partialChecksum, sizeGroup.files, &hashes)

		// Let's de-dupe now...
		for h, l := range hashes {
			if len(l) < 2 {
				droppedGroups++
				droppedFiles += len(l)
				delete(hashes, h)
			}
		}

		// Done, save the result
		data.sizeGroups[size].medsums = hashes

		// We remove the items from "files" since they are in the hash
		// tree now.
		sizeGroup.files = nil

		if len(hashes) == 0 { // We're done with this size group
			delete(data.sizeGroups, size)
		}
	}

	if droppedFiles > 0 {
		myLog.Printf(3, "  Dropped %d files in %d groups\n",
			droppedFiles, droppedGroups)
	}
	return
}

func (data *dataT) calculateChecksums() {
	var bigList FileObjList
	var droppedGroups int
	var droppedFiles int

	// Create a unique list of files to be fully checksummed
	for _, sizeGroup := range data.sizeGroups {
		// #1: small files
		bigList = append(bigList, sizeGroup.files...)
		// #2: files with same partial checksum
		for _, l := range sizeGroup.medsums {
			bigList = append(bigList, l...)
		}
	}

	// Sort the list for better efficiency -- that's the whole point of
	// the unique big list.
	sort.Sort(ByInode(bigList))

	// Compute full checksums
	for _, fo := range bigList {
		if err := fo.Sum(fullChecksum); err != nil {
			myLog.Println(0, "Error:", err)
			droppedFiles++
			// The hash part will be nil and the file will
			// be dropped in createHashFromSum() below.
		}
	}

	// Reparse size-grouped hashes and use the new hash information
	// to build lists of files with the same checksum.
	for size, sizeGroup := range data.sizeGroups {
		hashes := make(map[string]FileObjList)
		// #1: small files
		createHashFromSum(fullChecksum, sizeGroup.files, &hashes)
		// #2: files with same partial checksum
		for _, l := range sizeGroup.medsums {
			createHashFromSum(fullChecksum, l, &hashes)
		}

		// Let's de-dupe now...
		for h, l := range hashes {
			if len(l) < 2 {
				droppedGroups++
				droppedFiles += len(l)
				delete(hashes, h)
			}
		}

		// Done, save the result
		data.sizeGroups[size].fullsums = hashes
		data.sizeGroups[size].medsums = nil
		sizeGroup.files = nil
	}
	if droppedFiles > 0 {
		myLog.Printf(3, "  Dropped %d files in %d groups\n",
			droppedFiles, droppedGroups)
	}
	return
}

func (data *dataT) dropEmptyFiles(ignoreEmpty bool) (emptyCount int) {
	sc, ok := data.sizeGroups[0]
	if ok == false {
		return // no empty files
	}
	if !ignoreEmpty {
		if len(sc.files) > 1 {
			data.emptyFiles = sc.files
		}
		delete(data.sizeGroups, 0)
		return
	}
	emptyCount = len(sc.files)
	delete(data.sizeGroups, 0)
	return
}

func (data *dataT) createSizeHash() (hardLinkCount, uniqueSizeCount int) {
	for s, sizeGroup := range data.sizeGroups {
		if len(sizeGroup.files) < 2 {
			delete(data.sizeGroups, s)
			uniqueSizeCount++
			continue
		}

		var hardlinksFound bool

		// Sort by device/inodes
		sort.Sort(ByInode(sizeGroup.files))

		// Check for hardlinks
		// TODO: what about symlinks?
		// Remove unique dev/inodes
		// Instead of this loop, another way would be to use the field
		// "Unique" of the fileObj to mark them to be discarded
		// and remove them all at the end.
		for {
			if !OSHasInodes() {
				break
			}
			var hardLinkIndex int
			fo := sizeGroup.files[0]
			prevDev, prevIno := GetDevIno(fo)
			//fmt.Printf(" - FO: %#v\n", *fo)

			for i, fo := range sizeGroup.files[1:] {
				//fmt.Printf(" - FO %d : %#v\n", i, *fo)
				dev, ino := GetDevIno(fo)
				if dev == prevDev && ino == prevIno {
					hardLinkIndex = i + 1
					hardLinkCount++
					hardlinksFound = true
					break
				}
				prevDev = dev
				prevIno = ino
			}

			if hardLinkIndex == 0 {
				break
			}
			i := hardLinkIndex
			// Remove hardink
			copy(sizeGroup.files[i:], sizeGroup.files[i+1:])
			sizeGroup.files[len(sizeGroup.files)-1] = nil
			sizeGroup.files = sizeGroup.files[:len(sizeGroup.files)-1]
		}
		// We have found hard links in this size group,
		// maybe we can remove it
		if hardlinksFound {
			if len(sizeGroup.files) < 2 {
				delete(data.sizeGroups, s)
				uniqueSizeCount++
				continue
			}
		}
	}
	return
}

func formatSize(sizeBytes uint64, short bool) string {
	var units = map[int]string{
		0: "B",
		1: "KiB",
		2: "MiB",
		3: "GiB",
		4: "TiB",
		5: "PiB",
	}
	humanSize := sizeBytes
	var n int
	for n < len(units)-1 {
		if humanSize < 10000 {
			break
		}
		humanSize /= 1024
		n++
	}
	if n < 1 {
		return fmt.Sprintf("%d bytes", sizeBytes)
	}
	if short {
		return fmt.Sprintf("%d %s", humanSize, units[n])
	}
	return fmt.Sprintf("%d bytes (%d %s)", sizeBytes, humanSize, units[n])
}

func main() {
	var verbose bool
	var summary bool
	var skipPartial bool
	var ignoreEmpty bool

	flag.BoolVar(&verbose, "verbose", false, "Be verbose (verbosity=1)")
	flag.BoolVar(&verbose, "v", false, "See --verbose")
	flag.BoolVar(&summary, "summary", false, "Do not display the duplicate list")
	flag.BoolVar(&summary, "s", false, "See --summary")
	flag.BoolVar(&skipPartial, "skip-partial", false, "Skip partial checksums")
	flag.IntVar(&myLog.verbosity, "verbosity", 0,
		"Set verbosity level (1-5)")
	flag.IntVar(&myLog.verbosity, "vl", 0, "See verbosity")
	timings := flag.Bool("timings", false, "Set detailed log timings")
	flag.BoolVar(&ignoreEmpty, "no-empty", false, "Ignore empty files")

	flag.Parse()

	if myLog.verbosity > 0 {
		verbose = true
	} else if verbose == true {
		myLog.verbosity = 1
	}

	if len(flag.Args()) == 0 {
		// TODO: more helpful usage statement
		myLog.Println(-1, "Usage:", os.Args[0],
			"[options] base_directory")
		os.Exit(0)
	}

	if *timings {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	data.sizeGroups = make(map[int64]*sizeClass)
	myLog.Println(1, "* Reading file metadata")

	for _, root := range flag.Args() {
		if err := filepath.Walk(root, visit); err != nil {
			myLog.Printf(-1, "* Error: could not read file tree:\n")
			myLog.Printf(-1, "> %v\n", err)
			os.Exit(1)
		}
	}
	emptyCount := data.dropEmptyFiles(ignoreEmpty)
	if verbose {
		if data.ignoreCount > 0 {
			myLog.Printf(1, "  %d special files were ignored\n",
				data.ignoreCount)
		}
		myLog.Println(2, "  Initial counter:", data.cmpt, "files")
		myLog.Println(2, "  Total size:", formatSize(data.totalSize,
			false))
		if emptyCount > 0 {
			myLog.Printf(1, "  %d empty files were ignored\n",
				emptyCount)
		}
		data.dispCount()
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
	}

	// Remove unique sizes
	myLog.Println(1, "* Removing files with unique size, sorting file lists...")
	hardLinkCount, uniqueSizeCount := data.createSizeHash()
	if verbose {
		myLog.Printf(2, "  Dropped %d files with unique size\n",
			uniqueSizeCount)
		myLog.Printf(2, "  Dropped %d hard links\n", hardLinkCount)
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
		data.dispCount()
	}

	// Calculate medsums
	if !skipPartial {
		myLog.Println(1, "* Calculating partial checksums...")
		data.calculateMedSums()
		data.dispCount()
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
	}

	// Calculate checksums
	myLog.Println(1, "* Calculating checksums...")
	data.calculateChecksums()
	data.dispCount()

	// Get list of dupes
	var result []FileObjList
	if len(data.emptyFiles) > 0 {
		result = append(result, data.emptyFiles)
	}
	result = append(result, data.filesWithSameHash()...)
	myLog.Println(3, "* Number of match groups:", len(result))

	// Done!  Dump dupes
	if len(result) > 0 && !summary {
		myLog.Println(1, "* Dupes:")
	}
	var dupeSize uint64
	data.cmpt = 0
	for i, l := range result {
		size := uint64(l[0].Size())
		// We do not count the size of the 1st item
		// so we get only duplicate size.
		dupeSize += size * uint64(len(l) - 1)
		if !summary {
			fmt.Printf("\nGroup #%d (%d files * %v):\n", i+1,
				len(l), formatSize(size, true))
		}
		for _, f := range l {
			if !summary {
				fmt.Println(f.FilePath)
			}
			data.cmpt++
		}
	}
	summaryLevel := 1 // Default verbosity for the summary line
	if summary == false {
		// Add a trailing newline
		if len(result) > 0 {
			fmt.Println("")
		}
	} else {
		// The summary is requested so we lower the verbosity level
		summaryLevel = 0
	}

	myLog.Println(summaryLevel, "Final count:", data.cmpt,
		"duplicate files in", len(result), "sets")
	myLog.Println(summaryLevel, "Redundant data size:",
		formatSize(dupeSize, false))
}
