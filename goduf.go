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
	noChecksum sumType = iota
	fullChecksum
	partialChecksum
)

type fileObj struct {
	//Unique   bool
	FilePath string
	os.FileInfo
	PartialHash []byte
	Hash        []byte
	needHash    sumType
}

// FileObjList is only exported so that we can have a sort interface on inodes.
type FileObjList []*fileObj

type sizeClass struct { // XXX still useful?
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
			myLog.Println(6, "Ignoring symbolic link", path)
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
	} else if sType == noChecksum {
		return nil
	}
	panic("Internal error: Invalid sType")
}

func (data *dataT) dispCount() { // FIXME rather useless
	if myLog.verbosity < 4 {
		return
	}
	var c1, c1b, c2 int
	var s1 string
	for _, sc := range data.sizeGroups {
		c1 += len(sc.files)
		c2++
	}
	c1b = len(data.emptyFiles)
	if c1b > 0 {
		s1 = fmt.Sprintf("+%d", c1b)
	}
	myLog.Printf(4, "  Current countdown: %d  [%d%s/%d]\n",
		c1+c1b, c1, s1, c2)
}

func (data *dataT) filesWithSameHash() (hgroups []FileObjList) {
	for _, sizeGroup := range data.sizeGroups {
		for _, l := range sizeGroup.fullsums {
			hgroups = append(hgroups, l)
		}
	}
	return
}

// checksum returns the requested checksum as a string.
// If the checksum has not been pre-computed, it is calculated now.
func (fo fileObj) checksum(sType sumType) (string, error) {
	var hbytes []byte
	if sType == partialChecksum {
		hbytes = fo.PartialHash
	} else if sType == fullChecksum {
		hbytes = fo.Hash
	} else {
		panic("Internal error: Invalid sType")
	}
	if hbytes == nil {
		if err := fo.Sum(sType); err != nil {
			return "", err
		}
		if sType == partialChecksum {
			hbytes = fo.PartialHash
		} else if sType == fullChecksum {
			hbytes = fo.Hash
		}
	}
	return hex.EncodeToString(hbytes), nil
}

func (fileList FileObjList) computeSheduledChecksums() {
	// Sort the list for better efficiency
	sort.Sort(ByInode(fileList))

	myLog.Printf(6, "  . will compute %d checksums\n", len(fileList))

	// Compute checksums
	for _, fo := range fileList {
		if err := fo.Sum(fo.needHash); err != nil {
			myLog.Println(0, "Error:", err)
		}
	}
}

func (fileList FileObjList) scheduleChecksum(sType sumType) {
	for _, fo := range fileList {
		fo.needHash = sType
	}
}

func (fileList FileObjList) findDupesChecksums(sType sumType) []FileObjList {
	var dupeList []FileObjList
	var scheduleFull []FileObjList
	hashes := make(map[string]FileObjList)

	// Sort the list for better efficiency
	sort.Sort(ByInode(fileList))

	// Compute checksums
	for _, fo := range fileList {
		hash, err := fo.checksum(sType)
		if err != nil {
			myLog.Println(0, "Error:", err)
			continue
		}
		hashes[hash] = append(hashes[hash], fo)
	}

	// Let's de-dupe now...
	for _, l := range hashes {
		if len(l) < 2 {
			continue
		}
		if sType == partialChecksum {
			scheduleFull = append(scheduleFull, l)
		} else { // full checksums -> we're done
			dupeList = append(dupeList, l)
			// TODO: sort by increasing size
			myLog.Printf(5, "  . found %d new duplicates\n", len(l))
		}
	}
	if sType == partialChecksum && len(scheduleFull) > 0 {
		var csList FileObjList
		for _, fol := range scheduleFull {
			csList = append(csList, fol...)
		}
		myLog.Printf(6, "  .. findDupesChecksums: computing %d "+
			"full checksums\n", len(csList)) // DBG
		csList.computeSheduledChecksums()
		for _, l := range scheduleFull {
			r := l.findDupesChecksums(fullChecksum)
			dupeList = append(dupeList, r...)
		}
	}

	return dupeList
}

// findDupes() uses checksums to find file duplicates
func (data *dataT) findDupes(skipPartial bool) []FileObjList {
	var dupeList []FileObjList
	var schedulePartial []FileObjList
	var scheduleFull []FileObjList

	for size, sizeGroup := range data.sizeGroups {
		// We skip partial checksums for small files or if requested
		if size > minSizePartialChecksum && !skipPartial {
			sizeGroup.files.scheduleChecksum(partialChecksum)
			schedulePartial = append(schedulePartial, sizeGroup.files)
		} else {
			sizeGroup.files.scheduleChecksum(fullChecksum)
			scheduleFull = append(scheduleFull, sizeGroup.files)
		}
	}

	var csList FileObjList
	for _, fol := range schedulePartial {
		csList = append(csList, fol...)
	}
	for _, fol := range scheduleFull {
		csList = append(csList, fol...)
	}
	myLog.Printf(6, "  .. findDupes: computing %d misc checksums\n",
		len(csList)) // DBG
	csList.computeSheduledChecksums()

	for _, l := range schedulePartial {
		r := l.findDupesChecksums(partialChecksum)
		dupeList = append(dupeList, r...)
	}
	for _, l := range scheduleFull {
		r := l.findDupesChecksums(fullChecksum)
		dupeList = append(dupeList, r...)
	}
	// TODO: sort by increasing size
	return dupeList
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

// initialCleanup() removes files with unique size as well as hard links
func (data *dataT) initialCleanup() (hardLinkCount, uniqueSizeCount int) {
	for s, sizeGroup := range data.sizeGroups {
		if len(sizeGroup.files) < 2 {
			delete(data.sizeGroups, s)
			uniqueSizeCount++
			continue
		}

		var hardlinksFound bool

		// Check for hardlinks
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

			for i, fo := range sizeGroup.files[1:] {
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
		"Set verbosity level (1-6)")
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
		data.dispCount() // XXX
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
	}

	// Remove unique sizes
	myLog.Println(1, "* Removing files with unique size and hard links...")
	hardLinkCount, uniqueSizeCount := data.initialCleanup()
	if verbose {
		myLog.Printf(2, "  Dropped %d files with unique size\n",
			uniqueSizeCount)
		myLog.Printf(2, "  Dropped %d hard links\n", hardLinkCount)
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
		data.dispCount() // XXX
	}

	// Get list of dupes
	myLog.Println(1, "* Computing checksums...")
	var result []FileObjList
	if len(data.emptyFiles) > 0 {
		result = append(result, data.emptyFiles)
	}
	result = append(result, data.findDupes(skipPartial)...)

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
		dupeSize += size * uint64(len(l)-1)
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
