/*
 * Copyright (C) 2014-2018 Mikael Berthe <mikael@lilotux.net>
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
//
// Installation:
//
// % go get hg.lilotux.net/golang/mikael/goduf
// or
// % go get github.com/McKael/goduf

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
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

// Options contains the command-line flags
type Options struct {
	Summary     bool
	OutToJSON   bool
	SkipPartial bool
	IgnoreEmpty bool
}

// Results contains the results of the duplicates search
type Results struct {
	Groups             []ResultSet `json:"groups"`
	Duplicates         uint        `json:"duplicates"`
	NumberOfSets       uint        `json:"number_of_sets"`
	RedundantDataSize  uint64      `json:"redundant_data_size"`
	RedundantDataSizeH string      `json:"redundant_data_size_h"`
	TotalFileCount     uint        `json:"total_file_count"`
}

// ResultSet contains a group of identical duplicate files
type ResultSet struct {
	Size  uint64   `json:"size"`  // Size of each item
	Paths []string `json:"paths"` // List of file paths
}

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
type foListList []FileObjList

type dataT struct {
	totalSize   uint64
	cmpt        uint
	sizeGroups  map[int64]*FileObjList
	emptyFiles  FileObjList
	ignoreCount int
}

var data dataT

// Implement my own logger
var myLog myLogT

// visit is called for every file and directory.
// We check the file object is correct (regular, readable...) and add
// it to the data.sizeGroups hash.
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
		data.sizeGroups[f.Size()] = new(FileObjList)
	}
	*data.sizeGroups[f.Size()] = append(*data.sizeGroups[f.Size()], fo)
	return nil
}

// Checksum computes the file's complete SHA1 hash.
func (fo *fileObj) Checksum() error {
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

// partialChecksum computes the file's partial SHA1 hash (first and last bytes).
func (fo *fileObj) partialChecksum() error {
	file, err := os.Open(fo.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha1.New()

	// Read first bytes and last bytes from file
	for i := 0; i < 2; i++ {
		if _, err := io.CopyN(hash, file, medsumBytes); err != nil {
			if err == nil {
				const errmsg = "failed to read bytes from file: "
				return errors.New(errmsg + fo.FilePath)
			}
			return err
		}
		if i == 0 { // Seek to end of file
			file.Seek(0-medsumBytes, 2)
		}
	}

	fo.PartialHash = hash.Sum(nil)

	return nil
}

// Sum computes the file's SHA1 hash, partial or full according to sType.
func (fo *fileObj) Sum(sType sumType) error {
	if sType == partialChecksum {
		return fo.partialChecksum()
	} else if sType == fullChecksum {
		return fo.Checksum()
	} else if sType == noChecksum {
		return nil
	}
	panic("Internal error: Invalid sType")
}

// dispCount display statistics to the user.
func (data *dataT) dispCount() { // It this still useful?
	if myLog.verbosity < 4 {
		return
	}
	var c1, c1b, c2 int
	var s1 string
	for _, scListP := range data.sizeGroups {
		c1 += len(*scListP)
		c2++
	}
	c1b = len(data.emptyFiles)
	if c1b > 0 {
		s1 = fmt.Sprintf("+%d", c1b)
	}
	myLog.Printf(4, "  Current countdown: %d  [%d%s/%d]\n",
		c1+c1b, c1, s1, c2)
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

// computeSheduledChecksums calculates the checksums for all the files
// from the fileLists slice items (the kind of hash is taken from the
// needHash field).
func computeSheduledChecksums(fileLists ...foListList) {
	var bigFileList FileObjList
	// Merge the lists of FileObjList lists and create a unique list
	// of file objects.
	for _, foll := range fileLists {
		for _, fol := range foll {
			bigFileList = append(bigFileList, fol...)
		}
	}

	// Sort the list for better efficiency
	sort.Sort(ByInode(bigFileList))

	// Compute checksums
	for _, fo := range bigFileList {
		if err := fo.Sum(fo.needHash); err != nil {
			myLog.Println(0, "Error:", err)
		}
		fo.needHash = noChecksum
	}
}

func (fileList FileObjList) scheduleChecksum(sType sumType) {
	for _, fo := range fileList {
		fo.needHash = sType
	}
}

// findDupesChecksums splits the fileObj list into several lists with the
// same sType hash.
func (fileList FileObjList) findDupesChecksums(sType sumType, dryRun bool) foListList {
	var dupeList foListList
	var scheduleFull foListList
	hashes := make(map[string]FileObjList)

	// Sort the list for better efficiency
	sort.Sort(ByInode(fileList))

	if sType == fullChecksum && dryRun {
		fileList.scheduleChecksum(fullChecksum)
		return append(dupeList, fileList)
	}
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
			myLog.Printf(5, "  . found %d new duplicates\n", len(l))
		}
	}
	if sType == partialChecksum && len(scheduleFull) > 0 {
		//computeSheduledChecksums(scheduleFull)
		for _, l := range scheduleFull {
			r := l.findDupesChecksums(fullChecksum, dryRun)
			dupeList = append(dupeList, r...)
		}
		if dryRun {
			return scheduleFull
		}
	}

	return dupeList
}

// findDupes() uses checksums to find file duplicates
func (data *dataT) findDupes(skipPartial bool) foListList {
	var dupeList foListList
	var schedulePartial foListList
	var schedulePartial2 foListList
	var scheduleFull foListList

	for size, sgListP := range data.sizeGroups {
		// We skip partial checksums for small files or if requested
		if size > minSizePartialChecksum && !skipPartial {
			sgListP.scheduleChecksum(partialChecksum)
			schedulePartial = append(schedulePartial, *sgListP)
		} else {
			sgListP.scheduleChecksum(fullChecksum)
			scheduleFull = append(scheduleFull, *sgListP)
		}
	}

	computeSheduledChecksums(schedulePartial, scheduleFull)

	for _, l := range schedulePartial {
		r := l.findDupesChecksums(partialChecksum, true) // dry-run
		schedulePartial2 = append(schedulePartial2, r...)
	}
	computeSheduledChecksums(schedulePartial2)
	for _, l := range schedulePartial {
		r := l.findDupesChecksums(partialChecksum, false)
		dupeList = append(dupeList, r...)
	}
	for _, l := range scheduleFull {
		r := l.findDupesChecksums(fullChecksum, false)
		dupeList = append(dupeList, r...)
	}
	return dupeList
}

// dropEmptyFiles removes the empty files from the main map, since we don't
// have to do any processing about them.
// If ignoreEmpty is false, the empty file list is saved in data.emptyFiles.
func (data *dataT) dropEmptyFiles(ignoreEmpty bool) (emptyCount int) {
	sgListP, ok := data.sizeGroups[0]
	if ok == false {
		return // no empty files
	}
	if !ignoreEmpty {
		if len(*sgListP) > 1 {
			data.emptyFiles = *sgListP
		}
		delete(data.sizeGroups, 0)
		return
	}
	emptyCount = len(*sgListP)
	delete(data.sizeGroups, 0)
	return
}

// initialCleanup() removes files with unique size as well as hard links
func (data *dataT) initialCleanup() (hardLinkCount, uniqueSizeCount int) {
	for s, sgListP := range data.sizeGroups {
		if len(*sgListP) < 2 {
			delete(data.sizeGroups, s)
			uniqueSizeCount++
			continue
		}

		// We can't look for hard links if the O.S. does not support
		// them...
		if !OSHasInodes() {
			continue
		}

		var hardlinksFound bool

		// Check for hard links
		// Remove unique dev/inodes
		// Instead of this loop, another way would be to use the field
		// "Unique" of the fileObj to mark them to be discarded
		// and remove them all at the end.
		// TODO: Should we also check for duplicate paths?
		for {
			type devinode struct{ dev, ino uint64 }
			devinodes := make(map[devinode]bool)
			var hardLinkIndex int

			for i, fo := range *sgListP {
				dev, ino := GetDevIno(fo)
				di := devinode{dev, ino}
				if _, hlink := devinodes[di]; hlink {
					hardLinkIndex = i
					hardLinkCount++
					hardlinksFound = true
					break
				} else {
					devinodes[di] = true
				}
			}

			if hardLinkIndex == 0 {
				break
			}
			i := hardLinkIndex
			// Remove hardink
			copy((*sgListP)[i:], (*sgListP)[i+1:])
			(*sgListP)[len(*sgListP)-1] = nil
			*sgListP = (*sgListP)[:len(*sgListP)-1]
		}
		// We have found hard links in this size group,
		// maybe we can remove it
		if hardlinksFound {
			if len(*sgListP) < 2 {
				delete(data.sizeGroups, s)
				uniqueSizeCount++
				continue
			}
		}
	}
	return
}

func duf(dirs []string, options Options) (Results, error) {
	var verbose bool
	if myLog.verbosity > 0 {
		verbose = true
	}

	var results Results
	data.sizeGroups = make(map[int64]*FileObjList)

	myLog.Println(1, "* Reading file metadata")

	for _, root := range dirs {
		if err := filepath.Walk(root, visit); err != nil {
			return results, fmt.Errorf("could not read file tree: %v", err)
		}
	}

	// Count empty files and drop them if they should be ignored
	emptyCount := data.dropEmptyFiles(options.IgnoreEmpty)

	// Display a small report
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

	// Remove unique sizes and hard links
	myLog.Println(1, "* Removing files with unique size and hard links...")
	hardLinkCount, uniqueSizeCount := data.initialCleanup()
	if verbose {
		myLog.Printf(2, "  Dropped %d files with unique size\n",
			uniqueSizeCount)
		myLog.Printf(2, "  Dropped %d hard links\n", hardLinkCount)
		myLog.Println(3, "* Number of size groups:", len(data.sizeGroups))
		data.dispCount()
	}

	// Get the final list of dupes, using checksums
	myLog.Println(1, "* Computing checksums...")
	var result foListList
	if len(data.emptyFiles) > 0 {
		result = append(result, data.emptyFiles)
	}
	result = append(result, data.findDupes(options.SkipPartial)...)

	myLog.Println(3, "* Number of match groups:", len(result))

	// Done!  Prepare results data
	if len(result) > 0 && !options.Summary {
		myLog.Println(1, "* Dupes:")
	}

	// Sort files by path inside each group
	for _, l := range result {
		sort.Sort(byFilePathName(l))
	}
	// Sort groups by increasing size (of the duplicated files)
	sort.Sort(byGroupFileSize(result))

	for _, l := range result {
		size := uint64(l[0].Size())
		// We do not count the size of the 1st item
		// so we get only duplicate size.
		results.RedundantDataSize += size * uint64(len(l)-1)
		newSet := ResultSet{Size: size}
		for _, f := range l {
			newSet.Paths = append(newSet.Paths, f.FilePath)
			results.Duplicates++
		}
		results.Groups = append(results.Groups, newSet)
	}
	results.NumberOfSets = uint(len(results.Groups))
	results.RedundantDataSizeH = formatSize(results.RedundantDataSize, true)
	results.TotalFileCount = data.cmpt

	return results, nil
}

// It all starts here.
func main() {
	var verbose bool
	var options Options

	// Assertion on constant values
	if minSizePartialChecksum <= 2*medsumBytes {
		myLog.Fatal("Internal error: assert minSizePartialChecksum > 2*medsumBytes")
	}

	// Command line parameters parsingg
	flag.BoolVar(&verbose, "verbose", false, "Be verbose (verbosity=1)")
	flag.BoolVar(&verbose, "v", false, "See --verbose")
	flag.BoolVar(&options.OutToJSON, "json", false, "Use JSON format for output")
	flag.BoolVar(&options.Summary, "summary", false, "Do not display the duplicate list")
	flag.BoolVar(&options.Summary, "s", false, "See --summary")
	flag.BoolVar(&options.SkipPartial, "skip-partial", false, "Skip partial checksums")
	flag.BoolVar(&options.IgnoreEmpty, "no-empty", false, "Ignore empty files")
	flag.IntVar(&myLog.verbosity, "verbosity", 0, "Set verbosity level (1-6)")
	flag.IntVar(&myLog.verbosity, "vl", 0, "See verbosity")
	timings := flag.Bool("timings", false, "Show detailed log timings")

	flag.Parse()

	// Set verbosity: --verbose=true == --verbosity=1
	if myLog.verbosity > 0 {
		verbose = true
	} else if verbose == true {
		myLog.verbosity = 1
	}

	if len(flag.Args()) == 0 {
		// TODO: more helpful usage statement
		myLog.Println(-1, "Usage:", os.Args[0],
			"[options] base_directory|file...")
		os.Exit(0)
	}

	// Change log format for benchmarking
	if *timings {
		myLog.SetBenchFlags()
	}

	results, err := duf(flag.Args(), options)
	if err != nil {
		myLog.Fatal("ERROR: " + err.Error())
	}

	// Output the results
	displayResults(results, options.OutToJSON, options.Summary)
}
