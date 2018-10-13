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

package main

// Implement a sort interface for the list of duplicate groups
type byGroupFileSize foListList

func (a byGroupFileSize) Len() int      { return len(a) }
func (a byGroupFileSize) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byGroupFileSize) Less(i, j int) bool {
	// Since this is supposed to be used for duplicate lists,
	// we use the size of the first file of the group.
	if a[i][0].Size() == a[j][0].Size() {
		return a[i][0].FilePath < a[j][0].FilePath
	}
	return a[i][0].Size() < a[j][0].Size()
}

// Implement a sort interface for a slice of files
type byFilePathName FileObjList

func (a byFilePathName) Len() int      { return len(a) }
func (a byFilePathName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byFilePathName) Less(i, j int) bool {
	return a[i].FilePath < a[j].FilePath
}
