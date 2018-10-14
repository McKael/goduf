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

import (
	"encoding/json"
	"fmt"
)

func displayResults(results Results, jsonOutput bool, summaryOnly bool) {
	if jsonOutput {
		displayResultsJSON(results)
		return
	}

	if !summaryOnly {
		for i, g := range results.Groups {
			fmt.Printf("\nGroup #%d (%d files * %v):\n", i+1,
				len(g.Paths), formatSize(g.Size, true))
			for _, f := range g.Paths {
				fmt.Println(f)
			}
		}
	}

	// We're done if we do not display statistics
	if myLog.verbosity < 1 && !summaryOnly {
		return
	}

	// Add a trailing newline
	if len(results.Groups) > 0 && myLog.verbosity > 0 {
		fmt.Println()
	}
	myLog.Println(0, "Final count:", results.Duplicates,
		"duplicate files in", len(results.Groups), "sets")
	myLog.Println(0, "Redundant data size:",
		formatSize(results.RedundantDataSize, false))
}

func displayResultsJSON(results Results) {
	b, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}
