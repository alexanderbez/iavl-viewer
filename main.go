package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	dbm "github.com/tendermint/tm-db"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

var moduleRe = regexp.MustCompile(`s\/k:(\w+)\/`)

func main() {
	db, err := dbm.NewDB("application", dbm.GoLevelDBBackend, "./data")
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}

	goLevelDB, ok := db.(*dbm.GoLevelDB)
	if !ok {
		log.Fatalf("invalid logical DB type; expected: %T, got: %T", &dbm.GoLevelDB{}, db)
	}

	// print native stats
	levelDBStats, err := goLevelDB.DB().GetProperty("leveldb.stats")
	if err != nil {
		log.Fatalf("failed to get LevelDB stats: %v", err)
	}

	fmt.Printf("%s\n", levelDBStats)

	var (
		totalKeys    int
		totalKeySize int
		totalValSize int

		moduleStats = make(map[string][]int)
	)

	iter := goLevelDB.DB().NewIterator(nil, nil)
	for iter.Next() {
		keySize := len(iter.Key())
		valSize := len(iter.Value())

		totalKeys++
		totalKeySize += keySize
		totalValSize += valSize

		var statKey string

		keyStr := string(iter.Key())
		if strings.HasPrefix(keyStr, "s/k:") {
			tokens := moduleRe.FindStringSubmatch(keyStr)
			statKey = tokens[1]
		} else {
			statKey = "misc"
		}

		if moduleStats[statKey] == nil {
			// XXX/TODO: Move this into a struct
			//
			// 0: total set size
			// 1: total key size
			// 2: total value size
			moduleStats[statKey] = make([]int, 3)
		}

		moduleStats[statKey][0]++
		moduleStats[statKey][1] += keySize
		moduleStats[statKey][2] += valSize
	}

	// print application-specific stats
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Module", "Avg Key Size", "Avg Value Size", "Total Key Size", "Total Value Size", "Total Key Pairs"})

	modules := maps.Keys(moduleStats)
	SortSlice(modules)

	for _, m := range modules {
		stats := moduleStats[m]
		t.AppendRow([]interface{}{
			m,
			ByteCountDecimal(stats[1] / stats[0]),
			ByteCountDecimal(stats[2] / stats[0]),
			ByteCountDecimal(stats[1]),
			ByteCountDecimal(stats[2]),
			stats[0],
		})
	}

	t.AppendFooter(table.Row{"Total", "", "", ByteCountDecimal(totalKeySize), ByteCountDecimal(totalValSize), totalKeys})

	t.Render()
}

func SortSlice[T constraints.Ordered](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func ByteCountDecimal(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := int64(b) / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
