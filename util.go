package main

import cool "github.com/StirlingMarketingGroup/cool-mysql"

// appendTable appends to the table array
func appendTable(src *cool.Database, t string, tables *[]string) {
	checkIfInSource(src, t)
	*tables = append(*tables, t)
}
