package main

import mysql "github.com/StirlingMarketingGroup/cool-mysql"

// appendTable appends to the table array
func appendTable(src *mysql.Database, t string, tables *[]string) {
	checkIfInSource(src, t)
	*tables = append(*tables, t)
}
