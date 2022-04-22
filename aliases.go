package main

import (
	"fmt"
	"io/ioutil"

	cool "github.com/StirlingMarketingGroup/cool-mysql"
	"gopkg.in/yaml.v2"
)

// getTables returns a map of tables
// that can be checked for groups of tables
// set by the configuation
func getTables(file string, args *[]string, src *cool.Database) (*[]string, error) {
	var tables map[string][]string
	y, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("failed to read aliases file\ndefault location for config is %s/swoof/connections.yaml\n", confDir)
	} else {
		err = yaml.Unmarshal(y, &tables)
		if err != nil {
			return nil, err
		}
	}

	var tableNames []string
	checkTables(src, (*args)[2:], tables, &tableNames)

	return &tableNames, nil
}

func checkTables(src *cool.Database, tableList []string, aliases map[string][]string, tableNames *[]string) {
	for _, t := range tableList {
		if alias, ok := aliases[t]; ok {
			checkTables(src, alias, aliases, tableNames)
		} else {
			appendTable(src, t, tableNames)
		}
	}
}
