package main

import (
	"errors"
	"os"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"gopkg.in/yaml.v2"
)

// getTables returns a map of tables
// that can be checked for groups of tables
// set by the configuation
func getTables(file string, inverse bool, args *[]string, src *mysql.Database) (*[]string, error) {
	var tables map[string][]string
	y, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	if y != nil {
		err = yaml.Unmarshal(y, &tables)
		if err != nil {
			return nil, err
		}
	}

	var tableNames []string
	checkTables(src, (*args)[2:], tables, &tableNames)

	if inverse {
		var newTableNames []string
		err = src.Select(&newTableNames, "select`table_name`"+
			"from`information_schema`.`TABLES`"+
			"where`table_schema`=database()"+
			"and`table_type`='BASE TABLE'{{ if .Tables }}"+
			"and`table_name`not in(@@Tables){{ end }}", 0, mysql.Params{
			"Tables": tableNames,
		})
		if err != nil {
			return nil, err
		}

		tableNames = newTableNames
	}

	return &tableNames, nil
}

func checkTables(src *mysql.Database, tableList []string, aliases map[string][]string, tableNames *[]string) {
	for _, t := range tableList {
		if alias, ok := aliases[t]; ok {
			checkTables(src, alias, aliases, tableNames)
		} else {
			appendTable(src, t, tableNames)
		}
	}
}
