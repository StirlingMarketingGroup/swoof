package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// getTables returns a map of tables
// that can be checked for groups of tables
// set by the configuation
func getTables(file string) (tables map[string][]string, err error) {
	y, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(y, &tables)
	if err != nil {
		return nil, err
	}

	return
}
