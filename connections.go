package main

import (
	"io/ioutil"
	"net/url"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
)

type connection struct {
	User   string            `yaml:"user"`
	Pass   string            `yaml:"pass"`
	Host   string            `yaml:"host"`
	Schema string            `yaml:"schema"`
	Params map[string]string `yaml:"params"`

	SourceOnly bool `yaml:"source_only"`
	DestOnly   bool `yaml:"dest_only"`
}

// getConnections returns our connection map that's
// parsed from the user's config dir,
// makes calls to swoof much shorter and much easier
// and even a little safer potentially
func getConnections(file string) (connections map[string]connection, err error) {
	y, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(y, &connections)
	if err != nil {
		return nil, err
	}

	return
}

// connectionToDSN converts our own connection structs to the
// official mysql's own connection struct, formatted for our use case
func connectionToDSN(c connection) string {
	d := mysql.NewConfig()
	d.User = c.User
	d.Passwd = c.Pass
	d.Net = "tcp"
	d.Addr = c.Host
	d.DBName = c.Schema

	dsn := d.FormatDSN()
	i := 0
	for k, v := range c.Params {
		if i == 0 {
			dsn += "?"
		} else {
			dsn += "&"
		}
		dsn += url.QueryEscape(k) + "=" + url.QueryEscape(v)
		i++
	}

	return dsn
}
