package main

import (
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/posener/cmd"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

type yesNo bool

func (yn *yesNo) Scan(src interface{}) error {
	switch v := src.(type) {
	case []uint8:
		switch string(v) {
		case "YES":
			*yn = true
			return nil
		case "NO":
			*yn = false
			return nil
		}
	}

	return errors.Errorf("cannot scan %q of type %T as a yes/no", src, src)
}

var keysStartRegexp = regexp.MustCompile(`,\n {2}KEY`)
var keysEndRegexp = regexp.MustCompile(`\).+$`)
var fulltextsRegexp = regexp.MustCompile(`(?m)^add {2}FULLTEXT .+,?$`)

var (
	root = cmd.New()

	skipData = root.Bool("n", false, "drop/create tables and triggers only, without importing data")

	threads = root.Int("t", 4, "max concurrent tables at the same time. Anything more than 4 seems to crash things")

	rowBufferSize = root.Int("r", 50, "max rows buffer size. Will have this many rows downloaded and ready for importing")

	args = root.Args("source, dest, tables", "source, dest, tables, ex:\n'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3\n\nsee: https://github.com/go-sql-driver/mysql#dsn-data-source-name")
)

func main() {
	root.ParseArgs(os.Args...)

	var guard = make(chan struct{}, *threads)

	tablesMap := make(map[string]struct{})
	for _, t := range (*args)[2:] {
		tablesMap[t] = struct{}{}
	}
	tables := make([]string, 0, len(tablesMap))
	for t := range tablesMap {
		tables = append(tables, t)
	}

	if len(*args) < 3 {
		root.Usage()
		os.Exit(1)
	}

	src, err := mysql.NewFromDSN((*args)[0], (*args)[0])
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	pb := mpb.New(mpb.WithWaitGroup(&wg))

	for _, tableName := range tables {
		guard <- struct{}{}

		tableName := tableName
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-guard }()

			columns := make(chan struct {
				ColumnName           string `mysql:"COLUMN_NAME"`
				Position             int    `mysql:"ORDINAL_POSITION"`
				Nullable             yesNo  `mysql:"IS_NULLABLE"`
				DataType             string `mysql:"DATA_TYPE"`
				ColumnType           string `mysql:"COLUMN_TYPE"`
				GenerationExpression string `mysql:"GENERATION_EXPRESSION"`
			})
			err := src.Select(columns, "select`COLUMN_NAME`,`ORDINAL_POSITION`,`IS_NULLABLE`,`DATA_TYPE`,`COLUMN_TYPE`,"+
				"`GENERATION_EXPRESSION`"+
				"from`INFORMATION_SCHEMA`.columns "+
				"where`TABLE_SCHEMA`=database()"+
				"and table_name=@@TableName "+
				"order by`ORDINAL_POSITION`", 0, mysql.Params{
				"TableName": tableName,
			})
			if err != nil {
				panic(err)
			}

			rowStruct := dynamicstruct.NewStruct()
			columnsQuotedBld := new(strings.Builder)
			columnNames := make([]string, 0)
			colStructFields := make([]string, 0)
			i := 0
			for c := range columns {
				if len(c.GenerationExpression) != 0 {
					continue
				}

				columnNames = append(columnNames, c.ColumnName)

				if i != 0 {
					columnsQuotedBld.WriteByte(',')
				}
				columnsQuotedBld.WriteByte('`')
				columnsQuotedBld.WriteString(c.ColumnName)
				columnsQuotedBld.WriteByte('`')

				unsigned := strings.HasSuffix(c.ColumnType, "unsigned")

				f := "F" + strconv.Itoa(c.Position)
				colStructFields = append(colStructFields, f)

				tag := `mysql:"` + c.ColumnName + `"`

				switch c.DataType {
				case "tinyint":
					if unsigned {
						var v *uint8
						rowStruct.AddField(f, v, tag)
					} else {
						var v *int8
						rowStruct.AddField(f, v, tag)
					}
				case "smallint":
					if unsigned {
						var v *uint16
						rowStruct.AddField(f, v, tag)
					} else {
						var v *int16
						rowStruct.AddField(f, v, tag)
					}
				case "int", "mediumint":
					if unsigned {
						var v *uint32
						rowStruct.AddField(f, v, tag)
					} else {
						var v *int32
						rowStruct.AddField(f, v, tag)
					}
				case "bigint":
					if unsigned {
						var v *uint64
						rowStruct.AddField(f, v, tag)
					} else {
						var v *int64
						rowStruct.AddField(f, v, tag)
					}
				case "float":
					var v *float64
					rowStruct.AddField(f, v, tag)
				case "decimal":
					var v *mysql.Literal
					rowStruct.AddField(f, v, tag)
				case "timestamp", "date", "datetime":
					var v *string
					rowStruct.AddField(f, v, tag)
				case "binary", "varbinary", "blob", "mediumblob":
					var v *[]byte
					rowStruct.AddField(f, v, tag)
				case "char", "varchar", "text", "mediumtext":
					var v *string
					rowStruct.AddField(f, v, tag)
				case "json":
					var v *mysql.JSON
					rowStruct.AddField(f, v, tag)
				default:
					panic(errors.Errorf("unknown mysql column of type %q", c.ColumnType))
				}

				i++
			}

			structType := reflect.Indirect(reflect.ValueOf(rowStruct.Build().New())).Type()
			ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, structType), *rowBufferSize)

			columnsQuoted := columnsQuotedBld.String()

			dst, err := mysql.NewFromDSN((*args)[1], (*args)[1])
			if err != nil {
				panic(err)
			}
			dst.Writes.SetMaxOpenConns(1)

			err = dst.Exec("set`FOREIGN_KEY_CHECKS`=0")
			if err != nil {
				panic(err)
			}

			err = dst.Exec("drop table if exists`" + tableName + "`")
			if err != nil {
				panic(err)
			}

			var table struct {
				CreateMySQL string `mysql:"Create Table"`
			}
			err = src.Select(&table, "show create table`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			start := keysStartRegexp.FindStringIndex(table.CreateMySQL)
			end := keysEndRegexp.FindStringIndex(table.CreateMySQL)
			var keys string
			if len(start) > 0 && len(end) > 0 {
				keys = table.CreateMySQL[start[0]:end[0]]
				table.CreateMySQL = table.CreateMySQL[:start[0]] + table.CreateMySQL[end[0]:]
			}

			err = dst.Exec(table.CreateMySQL)
			if err != nil {
				panic(err)
			}

			var count struct {
				Count int64
			}
			err = src.Select(&count, "select count(*)`Count`from`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			chIface := ch.Interface()
			err = src.Select(chIface, "select"+columnsQuoted+"from`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			bar := pb.AddBar(count.Count,
				mpb.BarStyle("|▇▇ |"),
				mpb.PrependDecorators(
					decor.Name(color.HiBlueString(tableName)),
					decor.OnComplete(decor.Percentage(decor.WC{W: 5}), color.HiMagentaString(" done!")),
				),
				mpb.AppendDecorators(
					decor.CountersNoUnit("( "+color.HiCyanString("%d/%d")+", ", decor.WCSyncWidth),
					decor.AverageSpeed(-1, " "+color.HiGreenString("%.2f/s")+" ) ", decor.WCSyncWidth),
					decor.AverageETA(decor.ET_STYLE_MMSS),
				),
			)

			if !*skipData {
				err = dst.InsertWithRowComplete("insert into`"+tableName+"`", columnNames, chIface, func(start time.Time) {
					bar.Increment()
					bar.DecoratorEwmaUpdate(time.Since(start))
				})
				if err != nil {
					panic(err)
				}
			}

			bar.SetTotal(bar.Current(), true)

			if len(keys) != 0 {
				keys = "alter table`" + tableName + "`" + strings.ReplaceAll(keys[1:len(keys)-1], "\n", "\nadd")
				locs := fulltextsRegexp.FindAllStringIndex(keys, -1)
				if len(locs) > 1 {
					locs = locs[1:]
					for i := len(locs) - 1; i >= 0; i-- {
						l := locs[i]

						err := dst.Exec("alter table`" + tableName + "`" + strings.TrimRight(keys[l[0]:l[1]], ","))
						if err != nil {
							panic(err)
						}
						keys = keys[:l[0]] + keys[l[1]:]
					}
				}

				err := dst.Exec(keys)
				if err != nil {
					panic(err)
				}
			}

			triggers := make(chan struct {
				Trigger string
			})
			err = src.Select(triggers, "show triggers where`table`='"+tableName+"'", 0)
			if err != nil {
				panic(err)
			}
			for r := range triggers {
				var trigger struct {
					CreateMySQL string `mysql:"SQL Original Statement"`
				}
				err := src.Select(&trigger, "show create trigger`"+r.Trigger+"`", 0)
				if err != nil {
					panic(err)
				}

				err = dst.Exec(trigger.CreateMySQL)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	pb.Wait()
}
