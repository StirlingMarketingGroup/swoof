package main

import (
	"os"
	"reflect"
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

var (
	root = cmd.New()

	skipData = root.Bool("n", false, "drop/create tables and triggers only, without importing data")

	threads = root.Int("t", 4, "max concurrent tables at the same time. Anything more than 4 seems to crash things")

	// not entirely sure how much this really affects performance,
	// since the performance bottleneck is almost guaranteed to be writing
	// the rows to the source
	rowBufferSize = root.Int("r", 50, "max rows buffer size. Will have this many rows downloaded and ready for importing")

	args = root.Args("source, dest, tables", "source, dest, tables, ex:\n'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3\n\nsee: https://github.com/go-sql-driver/mysql#dsn-data-source-name")
)

func main() {
	root.ParseArgs(os.Args...)
	if len(*args) < 3 {
		root.Usage()
		os.Exit(1)
	}

	// guard channel of structs makes sure we can easily block for
	// running only a max number of goroutines at a time
	var guard = make(chan struct{}, *threads)

	// we're making a table map so we can efficiently de-duplicate the list of tables
	tablesMap := make(map[string]struct{})
	for _, t := range (*args)[2:] {
		tablesMap[t] = struct{}{}
	}

	// source connection is the first argument
	// this is where our rows are coming from
	src, err := mysql.NewFromDSN((*args)[0], (*args)[0])
	if err != nil {
		panic(err)
	}

	// our multi-progress bar ties right into our wait group
	var wg sync.WaitGroup
	pb := mpb.New(mpb.WithWaitGroup(&wg))

	for tableName := range tablesMap {
		// ensure we only run up to our max imports at a time
		guard <- struct{}{}

		// this makes sure we capture tableName in a way that it doesn't
		// change on us within our loop
		// And IMO this is cleaner than having the func below accept the string
		tableName := tableName
		wg.Add(1)

		go func() {
			defer wg.Done()

			// once this function is done, let another task take a place
			// in our guard
			defer func() { <-guard }()

			// the mysql lib we're using, cool mysql, lets us define a
			// chan of structs to read our rows into, for awesome
			// performance and type safety. The tags are the actual column names
			columns := make(chan struct {
				ColumnName           string `mysql:"COLUMN_NAME"`
				Position             int    `mysql:"ORDINAL_POSITION"`
				DataType             string `mysql:"DATA_TYPE"`
				ColumnType           string `mysql:"COLUMN_TYPE"`
				GenerationExpression string `mysql:"GENERATION_EXPRESSION"`
			})

			// in this query we're simply getting all the details about our column names
			// so we can make a dynamic struct that the rows can fit into
			err := src.Select(columns, "select`COLUMN_NAME`,`ORDINAL_POSITION`,`DATA_TYPE`,`COLUMN_TYPE`,"+
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

			// this is our dynamic struct of the actual row, which will have
			// properties added to it for each column in the following loop
			rowStruct := dynamicstruct.NewStruct()

			// this is our string builder for quoted column names,
			// which will be used in our select statement
			columnsQuotedBld := new(strings.Builder)

			// and because our cool mysql insert functions needs a slice of
			// column names, we'll need to keep track of those as we loop through them
			columnNames := make([]string, 0)

			// this slice will be our index of struct field names
			// we aren't naming our struct properties with the actual column names
			// because they can contain all sorts of weird characters,
			// and we also need uppercase starting letters so the other libraries
			// like cool mysql can actually access them
			colStructFields := make([]string, 0)

			i := 0

			// read c from our channel of columns, where c is our column struct
			// cool thing about cool mysql channel selecting, is that the selected row
			// only exists in memory during its loop here, only one at a time
			for c := range columns {
				// you can't insert into generated columns, and mysql will actually
				// throw errors if you try and do this, so we simply skip them altogether
				// and imagine they don't exist
				if len(c.GenerationExpression) != 0 {
					continue
				}

				columnNames = append(columnNames, c.ColumnName)

				// column string should be like "`Column1`,`Column2`..."
				if i != 0 {
					columnsQuotedBld.WriteByte(',')
				}
				columnsQuotedBld.WriteByte('`')
				columnsQuotedBld.WriteString(c.ColumnName)
				columnsQuotedBld.WriteByte('`')

				// column type will end with "unsigned" if the unsigned flag is set for
				// this column, used for unsigned integers
				unsigned := strings.HasSuffix(c.ColumnType, "unsigned")

				// these are our struct fields, which all look like "F0", "F1", etc
				f := "F" + strconv.Itoa(c.Position)
				// and we have to keep track of their order because dynamic struct uses
				// a map internally and these fields will be in random orders once we
				// build the struct
				colStructFields = append(colStructFields, f)

				// create the tag for the field with the exact column name so that
				// cool mysql insert func knows how to map the row values
				tag := `mysql:"` + c.ColumnName + `"`

				// the switch through data types (differnet than column types, doesn't include lengths)
				// to determine the type of our struct field
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
					// the json type here is important, because mysql needs
					// char set info for json columns, since json is supposed to be utf8,
					// and go treats this is bytes for some reason. mysql.JSON lets cool mysql
					// know to surround the inlined value with charset info
					var v *mysql.JSON
					rowStruct.AddField(f, v, tag)
				default:
					panic(errors.Errorf("unknown mysql column of type %q", c.ColumnType))
				}

				i++
			}

			// this gets the "type" of our struct from our dynamic struct
			structType := reflect.Indirect(reflect.ValueOf(rowStruct.Build().New())).Type()
			// and then we make a channel with reflection for our new type of struct
			ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, structType), *rowBufferSize).Interface()

			columnsQuoted := columnsQuotedBld.String()

			// we make the dest connection in the loop, once per table, because
			// we need to be able to set foreign keys off, and the connection pooling
			// by default makes this difficult. So instead we declare it here, and turn off
			// pooling, almost creating our own "ppol"
			dst, err := mysql.NewFromDSN((*args)[1], (*args)[1])
			if err != nil {
				panic(err)
			}
			// disables the pooling
			dst.Writes.SetMaxOpenConns(1)

			// we need to disable this because importing two tables at the same time
			// will almost certainly run into problems otherwise. Also dropping the table
			// will do the same
			err = dst.Exec("set`FOREIGN_KEY_CHECKS`=0")
			if err != nil {
				panic(err)
			}

			// delete the table from our destination
			err = dst.Exec("drop table if exists`" + tableName + "`")
			if err != nil {
				panic(err)
			}

			// now we get the table creation syntax from our source
			var table struct {
				CreateMySQL string `mysql:"Create Table"`
			}
			err = src.Select(&table, "show create table`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			// and get the count, so we can show are swick progress bars
			var count struct {
				Count int64
			}
			err = src.Select(&count, "select count(*)`Count`from`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			// oh yeah, that's just one query. We don't actually have to chunk this selection
			// because we're dealing with rows as they come in, instead of tryign to select them
			// all into memory or something first, which makes this code dramatically simpler
			// and should work with tables of all sizes
			err = src.Select(ch, "select"+columnsQuoted+"from`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			// our pretty bar config for the progress bars
			// their documention lives over here https://github.com/vbauerster/mpb
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

			// now we can make the table on our destination
			err = dst.Exec(table.CreateMySQL)
			if err != nil {
				panic(err)
			}

			if !*skipData {
				// and if we aren't skipping the data, start the import!
				// Now this *does* have to be chunked because there's no way to stream
				// rows to mysql, but cool mysql handles this for us, all it needs is the same
				// channel we got from the select
				err = dst.InsertWithRowComplete("insert into`"+tableName+"`", columnNames, ch, func(start time.Time) {
					bar.Increment()
					bar.DecoratorEwmaUpdate(time.Since(start))
				})
				if err != nil {
					panic(err)
				}
			}

			// and just in case the rows have changed count since our count selection,
			// we'll just tell the progress bar that we're finished
			bar.SetTotal(bar.Current(), true)

			// but we can't forget our triggers!
			// lets grab the triggers from the source table and make sure
			// we re-create them all on our destination
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
