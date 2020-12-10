package main

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/dustin/go-humanize/english"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/posener/cmd"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

var confDir, _ = os.UserConfigDir()

var (
	root = cmd.New()

	connectionsFile = root.String("c", confDir+"/swoof/connections.yaml", "your connections file")

	skipData = root.Bool("n", false, "drop/create tables and triggers only, without importing data")

	threads = root.Int("t", 4, "max concurrent tables at the same time, import stability may vary wildly between servers while increasing this")

	// not entirely sure how much this really affects performance,
	// since the performance bottleneck is almost guaranteed to be writing
	// the rows to the source
	rowBufferSize = root.Int("r", 50, "max rows buffer size. Will have this many rows downloaded and ready for importing")

	tempTablePrefix = root.String("p", "_swoof_", "prefix of the temp table used for initial creation before the swap and drop")

	args = root.Args("source, dest, tables", "source, dest, tables, ex:\n"+
		"swoof [flags] 'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3\n\n"+
		"see: https://github.com/go-sql-driver/mysql#dsn-data-source-name\n\n"+
		"Or, optionally, you can use your connections in your connections file like so:\n\n"+
		"swoof [flags] production localhost table1 table2 table3")
)

func main() {
	start := time.Now()

	root.ParseArgs(os.Args...)
	if len(*args) < 3 {
		root.Usage()
		os.Exit(1)
	}

	// guard channel of structs makes sure we can easily block for
	// running only a max number of goroutines at a time
	var guard = make(chan struct{}, *threads)

	sourceDSN := (*args)[0]
	destDSN := (*args)[1]

	// lookup connection information in the users config file
	// for much easier and shorter (and probably safer) command usage
	if connections, err := getConnections(*connectionsFile); err == nil {
		if c, ok := connections[sourceDSN]; ok {
			if c.DestOnly {
				panic(errors.Errorf("can't use %q as a source per your config", sourceDSN))
			}

			sourceDSN = connectionToDSN(c)
		}

		if c, ok := connections[destDSN]; ok {
			if c.SourceOnly {
				panic(errors.Errorf("can't use %q as a destination per your config", destDSN))
			}

			destDSN = connectionToDSN(c)
		}
	}

	// source connection is the first argument
	// this is where our rows are coming from
	src, err := mysql.NewFromDSN(sourceDSN, sourceDSN)
	if err != nil {
		panic(err)
	}

	for _, t := range (*args)[2:] {
		if ok, err := src.Exists("show tables like'"+t+"'", 0); err != nil {
			panic(err)
		} else if !ok {
			panic(errors.Errorf("table %q does not exist on the source connection", t))
		}
	}

	tables := make(chan struct {
		TableName string `mysql:"table_name"`
	})
	err = src.Select(tables, "select`table_name`"+
		"from`information_schema`.`TABLES`"+
		"where`table_schema`=database()"+
		"and`table_name`in(@@Tables)"+
		"order by`data_length`+`index_length`desc", 0, mysql.Params{
		"Tables": (*args)[2:],
	})
	if err != nil {
		panic(err)
	}

	// our multi-progress bar ties right into our wait group
	var wg sync.WaitGroup
	pb := mpb.New(mpb.WithWaitGroup(&wg))

	for table := range tables {
		// this makes sure we capture tableName in a way that it doesn't
		// change on us within our loop
		// And IMO this is cleaner than having the func below accept the string
		tableName := table.TableName

		// ensure we only run up to our max imports at a time
		guard <- struct{}{}

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
				// All of the field types are pointers so that our mysql scanning
				// handles null values gracefully
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
					// our cool mysql literal is exactly what it sounds like;
					// passed directly into the query with no escaping, which is know is
					// safe here because a decimal from mysql can't contain breaking characters
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
			// pooling, almost creating our own "pool"
			dst, err := mysql.NewFromDSN(destDSN, destDSN)
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

			// now we get the table creation syntax from our source
			var table struct {
				CreateMySQL string `mysql:"Create Table"`
			}
			err = src.Select(&table, "show create table`"+tableName+"`", 0)
			if err != nil {
				panic(err)
			}

			tempTableName := *tempTablePrefix + tableName

			// delete the table from our destination
			err = dst.Exec("drop table if exists`" + tempTableName + "`")
			if err != nil {
				panic(err)
			}

			// since foreign key constraints have globally unique names (for some reason)
			// we can't just create our temp table with constraints because
			// the names will likely conflict with the table that already exists

			// so we will strip the constraints here and add them back once we're done
			var constraints string

			// we can safely assume the constraints start like this because you can't
			// constraints without columns!
			constraintsStart := strings.Index(table.CreateMySQL, ",\n  CONSTRAINT ")
			if constraintsStart != -1 {
				// we have the start of our constraints block, and since mysql
				// always (hopefully) gives them in a block, we can find the last
				// constraint and everything in the middle is what we want
				constraintsEnd := strings.LastIndex(table.CreateMySQL, ",\n  CONSTRAINT ")

				// but we need the end of the line, so we'll get the byte index of the newline
				// after our last index as our end marker
				constraintsEnd = constraintsEnd + strings.IndexByte(table.CreateMySQL[constraintsEnd+2:], '\n') + 2

				// then we can keep track of our constraints so we can add them back
				// to our table once we've dropped the original table
				constraints = table.CreateMySQL[constraintsStart:constraintsEnd]

				// and store our create query without our constraints
				table.CreateMySQL = table.CreateMySQL[:constraintsStart] + table.CreateMySQL[constraintsEnd:]
			}

			// now we can make the table on our destination
			err = dst.Exec("CREATE TABLE `" + tempTableName + "`" + strings.TrimPrefix(table.CreateMySQL, "CREATE TABLE `"+tableName+"`"))
			if err != nil {
				panic(err)
			}

			if !*skipData {
				// and if we aren't skipping the data, start the import!
				// Now this *does* have to be chunked because there's no way to stream
				// rows to mysql, but cool mysql handles this for us, all it needs is the same
				// channel we got from the select
				err = dst.InsertWithRowComplete("insert into`"+tempTableName+"`", columnNames, ch, func(start time.Time) {
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

			// drop the old table now that our temp table is done
			err = dst.Exec("drop table if exists`" + tableName + "`")
			if err != nil {
				panic(err)
			}

			// rename our temp table to the real table name
			// we could do an atomic rename here, but the problem is that atomic renames
			// also rename all the constraints of other tables pointing to our original table, and
			// we want those constraints to point to our new table instead

			// if you're doing this live, there *is* some down time, but other tools handle this the same
			// way, so I don't think it's unreasonable if we do the same
			err = dst.Exec("alter table`" + tempTableName + "`rename`" + tableName + "`")
			if err != nil {
				panic(err)
			}

			// no we can add back our constraints if we have them
			// converting our constraints to alter table syntax by removing our leading
			// comma and adding the word "add" at the beginning of each line
			if len(constraints) != 0 {
				err = dst.Exec("alter table`" + tableName + "`" + strings.ReplaceAll(strings.TrimLeft(constraints, ","), "\n", "\nadd"))
				if err != nil {
					panic(err)
				}
			}

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

	fmt.Println("finished importing", len(tables), english.PluralWord(len(tables), "table", ""), "in", time.Since(start))
}
