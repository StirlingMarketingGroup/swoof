package main

import (
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"testing"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
)

const benchInsertQuery = "insert into`t`(`id`,`created_at`,`updated_at`,`name`,`email`,`description`,`price`,`qty`,`flags`,`metadata`)"

// BenchmarkInsertRowBuild measures the in-process cost of turning a slice of
// dynamically-typed rows into INSERT SQL and shipping it to an io.Discard sink.
// Slice source isolates the row-build / marshal cost from the reflect.Chan
// scheduler ping-pong we see in production.
//
// Column mix approximates a typical business table: mixed int widths, strings
// (hex-encoded with utf8mb4 wrapper), a JSON column, a decimal passed as
// mysql.Raw, and a binary blob.
func BenchmarkInsertRowBuild(b *testing.B) {
	structType := buildBenchStructType()
	rowsPerBatch := 5_000

	db, err := mysql.NewWriter(io.Discard)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-build the slice once so the benchmark measures insert cost, not
	// allocations for the row contents.
	sliceType := reflect.SliceOf(structType)
	rows := reflect.MakeSlice(sliceType, rowsPerBatch, rowsPerBatch)
	for i := 0; i < rowsPerBatch; i++ {
		rows.Index(i).Set(makeBenchRow(structType, i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := db.I().InsertContext(b.Context(),
			benchInsertQuery, rows.Interface()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertRowBuildChan layers the reflect.Chan ping-pong back on, so we
// can see how much of the in-production cost is scheduler / channel versus
// the marshal path itself. Same rows, same sink — row generation is hoisted
// out of b.Loop so we measure channel + insert, not makeBenchRow.
func BenchmarkInsertRowBuildChan(b *testing.B) {
	structType := buildBenchStructType()
	rowsPerBatch := 5_000

	db, err := mysql.NewWriter(io.Discard)
	if err != nil {
		b.Fatal(err)
	}

	sliceType := reflect.SliceOf(structType)
	rows := reflect.MakeSlice(sliceType, rowsPerBatch, rowsPerBatch)
	for i := 0; i < rowsPerBatch; i++ {
		rows.Index(i).Set(makeBenchRow(structType, i))
	}

	chanType := reflect.ChanOf(reflect.BothDir, structType)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		ch := reflect.MakeChan(chanType, 10_000)

		go func() {
			defer ch.Close()
			for i := 0; i < rowsPerBatch; i++ {
				ch.Send(rows.Index(i))
			}
		}()

		if err := db.I().InsertContext(b.Context(),
			benchInsertQuery, ch.Interface()); err != nil {
			b.Fatal(err)
		}
	}
}

func buildBenchStructType() reflect.Type {
	s := dynamicstruct.NewStruct()
	// F1..F10, positionally — same shape main.go builds from INFORMATION_SCHEMA.
	s.AddField("F1", new([]byte), `mysql:"id"`)
	s.AddField("F2", new(string), `mysql:"created_at"`)
	s.AddField("F3", new(string), `mysql:"updated_at"`)
	s.AddField("F4", new(string), `mysql:"name"`)
	s.AddField("F5", new(string), `mysql:"email"`)
	s.AddField("F6", new(string), `mysql:"description"`)
	s.AddField("F7", new(mysql.Raw), `mysql:"price"`)
	s.AddField("F8", new(uint32), `mysql:"qty"`)
	s.AddField("F9", new(int8), `mysql:"flags"`)
	s.AddField("F10", new(json.RawMessage), `mysql:"metadata"`)
	return reflect.TypeOf(s.Build().New()).Elem()
}

func makeBenchRow(t reflect.Type, i int) reflect.Value {
	v := reflect.New(t).Elem()

	id := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24), 0, 0, 0, 1}
	ts := "2026-04-24 17:27:14.000000"
	name := "Customer " + strconv.Itoa(i)
	email := "customer" + strconv.Itoa(i) + "@example.com"
	desc := "Some reasonably long description text that is representative of a real varchar/text column full of plain ASCII content."
	price := mysql.Raw("19.99")
	qty := uint32(i)
	flags := int8(i % 8)
	meta := json.RawMessage(`{"source":"bench","idx":` + strconv.Itoa(i) + `}`)

	v.Field(0).Set(reflect.ValueOf(&id))
	v.Field(1).Set(reflect.ValueOf(&ts))
	v.Field(2).Set(reflect.ValueOf(&ts))
	v.Field(3).Set(reflect.ValueOf(&name))
	v.Field(4).Set(reflect.ValueOf(&email))
	v.Field(5).Set(reflect.ValueOf(&desc))
	v.Field(6).Set(reflect.ValueOf(&price))
	v.Field(7).Set(reflect.ValueOf(&qty))
	v.Field(8).Set(reflect.ValueOf(&flags))
	v.Field(9).Set(reflect.ValueOf(&meta))

	return v
}
