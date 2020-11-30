# OVSKV

Go library that presents high performance KV interface built on top of OVSDB.

## Features

* Implements generic key-value interface with support for hierarhical searches, e.g. /a/b/c => values
* Implements Go struct introspection API, e.g. mapping Go struct on top of hierarhical key-value interface
* Using OVSDB built-in conditional search to speed up key look up
* Implements native OVSDB Set and Map primitives as key's value

Examples:

* Hierarhical Key-Value interface
```golang
ovs, _ := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, nil)

// create hierarhy of keys
uuid, _ := ovs.SetKV("/a", "a")
uuid, _ := ovs.SetKV("/a/b", "b")
uuid, _ := ovs.SetKV("/a/b/c", "c")
uuid, _ := ovs.SetKV("/a/b/d", "d")

// retrieve /a/b, /a/b/c, /a/b/d
rows, _ := ovs.GetKV("includes", "/a/b")

// delete all
ovs.DeleteKV("includes", "")

ovs.Disconnect()
```

* Go struct introspection Save interface
```golang
type C struct {
	SubSubField1 string      `ovskv:"subfield1"`
}

type B struct {
	SubField1 string         `ovskv:"subfield1"`
	SubField2 C              `ovskv:"subfield2"`
}

type A struct {
	Field1 string            `ovskv:"field1"`
	Field2 int               `ovskv:"field2"`
	Field3 int64             `ovskv:"field3"`
	Field4 bool              `ovskv:"field4"`
	Field5 B                 `ovskv:"field5"`
	Field6 []B               `ovskv:"field6"`
}

...

a := A{
	Field1: "value1",
	Field2: 0,
	Field3: 123,
	Field4: false,
	Field5: B{
		SubField1: "value1",
		SubField2: C{
			SubSubField1: "value1",
		},
	},
	Field6: []B{
		{
			SubField1: "value1",
			SubField2: C{
				SubSubField1: "value1",
			},
		},
	},
}

ovs, _ := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &a)

// store the entire structure pointed by &a in key-value hierarhy
ovs.Save()

// modify one field
a.Field1 = "value1 changed3"

// update just one field
ovs.SaveField(&a.Field1)

ovs.Disconnect()
```

* Go struct introspection Load interface
```golang
ovs, _ := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &a)

// load &a from existing key-value hierarhy
ovs.Load()

// re-load just one field
ovs.LoadField(&a.Field1, "/field1")

ovs.Disconnect()
```

## Getting started

Steps to get library compiled and execute tests

### install deps
```
go get -u github.com/stretchr/testify/assert github.com/ebay/libovsdb
```

### create db
```
ovsdb-tool create ./testkv.db ./testkv.ovsschema
```

### start server
```
ovsdb-server --remote=ptcp:6641 --remote=tcp:127.0.0.1:6641 ./testkv.db &>/tmp/out &
```

### start tests and benchmarks
```
go test ovskv_test.go -v -bench

=== RUN   TestKV
--- PASS: TestKV (0.00s)
=== RUN   TestSaveAndLoad
Create Go struct, save it and load it back
--- PASS: TestSaveAndLoad (0.01s)
=== RUN   TestSaveField
Create Go struct with just one element, save it, modify it, save again and load it back
--- PASS: TestSaveField (0.00s)
goos: linux
goarch: amd64
BenchmarkStructSave
BenchmarkStructSave-8                854           1882251 ns/op           43223 B/op        863 allocs/op
BenchmarkStructLoad
BenchmarkStructLoad-8               1099           1203485 ns/op           38194 B/op       1019 allocs/op
BenchmarkKVSet
BenchmarkKVSet-8                    6025            307200 ns/op            7718 B/op        150 allocs/op
BenchmarkKVMGet
BenchmarkKVMGet-8                   3574            337058 ns/op            6289 B/op        136 allocs/op
PASS
```

### access it
```
ovsdb-client list-dbs tcp:127.0.0.1:6641
ovsdb-client dump tcp:127.0.0.1:6641 TestKV
```

### convert existing db to new schema version (bump version first)
```
ovsdb-tool convert ./testkv.db testkv.ovsschema
```
