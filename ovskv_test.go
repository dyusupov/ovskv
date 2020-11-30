package ovskv

import (
	"testing"
	"fmt"
	"strconv"
	"strings"
	"time"
	"math/rand"

	"github.com/stretchr/testify/assert"

	"."
)

const (
	DB_NAME string = "TestKV"
	DB_CONNECT string = "tcp:127.0.0.1:6641"
	DB_NAMESPACE string = "Zone_"
)

func TestKV(t *testing.T) {
	fmt.Println("Verify *KV and *KVM interfaces")
	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, nil)
	assert.Equal(t, err, nil)

	_, err = ovs.InsertKV("t", "t")
	assert.Equal(t, err, nil)

	_, err = ovs.InsertKV("t", "t")
	assert.NotEqual(t, err, nil)

	rows, err := ovs.GetKV("==", "t")
	assert.Equal(t, err, nil)
	assert.Equal(t, "t", rows[0]["value"])

	_, err = ovs.InsertKV("/1", "/1")
	assert.Equal(t, err, nil)

	_, err = ovs.InsertKV("/1", "/1")
	assert.NotEqual(t, err, nil)

	rows, err = ovs.GetKV("==", "/1")
	assert.Equal(t, err, nil)
	assert.Equal(t, "/1", rows[0]["value"])

	_, err = ovs.SetKVM("Test1", map[string]string{"Description":"desc1"})
	assert.Equal(t, err, nil)
	_, err = ovs.SetKVM("Test1/Tenants", map[string]string{"Description":"desc2"})
	assert.Equal(t, err, nil)
	_, err = ovs.SetKVM("Test1/Tenants/Foo", map[string]string{"Description":"desc3","par1":"val1"})
	assert.Equal(t, err, nil)
	_, err = ovs.SetKVM("Test1/Tenants/Foo/Chassis/1", map[string]string{"Description":"desc3"})
	assert.Equal(t, err, nil)
	_, err = ovs.SetKV("Test1/Tenants/Foo/Chassis/1/NetInterfaces/1", "val1")
	assert.Equal(t, err, nil)
	_, err = ovs.SetKV("Test1/Tenants/Foo/Chassis/1/NetInterfaces/2", "val2")
	assert.Equal(t, err, nil)
	_, err = ovs.SetKVM("Test2", map[string]string{"Description":"desc1"})
	assert.Equal(t, err, nil)
	_, err = ovs.SetKVM("Test2/Tenants/Foo", map[string]string{"Description":"desc3","par1":"val1"})
	assert.Equal(t, err, nil)

	rows, err = ovs.GetKV("includes", "Test1/Tenants/Foo/Chassis/1")
	found := false
	for i, _ := range rows {
		if rows[i]["key"] == "Test1/Tenants/Foo/Chassis/1/NetInterfaces/2" {
			found = true
			break
		}
	}
	assert.Equal(t, true, found)

	rows, err = ovs.GetKV("includes", "Test2")
	found = false
	for i, _ := range rows {
		if rows[i]["key"] == "Test2/Tenants/Foo" {
			found = true
			break
		}
	}
	assert.Equal(t, true, found)

	rows, err = ovs.GetKV("==", "Test1/Tenants/Foo/Chassis/1/NetInterfaces/1")
	assert.Equal(t, 1, len(rows))

	_, err = ovs.DeleteKV("excludes", "")
	assert.Equal(t, err, nil)

	_, err = ovs.DeleteKV("includes", "")
	assert.Equal(t, err, nil)

        ovs.Disconnect()
}

type C struct {
	SubSubField1 string         `ovskv:"subfield1"`
}

type B struct {
	SubField1 string         `ovskv:"subfield1"`
	SubField2 C              `ovskv:"subfield2"`
}

type A struct {
	Field1 string             `ovskv:"field1"`
	Field2 int                `ovskv:"field2"`
	Field3 int64              `ovskv:"field3"`
	Field4 bool               `ovskv:"field4"`
	Field5 B                  `ovskv:"field5"`
	Field6 []string           `ovskv:"field6"`
	Field7 []B                `ovskv:"field7"`
	Field8 map[string]B       `ovskv:"field8"`
	Field9 []int              `ovskv:"field9"`
	Field10 []int64           `ovskv:"field10"`
	Field11 []bool            `ovskv:"field11"`
	Field12 map[string]string `ovskv:"field12"`
	Field13 map[string]int    `ovskv:"field13"`
}

func TestSaveAndLoad(t *testing.T) {
	fmt.Println("Create Go struct, save it and load it back")
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
		Field6: []string{"value1", "value2"},
		Field7: []B{
			{
				SubField1: "value1-B0",
				SubField2: C{
					SubSubField1: "value1-B0",
				},
			},
			{
				SubField1: "value1-B1",
				SubField2: C{
					SubSubField1: "value1-B1",
				},
			},
		},
		Field8: map[string]B{
			"test 1": {
				SubField1: "value1-B0",
				SubField2: C{
					SubSubField1: "value1-B0",
				},
			},
			"test 2": {
				SubField1: "value1-B1",
				SubField2: C{
					SubSubField1: "value1-B1",
				},
			},
		},
		Field9: []int{1, 2},
		Field10: []int64{1, 2},
		Field11: []bool{true, false},
		Field12: map[string]string{"key 1":"value1", "key 2":"value2"},
		Field13: map[string]int{"k 1":1, "k 2":2},
	}

	// save
	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &a)
	assert.Equal(t, err, nil)

	err = ovs.Save()
	assert.Equal(t, err, nil)

	// change and reload /field5
	a.Field5.SubField2.SubSubField1 = "changed value2"
	err = ovs.LoadField(&a.Field5, "/field5")
	assert.Equal(t, err, nil)

	rows, err := ovs.GetKV("==", "/field5/subfield2/subfield1")
	assert.Equal(t, err, nil)
	assert.Equal(t, "value1", rows[0]["value"])

	// change and reload /field6 array element
	a.Field7[1].SubField2.SubSubField1 = "changed value2"
	err = ovs.LoadField(&a.Field7[1].SubField2, "/field7/1/subfield2")
	assert.Equal(t, err, nil)

	rows, err = ovs.GetKV("==", "/field7/1/subfield2/subfield1")
	assert.Equal(t, err, nil)
	assert.Equal(t, "value1-B1", rows[0]["value"])

        ovs.Disconnect()

	var b A

	// load on start
	ovs, err = ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &b)
	assert.Equal(t, err, nil)

	err = ovs.Load()
	assert.Equal(t, err, nil)

	assert.Equal(t, "value1", b.Field1)
	assert.Equal(t, 0, b.Field2)
	assert.Equal(t, int64(123), b.Field3)
	assert.Equal(t, false, b.Field4)
	assert.Equal(t, "value1", b.Field5.SubField2.SubSubField1)
	assert.Equal(t, "value1", b.Field6[0])
	assert.Equal(t, "value2", b.Field6[1])
	assert.Equal(t, "value1-B0", b.Field7[0].SubField2.SubSubField1)
	assert.Equal(t, "value1-B1", b.Field7[1].SubField2.SubSubField1)
	assert.Equal(t, "value1-B0", b.Field8["test 1"].SubField2.SubSubField1)
	assert.Equal(t, "value1-B1", b.Field8["test 2"].SubField2.SubSubField1)
	assert.Equal(t, 1, b.Field9[0])
	assert.Equal(t, 2, b.Field9[1])
	assert.Equal(t, int64(1), b.Field10[0])
	assert.Equal(t, int64(2), b.Field10[1])
	assert.Equal(t, true, b.Field11[0])
	assert.Equal(t, false, b.Field11[1])
	assert.Equal(t, "value1", b.Field12["key 1"])
	assert.Equal(t, "value2", b.Field12["key 2"])
	assert.Equal(t, 1, b.Field13["k 1"])
	assert.Equal(t, 2, b.Field13["k 2"])

	_, err = ovs.DeleteKV("includes", "")
	assert.Equal(t, err, nil)

        ovs.Disconnect()
}

func TestSaveField(t *testing.T) {
	fmt.Println("Create Go struct with just one element, save it, modify it, save again and load it back")
	a := A{
		Field1: "value1 changed2",
	}

	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &a)
	assert.Equal(t, err, nil)

	err = ovs.SaveField(&a.Field1)
	assert.Equal(t, err, nil)

	rows, err := ovs.GetKV("==", "/field1")
	assert.Equal(t, err, nil)
	assert.Equal(t, rows[0]["value"], "value1 changed2")

	// change again
	a.Field1 = "value1 changed3"

	err = ovs.SaveField(&a.Field1)
	assert.Equal(t, err, nil)

	rows, err = ovs.GetKV("==", "/field1")
	assert.Equal(t, err, nil)
	assert.Equal(t, rows[0]["value"], "value1 changed3")

	_, err = ovs.DeleteKV("includes", "")
	assert.Equal(t, err, nil)

        ovs.Disconnect()
}

type Info struct {
	Name     string  `ovskv:"name"`
	BirthDay int64   `ovskv:"birthday"`
	Phone    string  `ovskv:"phone"`
	Siblings int     `ovskv:"siblings"`
	Spouse   bool    `ovskv:"spouse"`
//	Money    float64 `ovskv:"money"`
}

type Book struct {
	UUID string `ovskv:"_uuid"`
	Info Info   `ovskv:"info"`
}

type Bench struct {
	Books []Book  `ovskv:"books"`
}

func randString(l int) string {
	buf := make([]byte, l)
	for i := 0; i < (l+1)/2; i++ {
		buf[i] = byte(rand.Intn(256))
	}
	return fmt.Sprintf("%x", buf)[:l]
}

var benchBooksNum int = 1200
func generateBooks() []Book {
	a := make([]Book, 0, benchBooksNum)
	for i := 0; i < benchBooksNum; i++ {
		a = append(a, Book{
			Info: Info{
				Name:     randString(16),
				BirthDay: time.Now().UnixNano(),
				Phone:    randString(10),
				Siblings: rand.Intn(5),
				Spouse:   rand.Intn(2) == 1,
			},
		})
	}
	return a
}

var bench Bench = Bench{}

func BenchmarkStructSave(b *testing.B) {

	bench.Books = generateBooks()

	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &bench)
	assert.Equal(b, err, nil)

	b.ReportAllocs()
	b.ResetTimer()

	if b.N > benchBooksNum { b.N = benchBooksNum }
	for i := 0; i < b.N; i++ {
		err = ovs.SaveField(&bench.Books[i].Info)
		assert.Equal(b, err, nil)
	}

	b.StopTimer()

	_, err = ovs.DeleteKV("includes", "")
	assert.Equal(b, err, nil)

        ovs.Disconnect()
}

func BenchmarkStructLoad(b *testing.B) {

	bench.Books = generateBooks()

	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, &bench)
	assert.Equal(b, err, nil)

	err = ovs.Save()
	assert.Equal(b, err, nil)

	b.ReportAllocs()
	b.ResetTimer()

	if b.N > benchBooksNum { b.N = benchBooksNum }
	for i := 0; i < b.N; i++ {
		err = ovs.LoadField(&bench.Books[i].Info, fmt.Sprintf("/books/%d/info", i))
		assert.Equal(b, err, nil)
	}

	b.StopTimer()

	_, err = ovs.DeleteKV("includes", "")
	assert.Equal(b, err, nil)

        ovs.Disconnect()
}

func BenchmarkKVSet(b *testing.B) {
	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, nil)
	assert.Equal(b, err, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ovs.SetKV(fmt.Sprintf("%d", i), strconv.Itoa(i))
		assert.Equal(b, err, nil)
	}

	b.StopTimer()

	_, err = ovs.DeleteKV("excludes", "")
	assert.Equal(b, err, nil)

        ovs.Disconnect()
}

func BenchmarkKVInsert(b *testing.B) {
	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, nil)
	assert.Equal(b, err, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ovs.InsertKV(fmt.Sprintf("%d", i), strconv.Itoa(i))
		if err != nil && strings.Index(fmt.Sprintf("%v", err), "constraint violation") != -1 {
			continue
		}
		assert.Equal(b, err, nil)
	}

	b.StopTimer()

	_, err = ovs.DeleteKV("excludes", "")
	assert.Equal(b, err, nil)

        ovs.Disconnect()
}

func BenchmarkKVMGet(b *testing.B) {
	ovs, err := ovskv.Init(DB_NAME, DB_CONNECT, DB_NAMESPACE, nil)
	assert.Equal(b, err, nil)

	for i := 0; i < b.N; i++ {
		_, err := ovs.InsertKV(fmt.Sprintf("%d", i), strconv.Itoa(i))
		if err != nil && strings.Index(fmt.Sprintf("%v", err), "constraint violation") != -1 {
			continue
		}
		assert.Equal(b, err, nil)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ovs.GetKVM("==", fmt.Sprintf("%d", i))
		assert.Equal(b, err, nil)
	}

	b.StopTimer()

	_, err = ovs.DeleteKV("excludes", "")
	assert.Equal(b, err, nil)

        ovs.Disconnect()
}
