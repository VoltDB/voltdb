/*
 Generate input test scripts to verify index behaviour.

 Current functionality:
 - random width (1 to maxIdxColumn), random type schemas
 - prng values for all columns
 - "is", "ls", "if" commands on unique indexes

 todo: The abbrev (int, bint, sint...) handling is weird and
 repeated in the voltIdxTypes table as well as the Abbrev()
 functions. Oddly, NValues don't store any value, they only
 exist as types. Probably they should store their string
 representations.
*/
package main

import (
	"fmt"
	"rand"
	"strings"
)

var (
	// Types known to test tool that are valid index column types
	voltIdxTypes = [...]string{
		"int",
		"bint",
		"sint",
		"tint",
		"dec",
		"str4",
		"str128"}

	voltIdxIntTypes = [...]string {
	    "int",
        "bint",
        "sint",
        "tint"}

	// Maximum number of indexed columns
	maxIdxColumns = 50
)


// Interface all values implement
type NValue interface {
	Create() string
	Abbrev() string
}

// INTEGER
type IntValue struct{}

func (nv IntValue) Create() string {
	val := rand.Int() * sign()
	return fmt.Sprint(val)
}

func (nv IntValue) Abbrev() string {
	return "int"
}

// BIGINT
type BintValue struct{}

func (nv BintValue) Create() string {
	val := rand.Int63()
	val = val * int64(sign())
	return fmt.Sprint(val)
}

func (nv BintValue) Abbrev() string {
	return "bint"
}

// SMALLINT
type SintValue struct{}

func (nv SintValue) Create() string {
	val := rand.Intn(0x7FFF)
	signstr := ""
	if val != 0 && rand.Intn(10) < 5 {
		signstr = "-"
	}
	return fmt.Sprintf("%s%d", signstr, val)
}

func (nv SintValue) Abbrev() string {
	return "sint"
}

// TINYINT
type TintValue struct{}

func (nv TintValue) Create() string {
	val := rand.Intn(0x7F)
	signstr := ""
	if val != 0 && rand.Intn(10) < 5 {
		signstr = "-"
	}
	return fmt.Sprintf("%s%d", signstr, val)
}

func (nv TintValue) Abbrev() string {
	return "tint"
}

// DECIMAL
type DecValue struct{}

func (nv DecValue) Create() string {
	vals := make([]string, 3)
	lhs := rand.Int() * sign()
	vals[0] = fmt.Sprint(lhs)
	vals[1] = "."
	vals[2] = fmt.Sprint(rand.Intn(99999999))
	return strings.Join(vals, "")
}

func (nv DecValue) Abbrev() string {
	return "dec"
}

// VARCHAR(4|128)
type StrValue struct {
	size int
}

func (nv StrValue) Create() string {
	if nv.size == 128 {
		substrs := [...]string{"ning", "izzy", "ariel", "nick",
			"mazur", "ryan", "hugg", "yankeesfan", "volt", "runs",
			"with", "scissors", "blue", "awesome", "weak", "sauce",
			"chicken", "strength", "vikram", "bobbi", "jarr", "bungee",
			"banjo", "arrow", "trinity", "coffee", "pvc"}
		cnt := (rand.Int() % 10) + 1
		vals := make([]string, cnt)

		for i := 0; i < cnt; i++ {
			vals[i] = substrs[rand.Int()%len(substrs)]
		}
		return strings.Join(vals, "")
	} else if nv.size == 4 {
		substrs := [...]string{"a", "b", "c", "d"}
		cnt := 4
		vals := make([]string, cnt)

		for i := 0; i < cnt; i++ {
			vals[i] = substrs[rand.Int()%len(substrs)]
		}
		return strings.Join(vals, "")
	}
	panic("Invalid string size.")
}

func (nv StrValue) Abbrev() string {
	return fmt.Sprintf("%s%d", "str", nv.size)
}

// Utility Functions

func nvalueFactory(abbrev string) NValue {
	switch abbrev {
	case "int":
		return &IntValue{}
	case "bint":
		return &BintValue{}
	case "sint":
		return &SintValue{}
	case "tint":
		return &TintValue{}
	case "dec":
		return &DecValue{}
	case "str4":
		return &StrValue{4}
	case "str128":
		return &StrValue{128}
	}
	panic(abbrev)
}

func printSliceAsList(slice []string) {
	for i := 0; i < len(slice); i++ {
		fmt.Printf(slice[i])
		if i == len(slice)-1 {
			fmt.Printf("\n")
		} else {
			fmt.Printf(",")
		}
	}
}

func createSchema() ([]NValue, []string) {
	schema := make([]NValue, (rand.Int()%maxIdxColumns)+1)
	abbrev := make([]string, len(schema))
	for ii := 0; ii < len(schema); ii++ {
		schemaType := voltIdxTypes[rand.Int()%len(voltIdxTypes)]
		schema[ii] = nvalueFactory(schemaType)
		abbrev[ii] = schemaType
	}
	return schema, abbrev
}

func createIntsOnlySchema() ([]NValue, []string) {
	schema := make([]NValue, (rand.Int() % 4) + 1)
	abbrev := make([]string, len(schema))
	for ii := 0; ii < len(schema); ii++ {
		schemaType := voltIdxIntTypes[rand.Int() % len(voltIdxIntTypes)]
		schema[ii] = nvalueFactory(schemaType)
		abbrev[ii] = schemaType
	}
	return schema, abbrev
}

func createTuple(schema []NValue) []string {
	tuple := make([]string, len(schema))
	for ii := 0; ii < len(schema); ii++ {
		tuple[ii] = schema[ii].Create()
	}
	return tuple
}

// Create a reasonable serialization given a schema a tuple
func tupleKey(schema []NValue, tuple []string) string {
	parts := make([]string, len(schema)*2)
	pi := 0
	for si := 0; si < len(schema); si++ {
		parts[pi], parts[pi+1] = schema[si].Abbrev(), tuple[si]
		pi = pi + 2
	}
	return strings.Join(parts, ":")
}

func sign() int {
	sign := rand.Int() % 2
	if sign == 0 {
		return 1
	}
	return -1
}

/*
 * Commands known to the test harness:
 *
 * is : insert success
 * if : insert failure
 * ls : lookup success
 * lf : lookup failure
 * us : update success
 * uf : update failure
 */


func generateUniqueGenericTree(testrun int) {
	// map where the keys are strings and the values are slices of strings
	tuples := make(map[string][]string)
	schema, abbrev := createSchema()

	// print the test introduction
	fmt.Printf("begin TestUniqueGenericTree_%d UniqueGenericTree ", testrun)
	printSliceAsList(abbrev)

	simpleUniqueGenerator(tuples, schema)
}

func generateUniqueIntsHash(testrun int) {
	 tuples := make(map[string][]string)
	 schema, abbrev := createIntsOnlySchema()

	 fmt.Printf("begin TestUniqueIntsHash_%d UniqueIntsHash ", testrun)
	 printSliceAsList(abbrev)

	 simpleUniqueGenerator(tuples, schema)
}

func simpleUniqueGenerator(tuples map[string][]string, schema []NValue) {
	// create tuples. push them into a map to uniqify them.
	for cmd := 0; cmd < 10; cmd++ {
		tuple := createTuple(schema)
		tuplekey := tupleKey(schema, tuple)
		tuples[tuplekey] = tuple
	}

	// is commands
	for _, v := range tuples {
		fmt.Printf("is ")
		printSliceAsList(v)
	}

	// ls commands
	for _, v := range tuples {
		fmt.Printf("ls ")
		printSliceAsList(v)
	}

	// if commands (reinserting existing keys should fail)
	for _, v := range tuples {
		fmt.Printf("if ")
		printSliceAsList(v)
	}

	// ds commands (delete success)
	for _, v := range tuples {
		fmt.Printf("ds ")
		printSliceAsList(v)
	}
	for _, v := range tuples {
		fmt.Printf("df ")
		printSliceAsList(v)
	}
	for _, v := range tuples {
		fmt.Printf("lf ")
		printSliceAsList(v)
	}

	// print the test conclusion
	fmt.Println("exec")
}

func main() {
	fmt.Printf("# File generated by index_script_gen.go\n")
	var i int = 0
	for {
	    // generateUniqueGenericTree(i)
		 generateUniqueIntsHash(i)
		i++
	}
	fmt.Println("done")
}
