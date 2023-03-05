package cqarrow

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-json"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type JSONBuilder struct {
	*array.ExtensionBuilder
	dtype *JSONType
}

func NewJSONBuilder(mem memory.Allocator, dtype arrow.ExtensionType) *JSONBuilder {
	b := &JSONBuilder{
		ExtensionBuilder: array.NewExtensionBuilder(mem, dtype),
		dtype:            dtype.(*JSONType),
	}
	return b
}

func (b *JSONBuilder) Append(v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).Append(bytes)
}

func (b *JSONBuilder) UnsafeAppend(v any) {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).UnsafeAppend(bytes)
}

func (b *JSONBuilder) AppendValues(v []any, valid []bool) {
	data := make([][]byte, len(v))
	for i := range v {
		bytes, err := json.Marshal(v[i])
		if err != nil {
			panic(err)
		}
		data[i] = bytes
	}
	b.ExtensionBuilder.Builder.(*array.BinaryBuilder).AppendValues(data, valid)
}

// func (b *JSONBuilder) UnmarshalOne(dec *json.Decoder) error {
// 	t, err := dec.Token()
// 	if err != nil {
// 		return err
// 	}

// 	var data any
// 	switch v := t.(type) {
// 	case string:
// 		err := json.Unmarshal([]byte(v), &data)
// 		if err != nil {
// 			return err
// 		}
// 	case []byte:
// 		err := json.Unmarshal(v, &data)
// 		if err != nil {
// 			return err
// 		}
// 	case nil:
// 		b.AppendNull()
// 		return nil
// 	default:
// 		return &json.UnmarshalTypeError{
// 			Value:  fmt.Sprint(t),
// 			Type:   reflect.TypeOf([]byte{}),
// 			Offset: dec.InputOffset(),
// 			Struct: "JSONBuilder",
// 		}
// 	}

// 	b.Append(data)
// 	return nil
// }

// func (b *JSONBuilder) Unmarshal(dec *json.Decoder) error {
// 	for dec.More() {
// 		if err := b.UnmarshalOne(dec); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func (b *JSONBuilder) UnmarshalJSON(data []byte) error {
	var a []any
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	valid := make([]bool, len(a))
	for i := range a {
		valid[i] = a[i] != nil
	}
	b.AppendValues(a, valid)
	// dec := json.NewDecoder(bytes.NewReader(data))
	// t, err := dec.Token()
	// if err != nil {
	// 	return err
	// }

	// if delim, ok := t.(json.Delim); !ok || delim != '[' {
	// 	return fmt.Errorf("json builder must unpack from json array, found %s", delim)
	// }

	// return b.Unmarshal(dec)
	return nil
}

// JSONArray is a simple array which is a Binary
type JSONArray struct {
	array.ExtensionArrayBase
}

func (a JSONArray) String() string {
	arr := a.Storage().(*array.Binary)
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString("(null)")
		default:
			fmt.Fprintf(o, "\"%s\"", arr.Value(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *JSONArray) MarshalJSON() ([]byte, error) {
	arr := a.Storage().(*array.Binary)
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			err := json.Unmarshal(arr.Value(i), &vals[i])
			if err != nil {
				panic(fmt.Errorf("invalid json: %w", err))
			}
		} else {
			vals[i] = nil
		}
	}
	return json.Marshal(vals)
}

func (a *JSONArray) GetOneForMarshal(i int) interface{} {
	arr := a.Storage().(*array.Binary)
	if a.IsValid(i) {
		var data any
		err := json.Unmarshal(arr.Value(i), &data)
		if err != nil {
			panic(fmt.Errorf("invalid json: %w", err))
		}
		return data
	}
	return nil
}

// JSONType is a simple extension type that represents a FixedSizeBinary(16)
// to be used for representing JSONs
type JSONType struct {
	arrow.ExtensionBase
}

// NewJSONType is a convenience function to create an instance of JSONType
// with the correct storage type
func NewJSONType() *JSONType {
	return &JSONType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: &arrow.BinaryType{}}}
}

// ArrayType returns TypeOf(JSONType) for constructing JSON arrays
func (JSONType) ArrayType() reflect.Type { return reflect.TypeOf(JSONArray{}) }

func (JSONType) ExtensionName() string { return "json" }

func (e JSONType) String() string { return fmt.Sprintf("extension_type<storage=%s>", e.Storage) }

func (e JSONType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}
// Serialize returns "json-serialized" for testing proper metadata passing
func (JSONType) Serialize() string { return "json-serialized" }

// Deserialize expects storageType to be FixedSizeBinaryType{ByteWidth: 16} and the data to be
// "json-serialized" in order to correctly create a UuidType for testing deserialize.
func (JSONType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if string(data) != "json-serialized" {
		return nil, fmt.Errorf("type identifier did not match: '%s'", string(data))
	}
	if !arrow.TypeEqual(storageType, &arrow.BinaryType{}) {
		return nil, fmt.Errorf("invalid storage type for JSONType: %s", storageType.Name())
	}
	return NewJSONType(), nil
}

// UuidTypes are equal if both are named "uuid"
func (u JSONType) ExtensionEquals(other arrow.ExtensionType) bool {
	return u.ExtensionName() == other.ExtensionName()
}

func (u JSONType) NewBuilder(mem memory.Allocator, dt arrow.ExtensionType) interface{} {
	return NewJSONBuilder(mem, dt)
}