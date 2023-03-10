package cqarrow

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-json"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/google/uuid"
)

type UUIDBuilder struct {
	*array.ExtensionBuilder
	dtype *UUIDType
}

func NewUUIDBuilder(mem memory.Allocator, dtype arrow.ExtensionType) *UUIDBuilder {
	b := &UUIDBuilder{
		ExtensionBuilder: array.NewExtensionBuilder(mem, dtype),
		dtype:            dtype.(*UUIDType),
	}
	return b
}

func (b *UUIDBuilder) Append(v uuid.UUID) {
	b.ExtensionBuilder.Builder.(*array.FixedSizeBinaryBuilder).Append(v[:])
}

func (b *UUIDBuilder) UnsafeAppend(v uuid.UUID) {
	b.ExtensionBuilder.Builder.(*array.FixedSizeBinaryBuilder).UnsafeAppend(v[:])
}

func (b *UUIDBuilder) AppendValues(v []uuid.UUID, valid []bool) {
	data := make([][]byte, len(v))
	for i := range v {
		data[i] = v[i][:]
	}
	b.ExtensionBuilder.Builder.(*array.FixedSizeBinaryBuilder).AppendValues(data, valid)
}

func (b *UUIDBuilder) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	var val uuid.UUID
	switch v := t.(type) {
	case string:
		data, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		val = data
	case []byte:
		data, err := uuid.ParseBytes(v)
		if err != nil {
			return err
		}
		val = data
	case nil:
		b.AppendNull()
		return nil
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
			Struct: fmt.Sprintf("FixedSizeBinary[%d]", 16),
		}
	}

	if len(val) != 16 {
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(val),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
			Struct: fmt.Sprintf("FixedSizeBinary[%d]", 16),
		}
	}
	b.Append(val)
	return nil
}

func (b *UUIDBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *UUIDBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("uuid builder must unpack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

// UUIDArray is a simple array which is a FixedSizeBinary(16)
type UUIDArray struct {
	array.ExtensionArrayBase
}

func (a UUIDArray) String() string {
	arr := a.Storage().(*array.FixedSizeBinary)
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
			uuidStr, err := uuid.FromBytes(arr.Value(i))
			if err != nil {
				panic(fmt.Errorf("invalid uuid: %w", err))
			}
			fmt.Fprintf(o, "%q", uuidStr)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *UUIDArray) MarshalJSON() ([]byte, error) {
	arr := a.Storage().(*array.FixedSizeBinary)
	vals := make([]any, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			uuidStr, err := uuid.FromBytes(arr.Value(i))
			if err != nil {
				panic(fmt.Errorf("invalid uuid: %w", err))
			}
			vals[i] = uuidStr
		} else {
			vals[i] = nil
		}
	}
	return json.Marshal(vals)
}

func (a *UUIDArray) GetOneForMarshal(i int) any {
	arr := a.Storage().(*array.FixedSizeBinary)
	if a.IsValid(i) {
		uuidObj, err := uuid.FromBytes(arr.Value(i))
		if err != nil {
			panic(fmt.Errorf("invalid uuid: %w", err))
		}
		return uuidObj
	}
	return nil
}

// UUIDType is a simple extension type that represents a FixedSizeBinary(16)
// to be used for representing UUIDs
type UUIDType struct {
	arrow.ExtensionBase
}

// NewUUIDType is a convenience function to create an instance of UuidType
// with the correct storage type
func NewUUIDType() *UUIDType {
	return &UUIDType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: &arrow.FixedSizeBinaryType{ByteWidth: 16}}}
}

// ArrayType returns TypeOf(UuidArray) for constructing uuid arrays
func (UUIDType) ArrayType() reflect.Type {
	return reflect.TypeOf(UUIDArray{})
}

func (UUIDType) ExtensionName() string {
	return "uuid"
}

func (e UUIDType) String() string {
	return fmt.Sprintf("extension_type<storage=%s>", e.Storage)
}

func (e UUIDType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}

// Serialize returns "uuid-serialized" for testing proper metadata passing
func (UUIDType) Serialize() string {
	return "uuid-serialized"
}

// Deserialize expects storageType to be FixedSizeBinaryType{ByteWidth: 16} and the data to be
// "uuid-serialized" in order to correctly create a UuidType for testing deserialize.
func (UUIDType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if data != "uuid-serialized" {
		return nil, fmt.Errorf("type identifier did not match: '%s'", data)
	}
	if !arrow.TypeEqual(storageType, &arrow.FixedSizeBinaryType{ByteWidth: 16}) {
		return nil, fmt.Errorf("invalid storage type for UuidType: %s", storageType.Name())
	}
	return NewUUIDType(), nil
}

// UuidTypes are equal if both are named "uuid"
func (e UUIDType) ExtensionEquals(other arrow.ExtensionType) bool {
	return e.ExtensionName() == other.ExtensionName()
}

func (UUIDType) NewBuilder(mem memory.Allocator, dt arrow.ExtensionType) any {
	return NewUUIDBuilder(mem, dt)
}
