package parquet

import (
	"github.com/apache/arrow/go/v13/arrow"
)

type listLikeType interface {
	arrow.DataType
	Elem() arrow.DataType
}
