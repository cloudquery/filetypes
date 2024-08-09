package parquet

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/invopop/jsonschema"
)

var allowedVersions = []string{"v1.0", "v2.4", "v2.6", "v2Latest"}
var allowedRootRepetitions = []string{"undefined", "required", "optional", "repeated"}

type ParquetSpec struct {
	Version        string `json:"version,omitempty"`
	RootRepetition string `json:"root_repetition,omitempty"`
}

func (s *ParquetSpec) GetVersion() parquet.Version {
	switch s.Version {
	case "v1.0":
		return parquet.V1_0
	case "v2.4":
		return parquet.V2_4
	case "v2.6":
		return parquet.V2_6
	case "v2Latest":
		return parquet.V2_LATEST
	}
	return parquet.V2_LATEST
}

func (s *ParquetSpec) GetRootRepetition() parquet.Repetition {
	switch s.RootRepetition {
	case "undefined":
		return parquet.Repetitions.Undefined
	case "required":
		return parquet.Repetitions.Required
	case "optional":
		return parquet.Repetitions.Optional
	case "repeated":
		return parquet.Repetitions.Repeated
	}
	return parquet.Repetitions.Repeated
}

func (ParquetSpec) JSONSchema() *jsonschema.Schema {
	properties := jsonschema.NewProperties()
	allowedVersionsAsAny := make([]interface{}, len(allowedVersions))
	for i, v := range allowedVersions {
		allowedVersionsAsAny[i] = v
	}
	properties.Set("version", &jsonschema.Schema{
		Type:        "string",
		Description: "Parquet format version",
		Enum:        allowedVersionsAsAny,
		Default:     "v2Latest",
	})
	return &jsonschema.Schema{
		Description:          "CloudQuery Parquet file output spec.",
		Properties:           properties,
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (s *ParquetSpec) SetDefaults() {
	if s.Version == "" {
		s.Version = "v2Latest"
	}
	if s.RootRepetition == "" {
		s.RootRepetition = "repeated"
	}
}

func (s *ParquetSpec) Validate() error {
	if !slices.Contains(allowedVersions, s.Version) {
		return fmt.Errorf("invalid version: %s. Allowed values are %s", s.Version, strings.Join(allowedVersions, ", "))
	}
	if !slices.Contains(allowedRootRepetitions, s.RootRepetition) {
		return fmt.Errorf("invalid rootRepetition: %s. Allowed values are %s", s.RootRepetition, strings.Join(allowedRootRepetitions, ", "))
	}
	return nil
}
