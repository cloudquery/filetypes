module github.com/cloudquery/filetypes/v4

go 1.19

require (
	github.com/apache/arrow/go/v14 v14.0.0-20230901024001-b6c0ea455d08
	github.com/bradleyjkemp/cupaloy/v2 v2.8.0
	github.com/cloudquery/plugin-sdk/v4 v4.6.3
	github.com/goccy/go-json v0.10.2
	github.com/google/uuid v1.3.1
	github.com/stretchr/testify v1.8.4
)

require google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect

// TODO: remove once all updates are merged
replace github.com/apache/arrow/go/v14 => github.com/cloudquery/arrow/go/v14 v14.0.0-20230904001200-cd3d4114faa0

require (
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/andybalholm/brotli v1.0.5 // indirect
	github.com/apache/thrift v0.18.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/zerolog v1.30.0 // indirect
	github.com/thoas/go-funk v0.9.3 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/net v0.14.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.11.0 // indirect; indirect // indirect
	golang.org/x/text v0.12.0 // indirect
	golang.org/x/tools v0.12.1-0.20230831182430-914b218fc34e // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/grpc v1.57.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
