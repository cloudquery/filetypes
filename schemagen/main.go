package main

import (
	"fmt"
	"log"
	"path"
	"runtime"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/cloudquery/filetypes/v4"
)

func main() {
	fmt.Println("Generating JSON schema for plugin spec")
	jsonschema.GenerateIntoFile(new(filetypes.FileSpec), path.Join(currDir(), "..", "schema.json"),
		jsonschema.WithAddGoComments("github.com/cloudquery/filetypes/v4", path.Join(currDir(), "..")),
	)
}

func currDir() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Failed to get caller information")
	}
	return path.Dir(filename)
}