package main

import (
    "log"
    "fmt"
    cdg "github.com/averyniceday/cbioportal-databricks-gateway"
    "github.com/docopt/docopt-go"
)


const usage = `cbioportal-databricks-gateway.

Usage:
  cbioportal-databricks-gateway -h | --help
  cbioportal-databricks-gateway --token=<token>
                --host=<host>
                --catalog=<catalog>
                --schema=<schema>
                --path=<path>
                --port=<port>
                --directory=<directory>

Options:
  -h --help                           Show this screen.
  --token=<token>                     The access token required to interact with Datbricks.
  --host=<host>                       The Databricks hostname
  --port=<port>                       The Databricks port
  --path=<path>                       The HTTP path provided in Databricks warehouse server connection details
  --catalog=<catalog>                 The catalog to query for a given schema
  --schema=<schema>                   The schema to query (represents a cBioPortal study)
  --directory=<directory>             The directory to write generated cBioPortal files out to
`

func handleIfError(err error, message string) {
    if err != nil {
        log.Fatalf("%s: %v", message, err)
    }
}

func main() {
    args, err := docopt.ParseDoc(usage)
    handleIfError(err, "Arguments cannot be parsed")

    var config cdg.Config
    err = args.Bind(&config)
    handleIfError(err, "Error binding arguments")

    ds, closeDB, err := cdg.NewDatabricksService(config.Token, config.Host, config.Path, config.Catalog, config.Schema, config.Port)
    handleIfError(err, "Error connecting to the database")

    tables, err := ds.GetValidTablesInSchema()
    handleIfError(err, "Error getting list of valid tables in schema")
    for _, table := range tables {
        err = ds.WriteAllTableData(table, config.Directory)
        handleIfError(err, fmt.Sprintf("Error writing table data for '%s'", table))
        err = ds.WriteMetaData(table, config.Directory)
        handleIfError(err, fmt.Sprintf("Error writing metadata data for '%s'", table))
    }
    closeDB()
}
