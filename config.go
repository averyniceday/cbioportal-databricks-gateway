package cbioportal_databricks_gateway

type Config struct {
	Token              string  `docopt:"--token"`
	Host               string  `docopt:"--host"`
	Port               int     `docopt:"--port"`
	Path               string  `docopt:"--path"`
	Catalog            string  `docopt:"--catalog"`
	Schema             string  `docopt:"--schema"`
	Directory          string  `docopt:"--directory"`
}

