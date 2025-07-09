package cbioportal_databricks_gateway

import (
	"database/sql"
	"fmt"

	dbsql "github.com/databricks/databricks-sql-go"
)

type DatabricksService struct {
	db           *sql.DB
	schema       string
}

func GetString() {
	fmt.Println("HERE I AM")
}

func NewDatabricksService(token, hostname, httpPath, schema string, port int) (*DatabricksService, func(), error) {
	db, err := openDatabase(token, hostname, httpPath, port)
	if err != nil {
		return nil, nil, err
	}
	closeFunc := func() {
		db.Close()
	}
	return &DatabricksService{db: db, schema: schema}, closeFunc, nil
}

type ToReturnStruct struct {
	patientId    string
	cancerType   string
	sampleId     string
}

func (d *DatabricksService) GetSample(sampleName string) (ToReturnStruct, error) {
	var toReturn ToReturnStruct
	if err := d.db.Ping(); err != nil {
		errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", sampleName, err)
		return toReturn, errReturn
	}
	query := fmt.Sprintf("select PATIENT_ID, CANCER_TYPE, SAMPLE_ID from %s.%s where SAMPLE_ID = '%s'", d.schema, "data_clinical_sample", sampleName)
	fmt.Println(query)
	err := d.db.QueryRow(query).Scan(&toReturn.patientId, &toReturn.cancerType, &toReturn.sampleId)
	if err != nil {
		errReturn := fmt.Errorf("Failed to get sample: '%s', %q", sampleName, err)
		return toReturn, errReturn
	}
	return toReturn, nil
}

func openDatabase(accessToken, hostname, httpPath string, port int) (*sql.DB, error) {
	connector, err := dbsql.NewConnector(
		dbsql.WithAccessToken(accessToken),
		dbsql.WithServerHostname(hostname),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
	)

	if err != nil {
		return nil, err
	}

	return sql.OpenDB(connector), nil
}
