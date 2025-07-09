package cbioportal_databricks_gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"

	dbsql "github.com/databricks/databricks-sql-go"
)

type DatabricksService struct {
	tokenComment string
	db           *sql.DB
	schema       string
	requestTable string
	sampleTable  string
}

func NewDatabricksService(token, tokenComment, hostname, httpPath, schema, requestTable, sampleTable, slackURL string, port int) (*DatabricksService, func(), error) {
	db, err := openDatabase(token, hostname, httpPath, port)
	if err != nil {
		return nil, nil, err
	}
	closeFunc := func() {
		db.Close()
	}
	return &DatabricksService{tokenComment: tokenComment, db: db, schema: schema, requestTable: requestTable, sampleTable: sampleTable}, closeFunc, nil
}

func (d *DatabricksService) GetRequest(igoRequestID string) (SmileRequest, error) {
	var toReturn SmileRequest
	if err := d.db.Ping(); err != nil {
		errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	query := fmt.Sprintf("select REQUEST_JSON from %s.%s where IGO_REQUEST_ID = '%s'", d.schema, d.requestTable, igoRequestID)
	var rJSON sql.NullString
	err := d.db.QueryRow(query).Scan(&rJSON)
	if err != nil {
		errReturn := fmt.Errorf("Failed to get request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	if rJSON.Valid {
		err = json.Unmarshal([]byte(rJSON.String), &toReturn)
		if err != nil {
			errReturn := fmt.Errorf("Failed to Unmarshal request: '%s': %q", igoRequestID, err)
			return toReturn, errReturn
		}
	}
	return toReturn, nil
}

func (d *DatabricksService) GetSample(igoRequestID, sampleName string) (SmileSample, error) {
	var toReturn SmileSample
	if err := d.db.Ping(); err != nil {
		errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	query := fmt.Sprintf("select SAMPLE_JSON from %s.%s where IGO_REQUEST_ID = '%s' and IGO_SAMPLE_NAME = '%s'", d.schema, d.sampleTable, igoRequestID, sampleName)
	var sJSON sql.NullString
	err := d.db.QueryRow(query).Scan(&sJSON)
	if err != nil {
		errReturn := fmt.Errorf("Failed to get sample: '%s', request '%s': %q", sampleName, igoRequestID, err)
		return toReturn, errReturn
	}
	if sJSON.Valid {
		err = json.Unmarshal([]byte(sJSON.String), &toReturn)
		if err != nil {
			errReturn := fmt.Errorf("Failed to unmarshal sample: '%s', request '%s': %q", sampleName, igoRequestID, err)
			return toReturn, errReturn
		}
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
