package cbioportal_databricks_gateway

import (
    "database/sql"
    "encoding/csv"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    dbsql "github.com/databricks/databricks-sql-go"
)

type DatabricksService struct {
    db           *sql.DB
    catalog      string
    schema       string
}

var supportedFiletypes = map[string]string {
    "data_mutations_extended": "data_mutations_extended.txt",
    "data_sv": "data_sv.txt",
    "data_clinical_patient": "data_clinical_patient.txt",
    "data_clinical_sample": "data_clinical_sample.txt",
    "data_gene_matrix": "data_gene_matrix.txt",
    "data_timeline": "data_timeline.txt",
}

func NewDatabricksService(token, hostname, httpPath, catalog, schema string, port int) (*DatabricksService, func(), error) {
    db, err := openDatabase(token, hostname, httpPath, port)
    if err != nil {
        return nil, nil, err
    }
    closeFunc := func() {
        db.Close()
    }
    return &DatabricksService{db: db, catalog: catalog, schema: schema}, closeFunc, nil
}

type ToReturnStruct struct {
    patientId    string
    cancerType   string
    sampleId     string
}

func (d *DatabricksService) GetTableColumns(tableName string) ([]string, error) {
    var columnNames []string
    if err := d.db.Ping(); err != nil {
        errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", tableName, err)
        return columnNames, errReturn
    }
    query := fmt.Sprintf("SHOW COLUMNS IN %s", tableName)
    rows, err := d.db.Query(query)
    if err != nil {
        errReturn := fmt.Errorf("Failed to get column names: '%s', %q", tableName, err)
        return columnNames, errReturn
    }
    defer rows.Close()
    for rows.Next() {
        var colName string
        if err := rows.Scan(&colName); err != nil {
            errReturn := fmt.Errorf("Failed to get column names: '%s', %q", tableName, err)
            return columnNames, errReturn
        }
        columnNames = append(columnNames, colName)
    }
    return columnNames, nil
}

func (d *DatabricksService) WriteMetaData(tableName string, outDir string) error {
    // Construct query with % in LIKE
    query := fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE data_filename LIKE '%s%%'", d.catalog, d.schema, "metadata", tableName)
    rows, err := d.db.Query(query)
    if err != nil {
        return fmt.Errorf("Failed to get data: '%s', %q", tableName, err)
    }
    defer rows.Close()

    var (
        studyId string
        dataFile string
        key string
        value string
        rowCount int
        lines []string
    )

    // Process rows, but don’t write yet
    for rows.Next() {
        err := rows.Scan(&studyId, &dataFile, &key, &value)
        if err != nil {
            return fmt.Errorf("Failed to scan row: %q", err)
        }

        if rowCount == 0 {
            lines = append(lines, fmt.Sprintf("cancer_study_identifier: %s", studyId))
            lines = append(lines, fmt.Sprintf("data_filename: %s", dataFile))
        }

        lines = append(lines, fmt.Sprintf("%s: %s", key, value))
        rowCount++
    }

    // If no rows, skip writing
    if rowCount == 0 {
        return nil
    }

    // Create the directory
    err = os.MkdirAll(outDir, 0755)
    if err != nil {
        return fmt.Errorf("Error creating directory: %v", err)
    }

    // Get output path
    metaFilename, ok := supportedFiletypes[tableName]
    if !ok {
        return fmt.Errorf("Unsupported table name: %s", tableName)
    }
    outFilePath := filepath.Join(outDir, strings.Replace(metaFilename, "data", "meta", 1))

    // Write to file
    file, err := os.Create(outFilePath)
    if err != nil {
        return fmt.Errorf("Failed to create file: '%s', %q", outFilePath, err)
    }
    defer file.Close()

    for _, line := range lines {
        _, err := fmt.Fprintln(file, line)
        if err != nil {
            return fmt.Errorf("Failed to write line to file: '%s', %q", outFilePath, err)
        }
    }

    return nil
}

// reading and writing at same time will save on memory and be faster
func (d *DatabricksService) WriteAllTableData(tableName string, outDir string) (error) {
    query := fmt.Sprintf("SELECT * FROM %s.%s.%s", d.catalog, d.schema, tableName)
    rows, err := d.db.Query(query)
    if err != nil {
        errReturn := fmt.Errorf("Failed to get data: '%s', %q", tableName, err)
        return errReturn
    }
    defer rows.Close()

	// Create the directory and any necessary parent directories
	err = os.MkdirAll(outDir, 0755)
	if err != nil {
		errReturn := fmt.Errorf("Error creating directory: %v\n", err)
		return errReturn
	}
    // TODO what if supportedFiletypes[tableName] is not found
    var outFilePath = filepath.Join(outDir, supportedFiletypes[tableName])
    file, err := os.Create(outFilePath)
    if err != nil {
        errReturn := fmt.Errorf("Failed create file: '%s', %q", outFilePath, err)
        return errReturn
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    writer.Comma = '\t'
    defer writer.Flush()

    columns, _ := rows.Columns()
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))

    // Write headers
    if err := writer.Write(columns); err != nil {
        errReturn := fmt.Errorf("Failed to get column data: %q", err)
        return errReturn
    }

    for i := range values {
        valuePtrs[i] = &values[i]
    }

    for rows.Next() {
        err := rows.Scan(valuePtrs...)
        if err != nil {
            errReturn := fmt.Errorf("Failed to get data: %q", err)
            return errReturn
        }

        // Convert []interface{} to []string for csv.Writer
        record := make([]string, len(values))
        for i, val := range values {
            if val != nil {
                record[i] = fmt.Sprintf("%v", val)
            } else {
                record[i] = ""
            }
        }
        writer.Write(record)
    }
    return nil
}

func (d *DatabricksService) GetSample(sampleName string) (ToReturnStruct, error) {
    var toReturn ToReturnStruct
    if err := d.db.Ping(); err != nil {
        errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", sampleName, err)
        return toReturn, errReturn
    }
    query := fmt.Sprintf("select PATIENT_ID, CANCER_TYPE, SAMPLE_ID from %s.%s.%s where SAMPLE_ID = '%s'", d.catalog, d.schema, "data_clinical_sample", sampleName)
    fmt.Println(query)
    err := d.db.QueryRow(query).Scan(&toReturn.patientId, &toReturn.cancerType, &toReturn.sampleId)
    if err != nil {
        errReturn := fmt.Errorf("Failed to get sample: '%s', %q", sampleName, err)
        return toReturn, errReturn
    }
    return toReturn, nil
}

func (d *DatabricksService) GetValidTablesInSchema() ([]string, error) {
    var toReturn []string
    allTables, err := d.GetTablesInSchema()
    if err != nil {
        return nil, err
    }
    for _, table := range allTables {
        _, ok := supportedFiletypes[table]
        if ok {
            toReturn = append(toReturn, table)
        }
    }
    return toReturn, nil
}

func (d *DatabricksService) GetTablesInSchema() ([]string, error) {
    var toReturn []string
    query := fmt.Sprintf("SHOW TABLES IN %s.%s", d.catalog, d.schema)
    rows, err := d.db.Query(query)
    if err != nil {
        fmt.Println(err)
        return nil, err
    }
    var tableName string
    var x interface{}
    var y interface{}
    for rows.Next() {
        err := rows.Scan(&x, &tableName, &y)
        if err != nil {
            errReturn := fmt.Errorf("Failed to get data: %q", err)
            return nil, errReturn
        }
        toReturn = append(toReturn, tableName)
    }
    return toReturn, nil
}

// this version if cdsi_public catalog is messy maybe?
func (d *DatabricksService) CheckIfTableExists(table string) (bool, error) {
    var exists int
    // query returns `database`, `tableName`, `isTemporary`
    query := fmt.Sprintf("select 1 from %s.information_schema.tables where table_schema = '%s' and table_name = '%s'", d.catalog, d.schema, table)
    err := d.db.QueryRow(query).Scan(&exists)
    if err != nil {
        return false, fmt.Errorf("Unable to find table: %s", table)
    }
    return true, nil
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
