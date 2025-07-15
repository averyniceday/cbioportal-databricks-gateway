package cbioportal_databricks_gateway

import (
    "fmt"
)

// supported cBioPortal filetypes
// maps expected tables inside databricks to corresponding files

func (d *DatabricksService) WritecBioStudyFiles(directory string) error {
	fmt.Println(directory)
	for k,_ := range supportedFiletypes {
		fmt.Println(k)
	}
	return nil
}
