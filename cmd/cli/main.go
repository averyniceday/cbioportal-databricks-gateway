package main

import (
	"log"
	"fmt"
	cdg "github.com/averyniceday/cbioportal-databricks-gateway"
)

func main() {
	cdg.GetString()
	ds, cf, err := cdg.NewDatabricksService()
	if err != nil {
		fmt.Println(err)
	}
	id, err := ds.GetSample("P-0000004-T01-IM3")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("This is our result:")
	fmt.Println(id)
	cf()
	log.Println("here I am")

}
