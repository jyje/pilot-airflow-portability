package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
)

func appendGo(_ airflow.Context, value string) (string, error) {
	return value + "-go", nil
}

func main() {
	bundle := airflow.Bundle()
	bundle.Register(airflow.TaskHandler("pilot_mixed_language_chain", "go", appendGo))
	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
