package main

import (
	_ "embed"
	"io/ioutil"
	"log"

	"github.com/DivPro/topology/internal/app"
	"gopkg.in/yaml.v2"
)

var (
	//go:embed assets/dot.gohtml
	tpl string
)

func main() {
	yamlFile, err := ioutil.ReadFile("cfg/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	var config app.Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}
	app.Run(tpl, config)
}
