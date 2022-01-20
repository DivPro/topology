package app

import (
	"context"
	"log"
	"net/http"
	"text/template"

	"github.com/DivPro/topology/internal/connect"

	"github.com/DivPro/topology/internal/ksql"
	"github.com/DivPro/topology/internal/topology"
	"github.com/goccy/go-graphviz"
)

func Run(tplRaw string, conf Config) {
	t := topology.New(
		ksql.New(http.DefaultClient, ksql.Config{
			URL:      conf.KSQL.URL,
			User:     conf.KSQL.User,
			Password: conf.KSQL.Password,
		}),
		connect.New(http.DefaultClient, connect.Config{
			URL:      conf.Connect.URL,
			User:     conf.Connect.User,
			Password: conf.Connect.Password,
		}),
		template.Must(template.New("tpl").Parse(tplRaw)),
	)
	if err := t.Fetch(context.TODO()); err != nil {
		log.Fatal(err)
	}

	g := graphviz.New()
	dot := t.OutputText()
	log.Println(dot)
	graph, err := graphviz.ParseBytes([]byte(dot))
	if err != nil {
		log.Fatal(err)
	}

	if err := g.RenderFilename(graph, graphviz.PNG, "graph.png"); err != nil {
		log.Fatal(err)
	}
	if err := g.RenderFilename(graph, graphviz.SVG, "graph.svg"); err != nil {
		log.Fatal(err)
	}
}
