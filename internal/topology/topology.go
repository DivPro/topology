package topology

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"text/template"

	"github.com/DivPro/topology/internal/ksql"
	"github.com/DivPro/topology/internal/models"
)

type Topology struct {
	api    *ksql.KSQL
	tpl    *template.Template
	topics map[string]*models.Topic
	// name -> id
	topicNames map[string]string
	streams    map[string]*models.Stream
	// name -> id
	streamNames map[string]string
	connectors  map[string]*models.Connector
	// name -> id
	connectorNames map[string]string
}

func New(
	api *ksql.KSQL,
	tpl *template.Template,
) *Topology {
	return &Topology{
		api: api,
		tpl: tpl,
	}
}

func (t *Topology) Fetch(ctx context.Context) error {
	topics, err := t.api.FetchTopics(ctx)
	if err != nil {
		return fmt.Errorf("topology.fetch topics: %w", err)
	}
	t.topics = make(map[string]*models.Topic, len(topics))
	t.topicNames = make(map[string]string, len(topics))
	for _, item := range topics {
		t.topics[item.ID] = item
		t.topicNames[item.Name] = item.ID
	}

	streams, err := t.api.FetchStreamsAndTables(ctx)
	if err != nil {
		return fmt.Errorf("topology.fetch topics: %w", err)
	}
	t.streams = make(map[string]*models.Stream, len(streams))
	t.streamNames = make(map[string]string, len(streams))
	for _, item := range streams {
		t.streams[item.ID] = item
		t.streamNames[item.Name] = item.ID
	}

	connectors, err := t.api.FetchConnectors(ctx)
	if err != nil {
		return fmt.Errorf("topology.fetch connectors: %w", err)
	}
	t.connectors = make(map[string]*models.Connector, len(connectors))
	t.connectorNames = make(map[string]string, len(connectors))
	for _, item := range connectors {
		t.connectors[item.ID] = item
		t.connectorNames[item.Name] = item.ID
	}

	return nil
}

func (t *Topology) OutputText() string {
	type Connect struct {
		From string
		To   string
	}

	data := struct {
		Topics     []*models.Topic
		Streams    []*models.Stream
		Connectors []*models.Connector
		Connects   []Connect
	}{}
	for _, t := range t.topics {
		data.Topics = append(data.Topics, t)
	}
	for _, s := range t.streams {
		data.Streams = append(data.Streams, s)

		if len(s.WriteStreams) != 0 {
			if _, ok := t.topicNames[s.Topic]; ok {
				data.Connects = append(data.Connects, Connect{From: s.ID, To: t.topicNames[s.Topic]})
			}
		}
		if len(s.ReadStreams) != 0 {
			if _, ok := t.topicNames[s.Topic]; ok {
				data.Connects = append(data.Connects, Connect{From: t.topicNames[s.Topic], To: s.ID})
			}

			for _, rs := range s.ReadStreams {
				if _, ok := t.streamNames[rs]; ok {
					data.Connects = append(data.Connects, Connect{From: s.ID, To: t.streamNames[rs]})
				}
			}
		}
	}
	for _, c := range t.connectors {
		data.Connectors = append(data.Connectors, c)
		for _, topic := range c.Topics {
			if _, ok := t.topicNames[topic]; ok {
				switch c.Type {
				case models.ConnectorTypeSink:
					data.Connects = append(data.Connects, Connect{From: t.topicNames[topic], To: c.ID})
				case models.ConnectorTypeSource:
					data.Connects = append(data.Connects, Connect{From: c.ID, To: t.topicNames[topic]})
				}
			}
		}
	}

	var result bytes.Buffer
	err := t.tpl.Execute(&result, data)
	if err != nil {
		log.Println("executing template:", err)
	}

	return result.String()
}
