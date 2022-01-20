package connect

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DivPro/topology/internal/models"
)

type ListConnectorItem struct {
	Info struct {
		Name   string `json:"name"`
		Config struct {
			Class       string `json:"connector.class"`
			Query       string `json:"query"`
			Mode        string `json:"mode"`
			SourceTopic string `json:"topic.prefix"`
			SinkTopic   string `json:"topics"`
		} `json:"config"`
		Type  string `json:"type"`
		Tasks []struct {
			Connector string `json:"connector"`
			Task      int    `json:"task"`
		} `json:"tasks"`
	} `json:"info"`
}

func (k *Connect) FetchConnectors(ctx context.Context) ([]*models.Connector, error) {
	respRaw, err := k.request(ctx, "connectors", map[string]string{"expand": "info"})
	if err != nil {
		return nil, err
	}
	defer respRaw.Body.Close()

	var resp map[string]ListConnectorItem
	dec := json.NewDecoder(respRaw.Body)
	err = dec.Decode(&resp)
	if err != nil {
		return nil, err
	}

	res := make([]*models.Connector, 0, len(resp))
	var i int
	for _, c := range resp {
		t := models.ConnectorType(c.Info.Type)
		var topics []string
		switch t {
		case models.ConnectorTypeSource:
			topics = []string{c.Info.Config.SourceTopic}
		case models.ConnectorTypeSink:
			topics = []string{c.Info.Config.SinkTopic}
		}

		res = append(res, &models.Connector{
			ID:        fmt.Sprintf("c_%d", i),
			Name:      c.Info.Name,
			Type:      models.ConnectorType(c.Info.Type),
			Topics:    topics,
			TaskCount: int8(len(c.Info.Tasks)),
		})
		i++
	}

	return res, nil
}
