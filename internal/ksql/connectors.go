package ksql

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/DivPro/topology/internal/models"
)

type ListConnectorItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ListConnectors struct {
	Type          string              `json:"@type"`
	StatementText string              `json:"statementText"`
	Connectors    []ListConnectorItem `json:"connectors"`
}

func (k *KSQL) FetchConnectors(ctx context.Context) ([]*models.Connector, error) {
	respRaw, err := k.request(ctx, "LIST CONNECTORS;")
	if err != nil {
		return nil, err
	}
	defer respRaw.Body.Close()

	var resp []ListConnectors
	dec := json.NewDecoder(respRaw.Body)
	err = dec.Decode(&resp)
	if err != nil {
		return nil, err
	}

	connectors := resp[0].Connectors
	res := make([]*models.Connector, 0, len(connectors))

	resCh := make(chan *models.Connector)
	var eg errgroup.Group
	eg.Go(func() error {
		defer close(resCh)
		var (
			eg errgroup.Group
		)
		for i := range connectors {
			connectorName := connectors[i]
			n := i
			eg.Go(func() error {
				model, err := k.fetchConnector(ctx, connectorName)
				if err != nil {
					return err
				}
				model.ID = fmt.Sprintf("c_%d", n)
				resCh <- model

				return nil
			})
		}
		if err = eg.Wait(); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		for c := range resCh {
			res = append(res, c)
		}
		return nil
	})
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

type OneConnector struct {
	Type          string `json:"@type"`
	StatementText string `json:"statementText"`
	Status        struct {
		Name  string `json:"name"`
		Tasks []struct {
			ID    int    `json:"id"`
			State string `json:"state"`
		} `json:"tasks"`
		Type string `json:"type"`
	} `json:"status"`
	Topics []string `json:"topics"`
}

func (k *KSQL) fetchConnector(ctx context.Context, c ListConnectorItem) (*models.Connector, error) {
	respRaw, err := k.request(ctx, fmt.Sprintf(`DESCRIBE CONNECTOR "%s";`, c.Name))
	if err != nil {
		return nil, err
	}
	defer respRaw.Body.Close()

	var resp []OneConnector
	dec := json.NewDecoder(respRaw.Body)
	err = dec.Decode(&resp)
	if err != nil {
		return nil, err
	}

	connector := resp[0]
	return &models.Connector{
		Name:      c.Name,
		Type:      models.ConnectorType(connector.Status.Type),
		Topics:    connector.Topics,
		TaskCount: int8(len(connector.Status.Tasks)),
	}, nil
}
