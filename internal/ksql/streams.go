package ksql

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/DivPro/topology/internal/models"
)

type ListStreamQuery struct {
	Query string   `json:"queryString"`
	Sinks []string `json:"sinks"`
}

type ListStreamFieldSchema struct {
	Type string `json:"type"`
}

type ListStreamField struct {
	Name   string                `json:"name"`
	Schema ListStreamFieldSchema `json:"schema"`
}

type ListStreamItem struct {
	Name         string            `json:"name"`
	ReadQueries  []ListStreamQuery `json:"readQueries"`
	WriteQueries []ListStreamQuery `json:"writeQueries"`
	Fields       []ListStreamField `json:"fields"`
	KeyFormat    string            `json:"keyFormat"`
	ValueFormat  string            `json:"valueFormat"`
	Topic        string            `json:"topic"`
}

type ListStream struct {
	Type          string           `json:"@type"`
	StatementText string           `json:"statementText"`
	Streams       []ListStreamItem `json:"sourceDescriptions"`
}

func (k *KSQL) FetchStreamsAndTables(ctx context.Context) ([]*models.Stream, error) {
	var (
		eg      errgroup.Group
		streams []*models.Stream
		tables  []*models.Stream
	)
	eg.Go(func() error {
		var err error
		streams, err = k.fetchStreamOrTable(ctx, false)
		return err
	})
	eg.Go(func() error {
		var err error
		tables, err = k.fetchStreamOrTable(ctx, true)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return append(streams, tables...), nil
}

func (k *KSQL) fetchStreamOrTable(ctx context.Context, isTable bool) ([]*models.Stream, error) {
	var q string
	if isTable {
		q = "LIST TABLES EXTENDED;"
	} else {
		q = "LIST STREAMS EXTENDED;"
	}

	respRaw, err := k.request(ctx, q)
	if err != nil {
		return nil, err
	}
	defer respRaw.Body.Close()

	var resp []ListStream
	dec := json.NewDecoder(respRaw.Body)
	err = dec.Decode(&resp)
	if err != nil {
		return nil, err
	}

	streams := resp[0].Streams
	res := make([]*models.Stream, len(streams))
	for i := range streams {
		stream := streams[i]
		fields := make([]models.Field, len(stream.Fields))
		for j := range stream.Fields {
			f := stream.Fields[j]
			fields[j] = models.Field{
				Name: f.Name,
				Type: f.Schema.Type,
			}
		}

		rs := make([]string, 0, len(stream.ReadQueries))
		for _, rq := range stream.ReadQueries {
			rs = append(rs, rq.Sinks...)
		}
		ws := make([]string, 0, len(stream.WriteQueries))
		for _, wq := range stream.WriteQueries {
			ws = append(ws, wq.Sinks...)
		}

		var prefix string
		if isTable {
			prefix = "tb"
		} else {
			prefix = "s"
		}
		res[i] = &models.Stream{
			ID:           fmt.Sprintf(prefix+"_%d", i),
			Name:         stream.Name,
			KeyFormat:    models.Encoding(stream.KeyFormat),
			ValueFormat:  models.Encoding(stream.ValueFormat),
			Fields:       fields,
			Topic:        stream.Topic,
			ReadStreams:  rs,
			WriteStreams: ws,
			IsTable:      isTable,
		}
	}

	return res, nil
}
