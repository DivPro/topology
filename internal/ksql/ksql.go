package ksql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type Request struct {
	Query string `json:"ksql"`
}

type Config struct {
	URL      string
	User     string
	Password string
}

type KSQL struct {
	client *http.Client
	config Config
}

func NewKSQL(client *http.Client, config Config) *KSQL {
	if client == nil {
		client = http.DefaultClient
	}

	return &KSQL{
		client: client,
		config: config,
	}
}

func (k *KSQL) request(ctx context.Context, cmd string) (*http.Response, error) {
	r := Request{Query: cmd}
	b, err := json.MarshalIndent(r, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("ksql.request marshal json: %w", err)
	}
	log.Println(string(b))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, k.config.URL, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("ksql.request new request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ksql.v1+json")
	req.SetBasicAuth(k.config.User, k.config.Password)

	return k.client.Do(req)
}
