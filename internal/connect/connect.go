package connect

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type Config struct {
	URL      string
	User     string
	Password string
}

type Connect struct {
	client *http.Client
	config Config
}

func New(client *http.Client, config Config) *Connect {
	if client == nil {
		client = http.DefaultClient
	}

	return &Connect{
		client: client,
		config: config,
	}
}

func (k *Connect) request(ctx context.Context, cmd string, params map[string]string) (*http.Response, error) {
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	u, err := url.Parse(k.config.URL)
	if err != nil {
		return nil, fmt.Errorf("connect.request parse url: %w", err)
	}
	u.Path = cmd
	u.RawQuery = values.Encode()
	log.Println(u.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("connect.request new request: %w", err)
	}
	req.SetBasicAuth(k.config.User, k.config.Password)
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		return nil, fmt.Errorf("invalid response status code [ %d ]: %s", resp.StatusCode, string(b))
	}

	return resp, nil
}
