package ksql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DivPro/topology/internal/models"
)

type ListTopicItem struct {
	Name        string `json:"name"`
	ReplicaInfo []int8 `json:"replicaInfo"`
}

type ListTopics struct {
	Type          string          `json:"@type"`
	StatementText string          `json:"statementText"`
	Topics        []ListTopicItem `json:"topics"`
}

func (k *KSQL) FetchTopics(ctx context.Context) ([]*models.Topic, error) {
	respRaw, err := k.request(ctx, "LIST TOPICS;")
	if err != nil {
		return nil, err
	}
	defer respRaw.Body.Close()

	var resp []ListTopics
	dec := json.NewDecoder(respRaw.Body)
	err = dec.Decode(&resp)
	if err != nil {
		return nil, err
	}

	topics := resp[0].Topics
	res := make([]*models.Topic, len(topics))
	for i := range topics {
		topic := topics[i]
		res[i] = &models.Topic{
			ID:               fmt.Sprintf("t_%d", i),
			Name:             topic.Name,
			PartitionCount:   int8(len(topic.ReplicaInfo)),
			ReplicationCount: topic.ReplicaInfo[0],
		}
	}

	return res, nil
}
