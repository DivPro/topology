package models

type Topic struct {
	ID               string
	Name             string
	PartitionCount   int8
	ReplicationCount int8
}
