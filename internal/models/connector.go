package models

type ConnectorType string

const (
	ConnectorTypeSource ConnectorType = "source"
	ConnectorTypeSink   ConnectorType = "sink"
)

type Connector struct {
	ID        string
	Name      string
	Type      ConnectorType
	Topics    []string
	TaskCount int8
}
