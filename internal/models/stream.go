package models

type Encoding string

const (
	EncodingKafka Encoding = "kafka"
	EncodingAVRO  Encoding = "avro"
	EncodingJSON  Encoding = "json"
)

type Stream struct {
	ID           string
	Name         string
	KeyFormat    Encoding
	ValueFormat  Encoding
	Fields       []Field
	Topic        string
	ReadStreams  []string
	WriteStreams []string
	IsTable      bool
}
