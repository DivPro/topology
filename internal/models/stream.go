package models

import (
	"regexp"
	"strconv"
	"time"
)

type Encoding string

const (
	EncodingKafka Encoding = "kafka"
	EncodingAVRO  Encoding = "avro"
	EncodingJSON  Encoding = "json"
)

var (
	statRegExpStream = regexp.MustCompile(`consumer-messages-per-sec:\s*([\d.]+)\s*consumer-total-bytes:\s*(\d+)\s*consumer-total-messages:\s*(\d+)\s*last-message:\s*(.+)`)
	statRegExpTable  = regexp.MustCompile(`consumer-messages-per-sec:\s*([\d.]+)\s*consumer-total-bytes:\s*(\d+)\s*consumer-total-messages:\s*(\d+)\s*messages-per-sec:\s*([\d.]+)\s*total-messages:\s*(\d+)\s*last-message:\s*(.+)`)
)

type StreamStatistics struct {
	ConsumerMsgPerSec  float64
	ConsumerBytesTotal int64
	ConsumerMsgTotal   int64
	MsgPerSec          float64
	MsgTotal           int64
	MsgLast            time.Time
}

func MustParseStreamStatistics(s string, isTable bool) StreamStatistics {
	var re *regexp.Regexp
	if isTable {
		re = statRegExpTable
	} else {
		re = statRegExpStream
	}

	matches := re.FindAllStringSubmatch(s, -1)
	if matches == nil {
		panic("no stat matches: " + s)
	}
	m := matches[0]

	var res StreamStatistics
	if isTable {
		t, _ := time.Parse(time.RFC3339, m[6])
		cps, _ := strconv.ParseFloat(m[1], 64)
		cbt, _ := strconv.ParseInt(m[2], 10, 64)
		cmt, _ := strconv.ParseInt(m[3], 10, 64)
		ps, _ := strconv.ParseFloat(m[4], 64)
		mt, _ := strconv.ParseInt(m[5], 10, 64)

		res = StreamStatistics{
			ConsumerMsgPerSec:  cps,
			ConsumerBytesTotal: cbt,
			ConsumerMsgTotal:   cmt,
			MsgPerSec:          ps,
			MsgTotal:           mt,
			MsgLast:            t,
		}
	} else {
		t, _ := time.Parse(time.RFC3339, m[4])
		cps, _ := strconv.ParseFloat(m[1], 64)
		cbt, _ := strconv.ParseInt(m[2], 10, 64)
		cmt, _ := strconv.ParseInt(m[3], 10, 64)

		res = StreamStatistics{
			ConsumerMsgPerSec:  cps,
			ConsumerBytesTotal: cbt,
			ConsumerMsgTotal:   cmt,
			MsgLast:            t,
		}
	}

	return res
}

type Stream struct {
	ID           string
	Name         string
	KeyFormat    Encoding
	ValueFormat  Encoding
	Fields       []Field
	Topic        string
	ReadStreams  []string
	WriteStreams []string
	Statistics   StreamStatistics
	IsTable      bool
}
