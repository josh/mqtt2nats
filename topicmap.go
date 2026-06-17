package main

import (
	"errors"
	"fmt"
)

// Topic/subject separators and wildcards. These mirror the NATS server's MQTT
// implementation so the mapping is byte-for-byte compatible with what a native
// NATS MQTT (3.1.1) deployment would produce.
const (
	mqttTopicLevelSep = '/' // MQTT topic level separator
	natsTokenSep      = '.' // NATS subject token separator
	natsPWC           = '*' // NATS single-level wildcard
	natsFWC           = '>' // NATS multi-level wildcard
	mqttSingleLevelWC = '+' // MQTT single-level wildcard
	mqttMultiLevelWC  = '#' // MQTT multi-level wildcard
)

// errUnsupportedTopicChars is returned for topics containing characters that
// cannot be represented in a NATS subject (whitespace, control chars, DEL):
// they would corrupt the NATS wire protocol.
var errUnsupportedTopicChars = errors.New("topic contains characters unsupported in NATS subjects")

// TopicToSubject converts a concrete MQTT topic into a NATS subject, replicating
// the NATS server's mqttToNATSSubjectConversion exactly:
//   - '/' becomes '.'; leading/trailing/empty levels are preserved via a '/.'
//     sentinel; a literal '.' is escaped to '//'.
//   - whitespace/control/DEL are rejected.
//   - '+' and '#' are rejected here (wildcards are not valid in a publish topic).
//
// The conversion is reversible for any topic it accepts: see SubjectToTopic.
func TopicToSubject(topic string) (string, error) {
	if topic == "" {
		return "", errors.New("empty topic")
	}
	out, err := mqttToNATSSubjectConversion([]byte(topic))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// SubjectToTopic converts a NATS subject back into an MQTT topic, the inverse of
// TopicToSubject (replicating the NATS server's natsSubjectToMQTTTopic).
func SubjectToTopic(subject string) string {
	return string(natsSubjectToMQTTTopic([]byte(subject)))
}

// mqttToNATSSubjectConversion is a direct port of the NATS server routine. We
// only ever convert concrete publish topics, so wildcards are not allowed.
func mqttToNATSSubjectConversion(mt []byte) ([]byte, error) {
	var cp bool
	var j int
	res := mt

	makeCopy := func(i int) {
		cp = true
		res = make([]byte, 0, len(mt)+10)
		if i > 0 {
			res = append(res, mt[:i]...)
		}
	}

	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch mt[i] {
		case mqttTopicLevelSep:
			if i == 0 || res[j-1] == natsTokenSep {
				if !cp {
					makeCopy(0)
				}
				res = append(res, mqttTopicLevelSep, natsTokenSep)
				j++
			} else if i == end || mt[i+1] == mqttTopicLevelSep {
				if !cp {
					makeCopy(i)
				}
				res = append(res, natsTokenSep, mqttTopicLevelSep)
				j++
			} else {
				if !cp {
					makeCopy(i)
				}
				res = append(res, natsTokenSep)
			}
		case ' ', '\t', '\n', '\r', '\f', 0x7f:
			return nil, errUnsupportedTopicChars
		case natsTokenSep:
			if !cp {
				makeCopy(i)
			}
			res = append(res, mqttTopicLevelSep, mqttTopicLevelSep)
			j++
		case mqttSingleLevelWC, mqttMultiLevelWC:
			return nil, fmt.Errorf("wildcards not allowed in publish topic: %q", mt)
		default:
			if cp {
				res = append(res, mt[i])
			}
		}
		j++
	}
	if cp && res[j-1] == natsTokenSep {
		res = append(res, mqttTopicLevelSep)
		j++
	}
	return res[:j], nil
}

// natsSubjectToMQTTTopic is a direct port of the NATS server routine.
func natsSubjectToMQTTTopic(subject []byte) []byte {
	topic := make([]byte, len(subject))
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if i < end {
				switch c := subject[i+1]; c {
				case natsTokenSep, mqttTopicLevelSep:
					if c == natsTokenSep {
						topic[j] = mqttTopicLevelSep
					} else {
						topic[j] = natsTokenSep
					}
					j++
					i++
				default:
				}
			}
		case natsTokenSep:
			topic[j] = mqttTopicLevelSep
			j++
		default:
			topic[j] = subject[i]
			j++
		}
	}
	return topic[:j]
}
