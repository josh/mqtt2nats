package main

import (
	"testing"
)

func TestMqttTopicToNatsSubject(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		addPrefix string
		want      string
	}{
		{
			name:      "foo/bar -> foo.bar (slash between two levels)",
			topic:     "foo/bar",
			addPrefix: "",
			want:      "foo.bar",
		},
		{
			name:      "/foo/bar -> /.foo.bar (slash as first level)",
			topic:     "/foo/bar",
			addPrefix: "",
			want:      "/.foo.bar",
		},
		{
			name:      "foo/bar/ -> foo.bar./ (slash as last level)",
			topic:     "foo/bar/",
			addPrefix: "",
			want:      "foo.bar./",
		},
		{
			name:      "foo//bar -> foo./.bar (slash next to another, middle)",
			topic:     "foo//bar",
			addPrefix: "",
			want:      "foo./.bar",
		},
		{
			name:      "//foo/bar -> /./.foo.bar (slash next to another, start)",
			topic:     "//foo/bar",
			addPrefix: "",
			want:      "/./.foo.bar",
		},
		{
			name:      "foo.bar -> foo//bar (dot character)",
			topic:     "foo.bar",
			addPrefix: "",
			want:      "foo//bar",
		},
		{
			name:      "simple topic with prefix",
			topic:     "foo/bar",
			addPrefix: "mqtt",
			want:      "mqtt.foo.bar",
		},
		{
			name:      "topic with dots and prefix",
			topic:     "foo.bar/baz",
			addPrefix: "mqtt",
			want:      "mqtt.foo//bar.baz",
		},
		{
			name:      "single level topic",
			topic:     "sensor",
			addPrefix: "",
			want:      "sensor",
		},
		{
			name:      "empty topic",
			topic:     "",
			addPrefix: "",
			want:      "/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mqttTopicToNatsSubject(tt.topic, tt.addPrefix)
			if got != tt.want {
				t.Errorf("mqttTopicToNatsSubject(%q, %q) = %q, want %q", tt.topic, tt.addPrefix, got, tt.want)
			}
		})
	}
}

func TestNatsSubjectToMqttTopic(t *testing.T) {
	tests := []struct {
		name         string
		subject      string
		removePrefix string
		want         string
	}{
		{
			name:         "foo.bar -> foo/bar (slash between two levels)",
			subject:      "foo.bar",
			removePrefix: "",
			want:         "foo/bar",
		},
		{
			name:         "/.foo.bar -> /foo/bar (slash as first level)",
			subject:      "/.foo.bar",
			removePrefix: "",
			want:         "/foo/bar",
		},
		{
			name:         "foo.bar./ -> foo/bar/ (slash as last level)",
			subject:      "foo.bar./",
			removePrefix: "",
			want:         "foo/bar/",
		},
		{
			name:         "foo./.bar -> foo//bar (slash next to another, middle)",
			subject:      "foo./.bar",
			removePrefix: "",
			want:         "foo//bar",
		},
		{
			name:         "/./.foo.bar -> //foo/bar (slash next to another, start)",
			subject:      "/./.foo.bar",
			removePrefix: "",
			want:         "//foo/bar",
		},
		{
			name:         "foo//bar -> foo.bar (double slash represents dot)",
			subject:      "foo//bar",
			removePrefix: "",
			want:         "foo.bar",
		},
		{
			name:         "simple subject with prefix",
			subject:      "mqtt.foo.bar",
			removePrefix: "mqtt",
			want:         "foo/bar",
		},
		{
			name:         "subject with double slashes and prefix",
			subject:      "mqtt.foo//bar.baz",
			removePrefix: "mqtt",
			want:         "foo.bar/baz",
		},
		{
			name:         "single level subject",
			subject:      "sensor",
			removePrefix: "",
			want:         "sensor",
		},
		{
			name:         "empty subject",
			subject:      "",
			removePrefix: "",
			want:         "",
		},
		{
			name:         "subject with prefix but not matching",
			subject:      "other.foo.bar",
			removePrefix: "mqtt",
			want:         "other/foo/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := natsSubjectToMqttTopic(tt.subject, tt.removePrefix)
			if got != tt.want {
				t.Errorf("natsSubjectToMqttTopic(%q, %q) = %q, want %q", tt.subject, tt.removePrefix, got, tt.want)
			}
		})
	}
}

func TestRoundTripConversion(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		addPrefix string
	}{
		{
			name:      "foo/bar (slash between two levels)",
			topic:     "foo/bar",
			addPrefix: "",
		},
		{
			name:      "/foo/bar (slash as first level)",
			topic:     "/foo/bar",
			addPrefix: "",
		},
		{
			name:      "foo/bar/ (slash as last level)",
			topic:     "foo/bar/",
			addPrefix: "",
		},
		{
			name:      "foo//bar (slash next to another, middle)",
			topic:     "foo//bar",
			addPrefix: "",
		},
		{
			name:      "//foo/bar (slash next to another, start)",
			topic:     "//foo/bar",
			addPrefix: "",
		},
		{
			name:      "foo.bar (dot character)",
			topic:     "foo.bar",
			addPrefix: "",
		},
		{
			name:      "foo/bar with prefix",
			topic:     "foo/bar",
			addPrefix: "mqtt",
		},
		{
			name:      "foo.bar/baz with prefix",
			topic:     "foo.bar/baz",
			addPrefix: "mqtt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := mqttTopicToNatsSubject(tt.topic, tt.addPrefix)
			got := natsSubjectToMqttTopic(subject, tt.addPrefix)
			if got != tt.topic {
				t.Errorf("round trip failed: original %q -> subject %q -> got %q", tt.topic, subject, got)
			}
		})
	}
}
