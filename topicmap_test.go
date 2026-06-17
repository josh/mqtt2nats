package main

import "testing"

func TestTopicToSubject(t *testing.T) {
	cases := []struct {
		topic   string
		subject string
	}{
		{"home/room/temp", "home.room.temp"},
		{"a", "a"},
		{"a/b", "a.b"},
		{"/foo", "/.foo"},
		{"foo/", "foo./"},
		{"a.b", "a//b"}, // literal dot escaped
		{"home/floor1.5/temp", "home.floor1//5.temp"},
	}
	for _, c := range cases {
		got, err := TopicToSubject(c.topic)
		if err != nil {
			t.Errorf("TopicToSubject(%q) unexpected error: %v", c.topic, err)
			continue
		}
		if got != c.subject {
			t.Errorf("TopicToSubject(%q) = %q, want %q", c.topic, got, c.subject)
		}
		if back := SubjectToTopic(got); back != c.topic {
			t.Errorf("round-trip %q -> %q -> %q", c.topic, got, back)
		}
	}
}

func TestTopicToSubjectErrors(t *testing.T) {
	for _, topic := range []string{"", "a b", "a\tb", "a\nb", "with#wild", "with+wild", "x\x7f"} {
		if _, err := TopicToSubject(topic); err == nil {
			t.Errorf("TopicToSubject(%q) = nil error, want error", topic)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	topics := []string{
		"home/room/temp", "a", "a/b/c", "/foo", "foo/", "a//b", "a.b",
		"x/y.z/w", "a/b/", "/", "//", "device/123/status",
		"unicode/✓/ok", "a*b", "a>b", "trailing.",
	}
	for _, topic := range topics {
		subj, err := TopicToSubject(topic)
		if err != nil {
			t.Errorf("TopicToSubject(%q): %v", topic, err)
			continue
		}
		if back := SubjectToTopic(subj); back != topic {
			t.Errorf("round-trip %q -> %q -> %q", topic, subj, back)
		}
	}
}

func FuzzTopicRoundTrip(f *testing.F) {
	for _, s := range []string{"a/b", "/x/", "a.b", "a//b", "x", "//"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, topic string) {
		subj, err := TopicToSubject(topic)
		if err != nil {
			return // rejected topics are not required to round-trip
		}
		if back := SubjectToTopic(subj); back != topic {
			t.Errorf("round-trip mismatch: %q -> %q -> %q", topic, subj, back)
		}
	})
}
