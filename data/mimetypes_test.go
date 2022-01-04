package data

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMimeAlias(t *testing.T) {
	t.Parallel()
	data := []struct {
		in, out string
	}{
		{csvMimetype, csvMimetype},
		{yamlMimetype, yamlMimetype},
		{"application/x-yaml", yamlMimetype},
	}

	for _, d := range data {
		assert.Equal(t, d.out, mimeAlias(d.in))
	}
}

func TestMimeType(t *testing.T) {
	s := &Source{URL: mustParseURL("http://example.com/list?type=a/b/c")}
	_, err := s.mimeType("")
	assert.Error(t, err)

	data := []struct {
		url       string
		mediaType string
		expected  string
	}{
		{"http://example.com/foo.json",
			"",
			jsonMimetype},
		{"http://example.com/foo.json",
			"text/foo",
			"text/foo"},
		{"http://example.com/foo.json?type=application/yaml",
			"text/foo",
			"application/yaml"},
		{"http://example.com/list?type=application/array%2Bjson",
			"text/foo",
			"application/array+json"},
		{"http://example.com/list?type=application/array+json",
			"",
			"application/array+json"},
		{"http://example.com/unknown",
			"",
			"text/plain"},
	}

	for i, d := range data {
		d := d
		t.Run(fmt.Sprintf("%d:%q,%q==%q", i, d.url, d.mediaType, d.expected), func(t *testing.T) {
			s := &Source{URL: mustParseURL(d.url), mediaType: d.mediaType}
			mt, err := s.mimeType("")
			assert.NoError(t, err)
			assert.Equal(t, d.expected, mt)

			mt, err = guessMimeType(mustParseURL(d.url), "", d.mediaType)
			assert.NoError(t, err)
			assert.Equal(t, d.expected, mt)
		})
	}
}

func TestMimeTypeWithArg(t *testing.T) {
	s := &Source{URL: mustParseURL("http://example.com")}
	_, err := s.mimeType("h\nttp://foo")
	assert.Error(t, err)

	data := []struct {
		url       string
		mediaType string
		arg       string
		expected  string
	}{
		{"http://example.com/unknown",
			"",
			"/foo.json",
			"application/json"},
		{"http://example.com/unknown",
			"",
			"foo.json",
			"application/json"},
		{"http://example.com/",
			"text/foo",
			"/foo.json",
			"text/foo"},
		{"git+https://example.com/myrepo",
			"",
			"//foo.yaml",
			"application/yaml"},
		{"http://example.com/foo.json",
			"",
			"/foo.yaml",
			"application/yaml"},
		{"http://example.com/foo.json?type=application/array+yaml",
			"",
			"/foo.yaml",
			"application/array+yaml"},
		{"http://example.com/foo.json?type=application/array+yaml",
			"",
			"/foo.yaml?type=application/yaml",
			"application/yaml"},
		{"http://example.com/foo.json?type=application/array+yaml",
			"text/plain",
			"/foo.yaml?type=application/yaml",
			"application/yaml"},
	}

	for i, d := range data {
		d := d
		t.Run(fmt.Sprintf("%d:%q,%q,%q==%q", i, d.url, d.mediaType, d.arg, d.expected), func(t *testing.T) {
			s := &Source{URL: mustParseURL(d.url), mediaType: d.mediaType}
			mt, err := s.mimeType(d.arg)
			assert.NoError(t, err)
			assert.Equal(t, d.expected, mt)

			mt, err = guessMimeType(mustParseURL(d.url), d.arg, d.mediaType)
			assert.NoError(t, err)
			assert.Equal(t, d.expected, mt)
		})
	}
}
