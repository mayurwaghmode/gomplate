package data

import (
	"fmt"
	"mime"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

const (
	textMimetype      = "text/plain"
	csvMimetype       = "text/csv"
	jsonMimetype      = "application/json"
	jsonArrayMimetype = "application/array+json"
	tomlMimetype      = "application/toml"
	yamlMimetype      = "application/yaml"
	envMimetype       = "application/x-env"
)

// mimeTypeAliases defines a mapping for non-canonical mime types that are
// sometimes seen in the wild
var mimeTypeAliases = map[string]string{
	"application/x-yaml": yamlMimetype,
	"application/text":   textMimetype,
}

func mimeAlias(m string) string {
	if a, ok := mimeTypeAliases[m]; ok {
		return a
	}
	return m
}

// mimeType returns the MIME type to use as a hint for parsing the datasource.
// It's expected that the datasource will have already been read before
// this function is called, and so the Source's Type property may be already set.
//
// The MIME type is determined by these rules:
// 1. the 'type' URL query parameter is used if present
// 2. otherwise, the Type property on the Source is used, if present
// 3. otherwise, a MIME type is calculated from the file extension, if the extension is registered
// 4. otherwise, the default type of 'text/plain' is used
func (s *Source) mimeType(arg string) (mimeType string, err error) {
	if len(arg) > 0 {
		if strings.HasPrefix(arg, "//") {
			arg = arg[1:]
		}
		if !strings.HasPrefix(arg, "/") {
			arg = "/" + arg
		}
	}
	argURL, err := url.Parse(arg)
	if err != nil {
		return "", fmt.Errorf("mimeType: couldn't parse arg %q: %w", arg, err)
	}
	mediatype := argURL.Query().Get("type")
	if mediatype == "" {
		mediatype = s.URL.Query().Get("type")
	}

	if mediatype == "" {
		mediatype = s.mediaType
	}

	// make it so + doesn't need to be escaped
	mediatype = strings.ReplaceAll(mediatype, " ", "+")

	if mediatype == "" {
		ext := filepath.Ext(argURL.Path)
		mediatype = mime.TypeByExtension(ext)
	}

	if mediatype == "" {
		ext := filepath.Ext(s.URL.Path)
		mediatype = mime.TypeByExtension(ext)
	}

	if mediatype != "" {
		t, _, err := mime.ParseMediaType(mediatype)
		if err != nil {
			return "", errors.Wrapf(err, "MIME type was %q", mediatype)
		}
		mediatype = t
		return mediatype, nil
	}

	return textMimetype, nil
}

// mimeType returns the MIME type to use as a hint for parsing the datasource.
// It's expected that the datasource will have already been read before
// this function is called, and so the Source's Type property may be already set.
//
// The MIME type is determined by these rules:
// 1. the 'type' URL query parameter is used if present
// 2. otherwise, the Type property on the Source is used, if present
// 3. otherwise, a MIME type is calculated from the file extension, if the extension is registered
// 4. otherwise, the default type of 'text/plain' is used
func guessMimeType(base *url.URL, name, mimeGuess string) (mimeType string, err error) {
	if len(name) > 0 {
		if strings.HasPrefix(name, "//") {
			name = name[1:]
		}
		if !strings.HasPrefix(name, "/") {
			name = "/" + name
		}
	}
	nameURL, err := url.Parse(name)
	if err != nil {
		return "", fmt.Errorf("mimeType: couldn't parse name %q: %w", name, err)
	}
	mediatype := nameURL.Query().Get("type")
	if mediatype == "" {
		mediatype = base.Query().Get("type")
	}

	if mediatype == "" {
		mediatype = mimeGuess
	}

	// make it so + doesn't need to be escaped
	mediatype = strings.ReplaceAll(mediatype, " ", "+")

	if mediatype == "" {
		ext := filepath.Ext(nameURL.Path)
		mediatype = mime.TypeByExtension(ext)
	}

	if mediatype == "" {
		ext := filepath.Ext(base.Path)
		mediatype = mime.TypeByExtension(ext)
	}

	if mediatype != "" {
		t, _, err := mime.ParseMediaType(mediatype)
		if err != nil {
			return "", errors.Wrapf(err, "MIME type was %q", mediatype)
		}
		mediatype = t
		return mediatype, nil
	}

	return textMimetype, nil
}
