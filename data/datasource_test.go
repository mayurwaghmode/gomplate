package data

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/hairyhenderson/gomplate/v3/internal/config"

	"github.com/stretchr/testify/assert"
)

func mustParseURL(in string) *url.URL {
	u, _ := url.Parse(in)
	return u
}

func TestNewData(t *testing.T) {
	d, err := NewData(nil, nil)
	assert.NoError(t, err)
	assert.Len(t, d.Sources, 0)

	d, err = NewData([]string{"foo=http:///foo.json"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "/foo.json", d.Sources["foo"].URL.Path)

	d, err = NewData([]string{"foo=http:///foo.json"}, []string{})
	assert.NoError(t, err)
	assert.Equal(t, "/foo.json", d.Sources["foo"].URL.Path)
	assert.Empty(t, d.Sources["foo"].Header)

	d, err = NewData([]string{"foo=http:///foo.json"}, []string{"bar=Accept: blah"})
	assert.NoError(t, err)
	assert.Equal(t, "/foo.json", d.Sources["foo"].URL.Path)
	assert.Empty(t, d.Sources["foo"].Header)

	d, err = NewData([]string{"foo=http:///foo.json"}, []string{"foo=Accept: blah"})
	assert.NoError(t, err)
	assert.Equal(t, "/foo.json", d.Sources["foo"].URL.Path)
	assert.Equal(t, "blah", d.Sources["foo"].Header["Accept"][0])
}

func TestDatasource(t *testing.T) {
	setup := func(t *testing.T, ext string, contents []byte) *Data {
		t.Helper()
		fname := "foo." + ext

		fsys := fstest.MapFS{}
		fsys["tmp"] = &fstest.MapFile{Mode: fs.ModeDir | 0777}
		fsys["tmp/"+fname] = &fstest.MapFile{Data: contents}

		fsmux := fsimpl.NewMux()
		fsmux.Add(fsimpl.WrappedFSProvider(fsys, "file"))

		sources := map[string]*Source{
			"foo": {
				Alias: "foo",
				URL:   mustParseURL("file:///tmp/" + fname),
			},
		}
		return &Data{Sources: sources, FSMux: fsmux}
	}

	test := func(t *testing.T, ext, mime string, contents []byte, expected interface{}) {
		t.Helper()
		data := setup(t, ext, contents)

		actual, err := data.Datasource("foo", "?type="+mime)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	testObj := func(t *testing.T, ext, mime string, contents []byte) {
		test(t, ext, mime, contents,
			map[string]interface{}{
				"hello": map[string]interface{}{"cruel": "world"},
			})
	}

	testObj(t, "json", jsonMimetype, []byte(`{"hello":{"cruel":"world"}}`))
	testObj(t, "yml", yamlMimetype, []byte("hello:\n  cruel: world\n"))
	test(t, "json", jsonMimetype, []byte(`[1, "two", true]`),
		[]interface{}{1, "two", true})
	test(t, "yaml", yamlMimetype, []byte("---\n- 1\n- two\n- true\n"),
		[]interface{}{1, "two", true})

	d := setup(t, "", nil)
	actual, err := d.Datasource("foo")
	assert.NoError(t, err)
	assert.Equal(t, "", actual)

	_, err = d.Datasource("bar")
	assert.Error(t, err)
}

func TestDatasourceReachable(t *testing.T) {
	fsys := fstest.MapFS{}
	fsys["tmp/foo.json"] = &fstest.MapFile{Data: []byte("{}")}

	fsmux := fsimpl.NewMux()
	fsmux.Add(fsimpl.WrappedFSProvider(fsys, "file"))

	sources := map[string]*Source{
		"foo": {
			Alias:     "foo",
			URL:       mustParseURL("file:///tmp/foo.json"),
			mediaType: jsonMimetype,
		},
		"bar": {
			Alias: "bar",
			URL:   &url.URL{Scheme: "file", Path: "/bogus"},
		},
	}
	data := &Data{Sources: sources, FSMux: fsmux}

	assert.True(t, data.DatasourceReachable("foo"))
	assert.False(t, data.DatasourceReachable("bar"))
}

func TestDatasourceExists(t *testing.T) {
	sources := map[string]*Source{
		"foo": {Alias: "foo"},
	}
	data := &Data{Sources: sources}
	assert.True(t, data.DatasourceExists("foo"))
	assert.False(t, data.DatasourceExists("bar"))
}

func TestInclude(t *testing.T) {
	contents := "hello world"

	fsys := fstest.MapFS{}
	fsys["tmp/foo.txt"] = &fstest.MapFile{Data: []byte(contents)}

	fsmux := fsimpl.NewMux()
	fsmux.Add(fsimpl.WrappedFSProvider(fsys, "file"))

	sources := map[string]*Source{
		"foo": {
			Alias:     "foo",
			URL:       mustParseURL("file:///tmp/foo.txt"),
			mediaType: textMimetype,
		},
	}
	data := &Data{Sources: sources, FSMux: fsmux}
	actual, err := data.Include("foo")
	assert.NoError(t, err)
	assert.Equal(t, contents, actual)
}

type errorReader struct{}

func (e errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("error")
}

// nolint: megacheck
func TestDefineDatasource(t *testing.T) {
	d := &Data{}
	_, err := d.DefineDatasource("", "foo.json")
	assert.Error(t, err)

	d = &Data{}
	_, err = d.DefineDatasource("", "../foo.json")
	assert.Error(t, err)

	d = &Data{}
	_, err = d.DefineDatasource("", "ftp://example.com/foo.yml")
	assert.Error(t, err)

	d = &Data{}
	_, err = d.DefineDatasource("data", "foo.json")
	s := d.Sources["data"]
	assert.NoError(t, err)
	assert.Equal(t, "data", s.Alias)
	assert.Equal(t, "file", s.URL.Scheme)
	assert.True(t, s.URL.IsAbs())

	d = &Data{}
	_, err = d.DefineDatasource("data", "/otherdir/foo.json")
	s = d.Sources["data"]
	assert.NoError(t, err)
	assert.Equal(t, "data", s.Alias)
	assert.Equal(t, "file", s.URL.Scheme)
	assert.True(t, s.URL.IsAbs())
	assert.Equal(t, "/otherdir/foo.json", s.URL.Path)

	d = &Data{}
	_, err = d.DefineDatasource("data", "sftp://example.com/blahblah/foo.json")
	s = d.Sources["data"]
	assert.NoError(t, err)
	assert.Equal(t, "data", s.Alias)
	assert.Equal(t, "sftp", s.URL.Scheme)
	assert.True(t, s.URL.IsAbs())
	assert.Equal(t, "/blahblah/foo.json", s.URL.Path)

	d = &Data{
		Sources: map[string]*Source{
			"data": {Alias: "data"},
		},
	}
	_, err = d.DefineDatasource("data", "/otherdir/foo.json")
	s = d.Sources["data"]
	assert.NoError(t, err)
	assert.Equal(t, "data", s.Alias)
	assert.Nil(t, s.URL)

	d = &Data{}
	_, err = d.DefineDatasource("data", "/otherdir/foo?type=application/x-env")
	s = d.Sources["data"]
	assert.NoError(t, err)
	assert.Equal(t, "data", s.Alias)
	m, err := s.mimeType("")
	assert.NoError(t, err)
	assert.Equal(t, "application/x-env", m)
}

func TestFromConfig(t *testing.T) {
	ctx := context.Background()

	cfg := &config.Config{}
	actual := FromConfig(ctx, cfg)
	expected := &Data{
		Ctx:     actual.Ctx,
		Sources: map[string]*Source{},
	}
	assert.EqualValues(t, expected, actual)

	cfg = &config.Config{
		DataSources: map[string]config.DataSource{
			"foo": {
				URL: mustParseURL("http://example.com"),
			},
		},
	}
	actual = FromConfig(ctx, cfg)
	expected = &Data{
		Ctx: actual.Ctx,
		Sources: map[string]*Source{
			"foo": {
				Alias: "foo",
				URL:   mustParseURL("http://example.com"),
			},
		},
	}
	assert.EqualValues(t, expected, actual)

	cfg = &config.Config{
		DataSources: map[string]config.DataSource{
			"foo": {
				URL: mustParseURL("http://foo.com"),
			},
		},
		Context: map[string]config.DataSource{
			"bar": {
				URL: mustParseURL("http://bar.com"),
				Header: http.Header{
					"Foo": []string{"bar"},
				},
			},
		},
		ExtraHeaders: map[string]http.Header{
			"baz": {
				"Foo": []string{"bar"},
			},
		},
	}
	actual = FromConfig(ctx, cfg)
	expected = &Data{
		Ctx: actual.Ctx,
		Sources: map[string]*Source{
			"foo": {
				Alias: "foo",
				URL:   mustParseURL("http://foo.com"),
			},
			"bar": {
				Alias: "bar",
				URL:   mustParseURL("http://bar.com"),
				Header: http.Header{
					"Foo": []string{"bar"},
				},
			},
		},
		ExtraHeaders: map[string]http.Header{
			"baz": {
				"Foo": []string{"bar"},
			},
		},
	}
	assert.EqualValues(t, expected, actual)
}

func TestListDatasources(t *testing.T) {
	sources := map[string]*Source{
		"foo": {Alias: "foo"},
		"bar": {Alias: "bar"},
	}
	data := &Data{Sources: sources}

	assert.Equal(t, []string{"bar", "foo"}, data.ListDatasources())
}

func TestSplitFSMuxURL(t *testing.T) {
	t.Skip()
	testdata := []struct {
		in   string
		arg  string
		url  string
		file string
	}{
		{"http://example.com/foo.json", "", "http://example.com/", "foo.json"},
		{
			"http://example.com/foo.json?type=application/array+yaml",
			"",
			"http://example.com/?type=application/array+yaml",
			"foo.json",
		},
		{
			"vault:///secret/a/b/c", "",
			"vault:///",
			"secret/a/b/c",
		},
		{
			"vault:///secret/a/b/", "",
			"vault:///",
			"secret/a/b",
		},
		{
			"s3://bucket/a/b/", "",
			"s3://bucket/",
			"a/b",
		},
		{
			"vault:///", "foo/bar",
			"vault:///",
			"foo/bar",
		},
		{
			"consul://myhost/foo/?q=1", "bar/baz",
			"consul://myhost/?q=1",
			"foo/bar/baz",
		},
		{
			"consul://myhost/foo/?q=1", "bar/baz",
			"consul://myhost/?q=1",
			"foo/bar/baz",
		},
		{
			"git+https://example.com/myrepo", "//foo.yaml",
			"git+https://example.com/myrepo", "foo.yaml",
		},
		{
			"ssh://git@github.com/hairyhenderson/go-which.git//a/b/",
			"c/d?q=1",
			"ssh://git@github.com/hairyhenderson/go-which.git?q=1",
			"a/b/c/d",
		},
	}

	for _, d := range testdata {
		u, err := url.Parse(d.in)
		assert.NoError(t, err)
		url, file := splitFSMuxURL(u)
		assert.Equal(t, d.url, url.String())
		assert.Equal(t, d.file, file)
	}
}

func TestResolveURL(t *testing.T) {
	out, err := resolveURL(mustParseURL("http://example.com/foo.json"), "bar.json")
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com/bar.json", out.String())

	out, err = resolveURL(mustParseURL("http://example.com/a/b/?n=2"), "bar.json?q=1")
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com/a/b/bar.json?n=2&q=1", out.String())
}
