package data

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/hairyhenderson/go-fsimpl/awssmfs"
	"github.com/hairyhenderson/go-fsimpl/blobfs"
	"github.com/hairyhenderson/go-fsimpl/filefs"
	"github.com/hairyhenderson/go-fsimpl/gitfs"
	"github.com/hairyhenderson/go-fsimpl/httpfs"
	"github.com/hairyhenderson/go-fsimpl/vaultfs"
	"github.com/hairyhenderson/gomplate/v3/internal/config"
	"github.com/hairyhenderson/gomplate/v3/internal/datafs"
	"github.com/hairyhenderson/gomplate/v3/libkv"
)

func regExtension(ext, typ string) {
	err := mime.AddExtensionType(ext, typ)
	if err != nil {
		panic(err)
	}
}

func init() {
	// Add some types we want to be able to handle which can be missing by default
	regExtension(".json", jsonMimetype)
	regExtension(".yml", yamlMimetype)
	regExtension(".yaml", yamlMimetype)
	regExtension(".csv", csvMimetype)
	regExtension(".toml", tomlMimetype)
	regExtension(".env", envMimetype)
}

// registerReaders registers the source-reader functions
func (d *Data) registerReaders() {
	d.sourceReaders = make(map[string]func(context.Context, *Source, ...string) ([]byte, error))

	d.sourceReaders["aws+smp"] = readAWSSMP
	d.sourceReaders["consul"] = readConsul
	d.sourceReaders["consul+http"] = readConsul
	d.sourceReaders["consul+https"] = readConsul
	d.sourceReaders["merge"] = d.readMerge
	d.sourceReaders["stdin"] = readStdin
}

// lookupReader - return the reader function for the given scheme
func (d *Data) lookupReader(scheme string) (func(context.Context, *Source, ...string) ([]byte, error), error) {
	if d.sourceReaders == nil {
		d.registerReaders()
	}
	r, ok := d.sourceReaders[scheme]
	if !ok {
		return nil, errors.Errorf("scheme %s not registered", scheme)
	}
	return r, nil
}

// Data -
// Deprecated: will be replaced in future
type Data struct {
	Ctx context.Context

	Sources map[string]*Source

	sourceReaders map[string]func(context.Context, *Source, ...string) ([]byte, error)
	cache         map[string]*fileContent

	// headers from the --datasource-header/-H option that don't reference datasources from the commandline
	ExtraHeaders map[string]http.Header

	FSMux fsimpl.FSMux
}

type fileContent struct {
	b           []byte
	contentType string
}

// Cleanup - clean up datasources before shutting the process down - things
// like Logging out happen here
func (d *Data) Cleanup() {
	for _, s := range d.Sources {
		s.cleanup()
	}
}

// NewData - constructor for Data
// Deprecated: will be replaced in future
func NewData(datasourceArgs, headerArgs []string) (*Data, error) {
	cfg := &config.Config{}
	err := cfg.ParseDataSourceFlags(datasourceArgs, nil, nil, headerArgs)
	if err != nil {
		return nil, err
	}
	data := FromConfig(context.Background(), cfg)
	return data, nil
}

// FromConfig - internal use only!
func FromConfig(ctx context.Context, cfg *config.Config) *Data {
	// XXX: This is temporary, and will be replaced with something a bit cleaner
	// when datasources are refactored
	ctx = ContextWithStdin(ctx, cfg.Stdin)

	sources := map[string]*Source{}
	for alias, d := range cfg.DataSources {
		sources[alias] = &Source{
			Alias:  alias,
			URL:    d.URL,
			Header: d.Header,
		}
	}
	for alias, d := range cfg.Context {
		sources[alias] = &Source{
			Alias:  alias,
			URL:    d.URL,
			Header: d.Header,
		}
	}
	return &Data{
		Ctx:          ctx,
		Sources:      sources,
		ExtraHeaders: cfg.ExtraHeaders,
	}
}

// Source - a data source
// Deprecated: will be replaced in future
type Source struct {
	Alias     string
	URL       *url.URL
	Header    http.Header  // used for http[s]: URLs, nil otherwise
	kv        *libkv.LibKV // used for consul:, etcd:, zookeeper: & boltdb: URLs, nil otherwise
	asmpg     awssmpGetter // used for aws+smp:, nil otherwise
	mediaType string
}

func (s *Source) inherit(parent *Source) {
	s.kv = parent.kv
	s.asmpg = parent.asmpg
}

func (s *Source) cleanup() {
	if s.kv != nil {
		s.kv.Logout()
	}
}

// String is the method to format the flag's value, part of the flag.Value interface.
// The String method's output will be used in diagnostics.
func (s *Source) String() string {
	return fmt.Sprintf("%s=%s (%s)", s.Alias, s.URL.String(), s.mediaType)
}

// DefineDatasource -
func (d *Data) DefineDatasource(alias, value string) (string, error) {
	if alias == "" {
		return "", errors.New("datasource alias must be provided")
	}
	if d.DatasourceExists(alias) {
		return "", nil
	}
	srcURL, err := config.ParseSourceURL(value)
	if err != nil {
		return "", err
	}
	s := &Source{
		Alias:  alias,
		URL:    srcURL,
		Header: d.ExtraHeaders[alias],
	}
	if d.Sources == nil {
		d.Sources = make(map[string]*Source)
	}
	d.Sources[alias] = s
	return "", nil
}

// DatasourceExists -
func (d *Data) DatasourceExists(alias string) bool {
	_, ok := d.Sources[alias]
	return ok
}

func (d *Data) lookupSource(alias string) (*Source, error) {
	source, ok := d.Sources[alias]
	if !ok {
		srcURL, err := url.Parse(alias)
		if err != nil || !srcURL.IsAbs() {
			return nil, errors.Errorf("Undefined datasource '%s'", alias)
		}
		source = &Source{
			Alias:  alias,
			URL:    srcURL,
			Header: d.ExtraHeaders[alias],
		}
		d.Sources[alias] = source
	}
	if source.Alias == "" {
		source.Alias = alias
	}
	return source, nil
}

func (d *Data) readDataSource(ctx context.Context, alias string, args ...string) (*fileContent, error) {
	source, err := d.lookupSource(alias)
	if err != nil {
		return nil, err
	}
	fc, err := d.readSource(ctx, source, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't read datasource '%s'", alias)
	}

	return fc, nil
}

// Include -
func (d *Data) Include(alias string, args ...string) (string, error) {
	fc, err := d.readDataSource(d.Ctx, alias, args...)
	return string(fc.b), err
}

// Datasource -
func (d *Data) Datasource(alias string, args ...string) (interface{}, error) {
	fc, err := d.readDataSource(d.Ctx, alias, args...)
	if err != nil {
		return nil, err
	}

	return parseData(fc.contentType, string(fc.b))
}

func parseData(mimeType, s string) (out interface{}, err error) {
	switch mimeAlias(mimeType) {
	case jsonMimetype:
		out, err = JSON(s)
		if err != nil {
			// maybe it's a JSON array
			out, err = JSONArray(s)
		}
	case jsonArrayMimetype:
		out, err = JSONArray(s)
	case yamlMimetype:
		out, err = YAML(s)
		if err != nil {
			// maybe it's a YAML array
			out, err = YAMLArray(s)
		}
	case csvMimetype:
		out, err = CSV(s)
	case tomlMimetype:
		out, err = TOML(s)
	case envMimetype:
		out, err = dotEnv(s)
	case textMimetype:
		out = s
	default:
		return nil, errors.Errorf("Datasources of type %s not yet supported", mimeType)
	}
	return out, err
}

// DatasourceReachable - Determines if the named datasource is reachable with
// the given arguments. Reads from the datasource, and discards the returned data.
func (d *Data) DatasourceReachable(alias string, args ...string) bool {
	source, ok := d.Sources[alias]
	if !ok {
		return false
	}
	_, err := d.readSource(d.Ctx, source, args...)
	return err == nil
}

// readSource returns the (possibly cached) data from the given source,
// as referenced by the given args
func (d *Data) readSource(ctx context.Context, source *Source, args ...string) (*fileContent, error) {
	if d.cache == nil {
		d.cache = make(map[string]*fileContent)
	}
	cacheKey := source.Alias
	for _, v := range args {
		cacheKey += v
	}
	cached, ok := d.cache[cacheKey]
	if ok {
		return cached, nil
	}

	var data []byte

	// TODO: initialize this elsewhere?
	if d.FSMux == nil {
		d.FSMux = fsimpl.NewMux()
		d.FSMux.Add(filefs.FS)
		d.FSMux.Add(httpfs.FS)
		d.FSMux.Add(blobfs.FS)
		d.FSMux.Add(gitfs.FS)
		d.FSMux.Add(vaultfs.FS)
		d.FSMux.Add(awssmfs.FS)
		d.FSMux.Add(datafs.EnvFS)
	}

	arg := ""
	if len(args) > 0 {
		arg = args[0]
	}

	u, err := resolveURL(source.URL, arg)
	if err != nil {
		return nil, err
	}

	// possible type hint
	mimeType := u.Query().Get("type")

	u, fname := splitFSMuxURL(u)

	fsys, err := d.FSMux.Lookup(u.String())
	if err == nil {
		f, err := fsys.Open(fname)
		if err != nil {
			return nil, fmt.Errorf("open (url: %q, name: %q): %w", u, fname, err)
		}

		fi, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat (url: %q, name: %q): %w", u, fname, err)
		}

		if mimeType == "" {
			mimeType = fsimpl.ContentType(fi)
		}

		if fi.IsDir() {
			des, err := fs.ReadDir(fsys, fname)
			if err != nil {
				return nil, fmt.Errorf("readDir (url: %q, name: %s): %w", u, fname, err)
			}

			entries := make([]string, len(des))
			for i, e := range des {
				entries[i] = e.Name()
			}
			data, err = json.Marshal(entries)
			if err != nil {
				return nil, fmt.Errorf("json.Marshal: %w", err)
			}

			mimeType = jsonArrayMimetype
		} else {
			data, err = ioutil.ReadAll(f)

			if err != nil {
				return nil, fmt.Errorf("read (url: %q, name: %s): %w", u, fname, err)
			}
		}

		fc := &fileContent{data, mimeType}
		d.cache[cacheKey] = fc

		return fc, nil
	}

	// TODO: get rid of this, I guess?
	r, err := d.lookupReader(u.Scheme)
	if err != nil {
		return nil, fmt.Errorf("lookupReader (url: %q): %w", u, err)
	}
	data, err = r(ctx, source, args...)
	if err != nil {
		return nil, err
	}

	if mimeType == "" {
		subpath := ""
		if len(args) > 0 {
			subpath = args[0]
		}

		mimeType, err = source.mimeType(subpath)
		if err != nil {
			return nil, err
		}
	}

	fc := &fileContent{data, mimeType}
	d.cache[cacheKey] = fc
	return fc, nil
}

// Show all datasources  -
func (d *Data) ListDatasources() []string {
	datasources := make([]string, 0, len(d.Sources))
	for source := range d.Sources {
		datasources = append(datasources, source)
	}
	sort.Strings(datasources)
	return datasources
}

// resolveURL parses the relative URL rel against base, and returns the
// resolved URL. Differs from url.ResolveReference in that query parameters are
// added. In case of duplicates, params from rel are used.
func resolveURL(base *url.URL, rel string) (*url.URL, error) {
	relURL, err := url.Parse(rel)
	if err != nil {
		return nil, err
	}

	out := base.ResolveReference(relURL)
	if base.RawQuery != "" {
		bq := base.Query()
		rq := relURL.Query()
		for k := range rq {
			bq.Set(k, rq.Get(k))
		}
		out.RawQuery = bq.Encode()
	}

	return out, nil
}

// splitFSMuxURL splits a URL into a filesystem URL and a relative file path
func splitFSMuxURL(in *url.URL) (*url.URL, string) {
	u := *in

	// base := path.Base(u.Path)
	// if path.Dir(u.Path) == path.Clean(u.Path) {
	// 	base = "."
	// }

	base := strings.TrimPrefix(u.Path, "/")

	if base == "" && u.Opaque != "" {
		base = u.Opaque
		u.Opaque = ""
	}

	if base == "" {
		base = "."
	}

	u.Path = "/"

	// handle some env-specific idiosyncrasies
	// if u.Scheme == "env" {
	// 	base = in.Path
	// 	base = strings.TrimPrefix(base, "/")
	// 	if base == "" {
	// 		base = in.Opaque
	// 	}
	// }
	// if u.Scheme == "vault" && !strings.HasSuffix(u.Path, "/") && u.Path != "" {
	// 	u.Path += "/"
	// }
	// if u.Scheme == "s3" && !strings.HasSuffix(u.Path, "/") && u.Path != "" {
	// 	u.Path += "/"
	// }

	return &u, base
}
