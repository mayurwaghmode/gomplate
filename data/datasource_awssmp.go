package data

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/pkg/errors"

	gaws "github.com/hairyhenderson/gomplate/v3/aws"
)

// awssmpGetter - A subset of SSM API for use in unit testing
type awssmpGetter interface {
	GetParameterWithContext(ctx context.Context, input *ssm.GetParameterInput, opts ...request.Option) (*ssm.GetParameterOutput, error)
	GetParametersByPathWithContext(ctx context.Context, input *ssm.GetParametersByPathInput, opts ...request.Option) (*ssm.GetParametersByPathOutput, error)
}

func readAWSSMP(ctx context.Context, source *Source, args ...string) (data []byte, err error) {
	if source.asmpg == nil {
		source.asmpg = ssm.New(gaws.SDKSession())
	}

	_, paramPath, err := parseDatasourceURLArgs(source.URL, args...)
	if err != nil {
		return nil, err
	}

	source.mediaType = jsonMimetype
	switch {
	case strings.HasSuffix(paramPath, "/"):
		source.mediaType = jsonArrayMimetype
		data, err = listAWSSMPParams(ctx, source, paramPath)
	default:
		data, err = readAWSSMPParam(ctx, source, paramPath)
	}
	return data, err
}

func readAWSSMPParam(ctx context.Context, source *Source, paramPath string) ([]byte, error) {
	input := &ssm.GetParameterInput{
		Name:           aws.String(paramPath),
		WithDecryption: aws.Bool(true),
	}

	response, err := source.asmpg.GetParameterWithContext(ctx, input)

	if err != nil {
		return nil, errors.Wrapf(err, "Error reading aws+smp from AWS using GetParameter with input %v", input)
	}

	result := *response.Parameter

	output, err := ToJSON(result)
	return []byte(output), err
}

// listAWSSMPParams - supports directory semantics, returns array
func listAWSSMPParams(ctx context.Context, source *Source, paramPath string) ([]byte, error) {
	input := &ssm.GetParametersByPathInput{
		Path: aws.String(paramPath),
	}

	response, err := source.asmpg.GetParametersByPathWithContext(ctx, input)
	if err != nil {
		return nil, errors.Wrapf(err, "Error reading aws+smp from AWS using GetParameter with input %v", input)
	}

	listing := make([]string, len(response.Parameters))
	for i, p := range response.Parameters {
		listing[i] = (*p.Name)[len(paramPath):]
	}

	output, err := ToJSON(listing)
	return []byte(output), err
}

func parseDatasourceURLArgs(sourceURL *url.URL, args ...string) (params map[string]interface{}, p string, err error) {
	if len(args) >= 2 {
		err = fmt.Errorf("maximum two arguments to %s datasource: alias, extraPath (found %d)",
			sourceURL.Scheme, len(args))
		return nil, "", err
	}

	p = sourceURL.Path
	params = make(map[string]interface{})
	for key, val := range sourceURL.Query() {
		params[key] = strings.Join(val, " ")
	}

	if p == "" && sourceURL.Opaque != "" {
		p = sourceURL.Opaque
	}

	if len(args) == 1 {
		parsed, err := url.Parse(args[0])
		if err != nil {
			return nil, "", err
		}

		if parsed.Path != "" {
			p = path.Join(p, parsed.Path)
			if strings.HasSuffix(parsed.Path, "/") {
				p += "/"
			}
		}

		for key, val := range parsed.Query() {
			params[key] = strings.Join(val, " ")
		}
	}
	return params, p, nil
}
