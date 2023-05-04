// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package elasticsearch

import (
	"context"

	"github.com/uber/cadence/common/elasticsearch/bulk"

	"github.com/olivere/elastic"
)

var _ bulk.GenericBulkProcessor = (*v6BulkProcessor)(nil)

type v6BulkProcessor struct {
	processor *elastic.BulkProcessor
}

func (c *elasticV6) RunBulkProcessor(ctx context.Context, parameters *bulk.BulkProcessorParameters) (bulk.GenericBulkProcessor, error) {
	beforeFunc := func(executionId int64, requests []elastic.BulkableRequest) {
		parameters.BeforeFunc(executionId, fromV6ToGenericBulkableRequests(requests))
	}

	afterFunc := func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
		gerr := convertV6ErrorToGenericError(err)
		parameters.AfterFunc(
			executionId,
			fromV6ToGenericBulkableRequests(requests),
			fromV6toGenericBulkResponse(response),
			gerr)
	}

	processor, err := c.client.BulkProcessor().
		Name(parameters.Name).
		Workers(parameters.NumOfWorkers).
		BulkActions(parameters.BulkActions).
		BulkSize(parameters.BulkSize).
		FlushInterval(parameters.FlushInterval).
		Backoff(parameters.Backoff).
		Before(beforeFunc).
		After(afterFunc).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &v6BulkProcessor{
		processor: processor,
	}, nil
}

func (v *v6BulkProcessor) Start(ctx context.Context) error {
	return v.processor.Start(ctx)
}

func (v *v6BulkProcessor) Stop() error {
	return v.processor.Stop()
}

func (v *v6BulkProcessor) Close() error {
	return v.processor.Close()
}

func (v *v6BulkProcessor) Add(request *bulk.GenericBulkableAddRequest) {
	var req elastic.BulkableRequest
	switch request.RequestType {
	case bulk.BulkableDeleteRequest:
		req = elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Type(request.Type).
			Id(request.ID).
			VersionType(request.VersionType).
			Version(request.Version)
	case bulk.BulkableIndexRequest:
		req = elastic.NewBulkIndexRequest().
			Index(request.Index).
			Type(request.Type).
			Id(request.ID).
			VersionType(request.VersionType).
			Version(request.Version).
			Doc(request.Doc)
	case bulk.BulkableCreateRequest:
		//for bulk create request still calls the bulk index method
		//with providing operation type
		req = elastic.NewBulkIndexRequest().
			OpType("create").
			Index(request.Index).
			Type(request.Type).
			Id(request.ID).
			VersionType("internal").
			Doc(request.Doc)
	}
	v.processor.Add(req)
}

func (v *v6BulkProcessor) Flush() error {
	return v.processor.Flush()
}

func convertV6ErrorToGenericError(err error) *bulk.GenericError {
	if err == nil {
		return nil
	}
	status := bulk.UnknownStatusCode
	switch e := err.(type) {
	case *elastic.Error:
		status = e.Status
	}
	return &bulk.GenericError{
		Status:  status,
		Details: err,
	}
}

func fromV6toGenericBulkResponse(response *elastic.BulkResponse) *bulk.GenericBulkResponse {
	if response == nil {
		return &bulk.GenericBulkResponse{}
	}
	return &bulk.GenericBulkResponse{
		Took:   response.Took,
		Errors: response.Errors,
		Items:  fromV6ToGenericBulkResponseItemMaps(response.Items),
	}
}

func fromV6ToGenericBulkResponseItemMaps(items []map[string]*elastic.BulkResponseItem) []map[string]*bulk.GenericBulkResponseItem {
	var gitems []map[string]*bulk.GenericBulkResponseItem
	for _, it := range items {
		gitems = append(gitems, fromV6ToGenericBulkResponseItemMap(it))
	}
	return gitems
}

func fromV6ToGenericBulkResponseItemMap(m map[string]*elastic.BulkResponseItem) map[string]*bulk.GenericBulkResponseItem {
	if m == nil {
		return nil
	}
	gm := make(map[string]*bulk.GenericBulkResponseItem, len(m))
	for k, v := range m {
		gm[k] = fromV6ToGenericBulkResponseItem(v)
	}
	return gm
}

func fromV6ToGenericBulkResponseItem(v *elastic.BulkResponseItem) *bulk.GenericBulkResponseItem {
	return &bulk.GenericBulkResponseItem{
		Index:         v.Index,
		Type:          v.Type,
		ID:            v.Id,
		Version:       v.Version,
		Result:        v.Result,
		SeqNo:         v.SeqNo,
		PrimaryTerm:   v.PrimaryTerm,
		Status:        v.Status,
		ForcedRefresh: v.ForcedRefresh,
	}
}

func fromV6ToGenericBulkableRequests(requests []elastic.BulkableRequest) []bulk.GenericBulkableRequest {
	var v6Reqs []bulk.GenericBulkableRequest
	for _, req := range requests {
		v6Reqs = append(v6Reqs, req)
	}
	return v6Reqs
}
