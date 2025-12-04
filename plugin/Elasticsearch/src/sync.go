package src

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"net/http"

	"github.com/olivere/elastic/v7"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

// commitNormal commitNormal
func (conn *ElasticsearchConn) commitNormal(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType, err error) {
	reqs := make([]elastic.BulkableRequest, 0, len(list))
	var normalFun = func(v *pluginDriver.PluginDataType, reqs1 []elastic.BulkableRequest) {
		var reqs2 []elastic.BulkableRequest
		switch v.EventType {
		case "insert":
			reqs2, _ = conn.makeInsertRequest(v.Rows)
			break
		case "update":
			reqs2, _ = conn.makeUpdateRequest(v.Rows)

			break
		case "delete":
			reqs2, _ = conn.makeDeleteRequest(v.Rows)
			break
		default:
			break
		}
		reqs = append(reqs, reqs2...)
	}

	for i := 0; i <= n-1; i++ {
		v := list[i]
		normalFun(v, reqs)
	}

	for !conn.p.hadMapping[conn.p.EsIndexName] {
		conn.doCreateMapping()
	}
	if err = conn.sendBulkRequests(reqs); err != nil {
		logrus.Printf("do ES bulk err %v, close sync", err)
		return
	}
	return
}

// makeInsertRequest makeInsertRequest
func (conn *ElasticsearchConn) makeInsertRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for _, values := range rows {
		id, err := conn.getDocID(values)
		if err != nil {
			return nil, err
		}
		req := elastic.NewBulkUpdateRequest().
			Index(conn.p.EsIndexName).
			RetryOnConflict(conn.esServerInfo.RetryCount).
			Id(id).
			Doc(values).DocAsUpsert(true).
			Upsert(values)

		reqs = append(reqs, req)
	}
	return reqs, nil
}

// makeDeleteRequest makeDeleteRequest
func (conn *ElasticsearchConn) makeDeleteRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for _, values := range rows {
		id, err := conn.getDocID(values)
		if err != nil {
			return nil, err
		}
		req := elastic.NewBulkDeleteRequest().
			Index(conn.p.EsIndexName).
			Id(id)
		reqs = append(reqs, req)
	}
	return reqs, nil
}

// makeUpdateRequest makeUpdateRequest
func (conn *ElasticsearchConn) makeUpdateRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	if len(rows)%2 != 0 {
		return nil, fmt.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for i := 0; i < len(rows); i += 2 {
		afterID, err := conn.getDocID(rows[i+1])
		if err != nil {
			return nil, err
		}
		req := elastic.NewBulkUpdateRequest().
			Index(conn.p.EsIndexName).
			RetryOnConflict(conn.esServerInfo.RetryCount).
			Id(afterID).
			Doc(rows[i+1]).DocAsUpsert(true).
			Upsert(rows[i+1])
		reqs = append(reqs, req)
	}
	return reqs, nil
}

func (conn *ElasticsearchConn) getDocID(row map[string]interface{}) (id string, err error) {
	for _, key := range conn.p.primaryKeys {
		if _, ok := row[key]; ok {
			id = fmt.Sprint(row[key])
		} else {
			return "", fmt.Errorf("key:" + key + " no exsit")
		}
	}
	return
}

func (conn *ElasticsearchConn) sendBulkRequests(reqs []elastic.BulkableRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	bulkRequest := conn.client.Bulk()
	bulkRequest.Add(reqs...)
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		return err
	}

	for _, item := range bulkResponse.Items {
		for action, result := range item {
			if conn.isSuccessful(result, action) {
				// tags: [pipelineName, index, action(index/create/delete/update), status(200/400)].
				// indices created in 6.x only allow a single-type per index, so we don't need the type as a tag.
				var status int
				if result.Status == http.StatusBadRequest {
					logrus.Printf("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, action:%s ,status:%d ,details: %T.", result.Index, action, status, result.Error)
					status = http.StatusBadRequest
				} else {
					status = http.StatusOK
				}
			} else if result.Status == http.StatusTooManyRequests {
				// when the server returns 429, it must be that all requests have failed.
				return fmt.Errorf("[output_elasticsearch] The remote server returned an error: (429) Too Many Requests.")
			} else {
				return fmt.Errorf("[output_elasticsearch] Received an error from server, status: [%d], index: %s, action:%s ,status:%d ,details: %+v.", result.Status, result.Index, action, result.Status, result.Error)
			}
		}
	}
	return nil
}

func (conn *ElasticsearchConn) isSuccessful(result *elastic.BulkResponseItem, action string) bool {
	return (result.Status >= 200 && result.Status <= 299) ||
		(result.Status == http.StatusNotFound && action == "delete") || // delete but not found, just ignore it.
		(result.Status == http.StatusBadRequest && !conn.p.BifrostMustBeSuccess) // ignore index not found, parse error, etc.
}
