package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/event"

	es "github.com/elastic/go-elasticsearch/v7"
)

var ErrEndOfBuildEventStream = errors.New("end of build event stream")
var ErrBuildEventStreamClosed = errors.New("build event stream closed")

//go:generate counterfeiter . EventSource

type EventSource interface {
	Next() (event.Envelope, error)
	Close() error
}

func newBuildEventSource(
	buildID int,
	table string,
	conn Conn,
	notifier Notifier,
	from uint,
) *buildEventSource {
	wg := new(sync.WaitGroup)

	source := &buildEventSource{
		buildID: buildID,
		table:   table,

		conn: conn,

		notifier: notifier,

		events: make(chan event.Envelope, 2000),
		stop:   make(chan struct{}),
		wg:     wg,
	}

	wg.Add(1)
	go source.collectEvents(from, elasticsearch)

	return source
}

type buildEventSource struct {
	buildID int
	table   string

	conn     Conn
	notifier Notifier

	events chan event.Envelope
	stop   chan struct{}
	err    error
	wg     *sync.WaitGroup
}

func (source *buildEventSource) Next() (event.Envelope, error) {
	e, ok := <-source.events
	if !ok {
		return event.Envelope{}, source.err
	}

	return e, nil
}

func (source *buildEventSource) Close() error {
	select {
	case <-source.stop:
		return nil
	default:
		close(source.stop)
	}

	source.wg.Wait()

	return source.notifier.Close()
}

func (source *buildEventSource) collectEvents(cursor uint, es *es.Client) {
	defer source.wg.Done()

	var batchSize = cap(source.events)

	for {
		select {
		case <-source.stop:
			source.err = ErrBuildEventStreamClosed
			close(source.events)
			return
		default:
		}

		completed := false

		tx, err := source.conn.Begin()
		if err != nil {
			return
		}

		defer Rollback(tx)

		err = tx.QueryRow(`
			SELECT builds.completed
			FROM builds
			WHERE builds.id = $1
		`, source.buildID).Scan(&completed)
		if err != nil {
			source.err = err
			close(source.events)
			return
		}

		err = tx.Commit()
		if err != nil {
			close(source.events)
			return
		}

		// Build the request body.
		var buf bytes.Buffer
		sortFields := []string{"event_id", "payload.time"}
		sourceFields := []string{"type", "version", "payload"}
		sort := make([]map[string]string, len(sortFields))
		for i, v := range sortFields {
			m := map[string]string{
				v: "asc",
			}
			sort[i] = m
		}

		//search query
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"build_id": source.buildID,
				},
			},
			"from":    cursor,
			"size":    batchSize,
			"sort":    sort,
			"_source": sourceFields,
		}
		if err := json.NewEncoder(&buf).Encode(query); err != nil {
			logger.Error("Error encoding the build events search query", err)
			close(source.events)
			return
		}

		// Perform the search request.
		res, err := es.Search(
			es.Search.WithContext(context.Background()),
			es.Search.WithIndex(source.table),
			es.Search.WithBody(&buf),
			es.Search.WithTrackTotalHits(true),
		)

		if err != nil {
			logger.Error("Error searching build events", err, lager.Data{
				"query":    query,
				"build_id": source.buildID,
			})
			close(source.events)
			return
		}

		defer res.Body.Close()

		if res.IsError() {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				logger.Error("Error parsing the search failure response body", err)
				source.err = err
			} else {
				ne := errors.New("Error searching build events res=" + res.Status())
				logger.Error("", ne, lager.Data{
					"status": res.Status(),
					"cause":  e,
				})
				source.err = ne
			}
			close(source.events)
			return
		}

		var result map[string]interface{}

		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			logger.Error("Error parsing the search result response body", err)
			source.err = err
			close(source.events)
			return
		}

		docs := result["hits"].(map[string]interface{})["hits"].([]interface{})

		rowsReturned := 0
		for _, doc := range docs {
			// log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
			rowsReturned++

			cursor++

			s := doc.(map[string]interface{})["_source"]
			t := s.(map[string]interface{})["type"].(string)
			v := s.(map[string]interface{})["version"].(string)
			p := s.(map[string]interface{})["payload"]

			var payloadBuf bytes.Buffer
			if err := json.NewEncoder(&payloadBuf).Encode(p); err != nil {
				logger.Error("Error encoding the event payload", err)
				close(source.events)
				return
			}

			data := json.RawMessage(payloadBuf.String())
			ev := event.Envelope{
				Data:    &data,
				Event:   atc.EventType(t),
				Version: atc.EventVersion(v),
			}

			select {
			case source.events <- ev:
			case <-source.stop:
				// _ = rows.Close()
				source.err = ErrBuildEventStreamClosed
				close(source.events)
				return
			}
		}

		if rowsReturned == batchSize {
			// still more events
			continue
		}

		if completed {
			source.err = ErrEndOfBuildEventStream
			close(source.events)
			return
		}

		select {
		case <-source.notifier.Notify():
		case <-source.stop:
			source.err = ErrBuildEventStreamClosed
			close(source.events)
			return
		}
	}
}
