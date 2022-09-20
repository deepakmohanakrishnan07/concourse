package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strconv"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/event"
)

var ErrEndOfBuildEventStream = errors.New("end of build event stream")
var ErrBuildEventStreamClosed = errors.New("build event stream closed")

const indexPatternName = "concourse-build-events"

//counterfeiter:generate . EventSource
type EventSource interface {
	Next() (event.Envelope, error)
	Close() error
}

type buildCompleteWatcherFunc func(Tx, int) (bool, error)

func newBuildEventSource(
	ctx context.Context,
	buildID int,
	table string,
	conn Conn,
	notifier Notifier,
	from uint,
	watcher buildCompleteWatcherFunc,
	elasticsearchClient *elastic.Client,
) *buildEventSource {
	wg := new(sync.WaitGroup)

	source := &buildEventSource{
		ctx:     ctx,
		buildID: buildID,
		table:   table,

		conn: conn,

		notifier: notifier,

		events: make(chan event.Envelope, 500),
		stop:   make(chan struct{}),
		wg:     wg,

		elasticsearchClient: elasticsearchClient,

		watcherFunc: watcher,
	}

	wg.Add(1)
	go source.collectEvents(from)

	return source
}

type buildEventSource struct {
	ctx     context.Context
	buildID int
	table   string

	conn     Conn
	notifier Notifier

	events chan event.Envelope
	stop   chan struct{}
	err    error
	wg     *sync.WaitGroup

	elasticsearchClient *elastic.Client

	watcherFunc buildCompleteWatcherFunc
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

func (source *buildEventSource) collectEvents(from uint) {
	defer source.wg.Done()

	batchSize := cap(source.events)
	// cursor points to the last emitted event, so subtract 1
	// (the first event is fetched using cursor == -1)
	cursor := int(from) - 1

	for {
		select {
		case <-source.stop:
			source.err = ErrBuildEventStreamClosed
			close(source.events)
			return
		default:
		}

		tx, err := source.conn.Begin()
		if err != nil {
			return
		}

		defer Rollback(tx)

		completed, err := source.watcherFunc(tx, source.buildID)
		if err != nil {
			source.err = err
			close(source.events)
			return
		}

		tableExists := false
		buildExists := false

		//err = sq.Select("1").
		//	Prefix("SELECT EXISTS (").
		//	From(source.table).
		//	Where(sq.Or{
		//		sq.Eq{"build_id": source.buildID},
		//		sq.Eq{"build_id_old": source.buildID},
		//	}).
		//	Suffix(")").
		//	RunWith(tx).
		//	QueryRow().
		//	Scan(&existsInPostgres)
		//
		//if err != nil {
		//	source.err = err
		//	close(source.events)
		//	return
		//}

		rowsReturned := 0

		if tableExists || buildExists {
			rows, err := psql.Select("event_id", "type", "version", "payload").
				From(source.table).
				Where(sq.Or{
					sq.Eq{"build_id": source.buildID},
					sq.Eq{"build_id_old": source.buildID},
				}).
				Where(sq.Gt{"event_id": cursor}).
				OrderBy("event_id ASC").
				Limit(uint64(batchSize)).
				RunWith(tx).
				Query()
			if err != nil {
				source.err = err
				close(source.events)
				return
			}

			for rows.Next() {
				rowsReturned++

				var t, v, p string
				err := rows.Scan(&cursor, &t, &v, &p)
				if err != nil {
					_ = rows.Close()

					source.err = err
					close(source.events)
					return
				}

				data := json.RawMessage(p)

				ev := event.Envelope{
					Data:    &data,
					Event:   atc.EventType(t),
					Version: atc.EventVersion(v),
					EventID: json.Number(strconv.Itoa(cursor)),
				}

				select {
				case source.events <- ev:
				case <-source.stop:
					_ = rows.Close()

					source.err = ErrBuildEventStreamClosed
					close(source.events)
					return
				}
			}
		} else {

			req := source.elasticsearchClient.Search(indexPatternName).
				Query(elastic.NewTermQuery("build_id", source.buildID)).
				Sort("event_id", true).
				Size(batchSize)

			if from != 0 {
				req = req.SearchAfter(from)
			}

			//revisit: to use request context explicitly to cancel the operation when the request is closed
			searchResult, err := req.Do(source.ctx)
			if err != nil {
				source.err = err
				close(source.events)
				return
			}

			rowsReturned = len(searchResult.Hits.Hits)
			if rowsReturned == 0 {
				source.err = ErrEndOfBuildEventStream
				close(source.events)
				return
			}

			for _, hit := range searchResult.Hits.Hits {
				var envelope event.Envelope
				if err = json.Unmarshal(hit.Source, &envelope); err != nil {
					source.err = err
					close(source.events)
					return
				}
				cursor, err = strconv.Atoi(string(envelope.EventID))
				if err != nil {
					source.err = err
					close(source.events)
					return
				}

				select {
				case source.events <- envelope:
				case <-source.stop:
					source.err = ErrBuildEventStreamClosed
					close(source.events)
					return
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			source.err = err
			close(source.events)
			return
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
		fmt.Printf("from: %d | completed: %v | rowsReturned: %d | batchSize: %d", from, completed, rowsReturned, batchSize)

		select {
		case <-source.notifier.Notify():
		case <-source.stop:
			source.err = ErrBuildEventStreamClosed
			close(source.events)
			return
		}
	}
}
