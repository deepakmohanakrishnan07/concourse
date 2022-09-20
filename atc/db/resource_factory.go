package db

import (
	"database/sql"
	"github.com/olivere/elastic/v7"

	sq "github.com/Masterminds/squirrel"
	"github.com/concourse/concourse/atc/db/lock"
)

//counterfeiter:generate . ResourceFactory
type ResourceFactory interface {
	Resource(int) (Resource, bool, error)
	VisibleResources([]string) ([]Resource, error)
	AllResources() ([]Resource, error)
}

type resourceFactory struct {
	conn                Conn
	lockFactory         lock.LockFactory
	elasticsearchClient *elastic.Client
}

func NewResourceFactory(conn Conn, lockFactory lock.LockFactory, elasticsearchClient *elastic.Client) ResourceFactory {
	return &resourceFactory{
		conn:                conn,
		lockFactory:         lockFactory,
		elasticsearchClient: elasticsearchClient,
	}
}

func (r *resourceFactory) Resource(resourceID int) (Resource, bool, error) {
	resource := newEmptyResource(r.conn, r.lockFactory, r.elasticsearchClient)
	row := resourcesQuery.
		Where(sq.Eq{"r.id": resourceID}).
		RunWith(r.conn).
		QueryRow()

	err := scanResource(resource, row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}

	return resource, true, nil
}

func (r *resourceFactory) VisibleResources(teamNames []string) ([]Resource, error) {
	rows, err := resourcesQuery.
		Where(sq.Or{
			sq.Eq{"t.name": teamNames},
			sq.And{
				sq.NotEq{"t.name": teamNames},
				sq.Eq{"p.public": true},
			},
		}).
		OrderBy("r.id ASC").
		RunWith(r.conn).
		Query()
	if err != nil {
		return nil, err
	}

	return scanResources(rows, r.conn, r.lockFactory, r.elasticsearchClient)
}

func (r *resourceFactory) AllResources() ([]Resource, error) {
	rows, err := resourcesQuery.
		OrderBy("r.id ASC").
		RunWith(r.conn).
		Query()
	if err != nil {
		return nil, err
	}

	return scanResources(rows, r.conn, r.lockFactory, r.elasticsearchClient)
}

func scanResources(resourceRows *sql.Rows, conn Conn, lockFactory lock.LockFactory, elasticsearchClient *elastic.Client) ([]Resource, error) {
	var resources []Resource

	for resourceRows.Next() {
		resource := newEmptyResource(conn, lockFactory, elasticsearchClient)
		err := scanResource(resource, resourceRows)
		if err != nil {
			return nil, err
		}

		resources = append(resources, resource)
	}

	return resources, nil
}
