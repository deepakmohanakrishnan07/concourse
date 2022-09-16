package atc

import (
	"encoding/json"
	"strings"
)

// Event represents an event emitted by a build. They are interpreted as a
// stream to render the build's output.
type Event interface {
	EventType() EventType
	Version() EventVersion
}

// EventType is a classification of an event payload, associated to a struct to
// parse it into.
type EventType string

// EventVersion is a MAJOR.MINOR version corresponding to an event type.
//
// Minor bumps must be backwards-compatible, meaning older clients can still
// unmarshal them into their old type and still handle the event.
//
// An example of a minor bump would be an additive change, i.e. a new field.
//
// Major bumps are backwards-incompatible and must be parsed and handled
// differently. An example of a major bump would be the changing or removal of a
// field.
type EventVersion string

// IsCompatibleWith checks whether the versions have the same major version.
func (version EventVersion) IsCompatibleWith(other EventVersion) bool {
	segs := strings.SplitN(string(other), ".", 2)
	return strings.HasPrefix(string(version), segs[0]+".")
}

type EventDoc struct {
	EventID      int              `json:"event_id"`
	BuildID      int              `json:"build_id"`
	BuildName    string           `json:"build_name"`
	JobID        int              `json:"job_id"`
	JobName      string           `json:"job_name"`
	PipelineID   int              `json:"pipeline_id"`
	PipelineName string           `json:"pipeline_name"`
	TeamID       int              `json:"team_id"`
	TeamName     string           `json:"team_name"`
	EventType    EventType        `json:"event"`
	Version      EventVersion     `json:"version"`
	Data         *json.RawMessage `json:"data"`
}
