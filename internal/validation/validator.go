package validation

import (
	"bytes"
	"encoding/json"
	"eventprocessor/internal/model"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Validator struct{ schemas map[string]*jsonschema.Schema }

func New(fsys fs.FS) (*Validator, error) {
	v := &Validator{schemas: make(map[string]*jsonschema.Schema)}
	files, err := fs.Glob(fsys, "*.json")
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no schema files found in the provided filesystem")
	}

	for _, file := range files {
		f, err := fs.ReadFile(fsys, file)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema file %s: %w", file, err)
		}
		var doc any
		if err = json.Unmarshal(f, &doc); err != nil {
			return nil, fmt.Errorf("failed to unmarshal schema file %s: %w", file, err)
		}

		s := jsonschema.NewCompiler()
		if err = s.AddResource(file, doc); err != nil {
			return nil, fmt.Errorf("failed to add schema resource %s: %w", file, err)
		}

		schema, err := s.Compile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to compile schema %s: %w", file, err)
		}

		stem := strings.TrimSuffix(file, ".json")
		idx := strings.LastIndex(stem, ".")
		if idx < 0 {
			return nil, fmt.Errorf("invalid schema file name %s: expected format <event_type>.<version>.json", file)
		}
		eventType, schemaVersion := stem[:idx], stem[idx+1:]
		v.schemas[eventType+"\x00"+schemaVersion] = schema
	}

	return v, nil
}

func (v *Validator) Validate(raw []byte, src model.Source) model.Record {
	r := model.Record{Raw: append([]byte(nil), raw...), Source: src, Status: string(model.StatusInvalid), Errors: []string{}}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r.Envelope); err != nil {
		r.Errors = append(r.Errors, model.ErrInvalidEnvelope+err.Error())
		return r
	}

	var extra any
	err := dec.Decode(&extra)
	if err != nil && err != io.EOF {
		r.Errors = append(r.Errors, model.ErrInvalidTrailingJSON)
		return r
	}

	if err == nil {
		r.Errors = append(r.Errors, model.ErrMultipleJSONValues)
		return r
	}

	e := r.Envelope
	if e.ProducerID == "" {
		r.Errors = append(r.Errors, model.ErrMissingProducerID)
	}
	if e.ReceiverID == "" {
		r.Errors = append(r.Errors, model.ErrMissingReceiverID)
	}
	if e.EventID == "" {
		r.Errors = append(r.Errors, model.ErrMissingEventID)
	}
	if e.EventType == "" {
		r.Errors = append(r.Errors, model.ErrMissingEventType)
	}
	if e.SchemaVersion == "" {
		r.Errors = append(r.Errors, model.ErrMissingSchemaVersion)
	}
	if e.TenantID == "" {
		r.Errors = append(r.Errors, model.ErrMissingTenantID)
	}
	if e.OccuredAt.IsZero() || e.OccuredAt.After(time.Now().Add(24*time.Hour)) {
		r.Errors = append(r.Errors, model.ErrInvalidOccurredAt)
	}
	if e.Payload == nil {
		r.Errors = append(r.Errors, model.ErrMissingPayload)
	}

	key := e.EventType + "\x00" + e.SchemaVersion
	schema, ok := v.schemas[key]
	if !ok {
		r.Errors = append(r.Errors, model.ErrUnknownEventContract+e.EventType+"."+e.SchemaVersion)
	} else if len(e.Payload) > 0 {
		var payload any
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			r.Errors = append(r.Errors, model.ErrInvalidPayloadJSON+err.Error())
		} else if err := schema.Validate(payload); err != nil {
			r.Errors = append(r.Errors, model.ErrSchemaValidation+err.Error())
		}
	}

	if len(r.Errors) == 0 {
		r.Status = string(model.StatusReady)
	}

	return r
}
