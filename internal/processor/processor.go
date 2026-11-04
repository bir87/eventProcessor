package processor

import (
	"context"
	"eventprocessor/internal/model"
	"eventprocessor/internal/routing"
	"eventprocessor/internal/validation"
	"fmt"
)

type Store interface {
	Save(context.Context, model.Record) (bool, error)
}

type Processor struct {
	Router    *routing.Router
	Validator *validation.Validator
	Store     Store
}

func (p *Processor) Process(ctx context.Context, raw []byte, src model.Source) (model.Record, bool, error) {
	r := p.Validator.Validate(raw, src)
	if r.Status == string(model.StatusReady) {
		r.Route = p.Router.Resolve(r.Envelope.TenantID, r.Envelope.EventType, r.Envelope.ReceiverID)
		if r.Route == "" {
			r.Status = string(model.StatusInvalid)
			r.Errors = append(r.Errors, model.ErrNoAuthorizedRoute)
		}
	}
	quarantined, err := p.Store.Save(ctx, r)
	if err != nil {
		return r, false, fmt.Errorf("persist event: %w", err)
	}
	return r, quarantined, nil
}
