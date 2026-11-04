package routing

import (
	"encoding/json"
	"fmt"
	"io/fs"
)

type Rule struct {
	TenantID    string `json:"tenant_id"`
	EventType   string `json:"event_type"`
	ReceiverID  string `json:"receiver_id"`
	Destination string `json:"destination"`
}

type Router struct{ routes map[string]string }

func key(tenant, event, recipient string) string { return tenant + "\x00" + event + "\x00" + recipient }

func New(fsys fs.FS, filename string) (*Router, error) {
	file, err := fs.ReadFile(fsys, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read routing file %s: %w", file, err)
	}

	var config struct {
		Routes []Rule `json:"routes"`
	}
	if err = json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal routing file %s: %w", file, err)
	}

	r := &Router{routes: make(map[string]string)}
	for _, rule := range config.Routes {
		if rule.TenantID == "" || rule.EventType == "" || rule.ReceiverID == "" || rule.Destination == "" {
			return nil, fmt.Errorf("invalid routing rule: all fields must be non-empty")
		}

		k := key(rule.TenantID, rule.EventType, rule.ReceiverID)
		if _, exists := r.routes[k]; exists {
			return nil, fmt.Errorf("duplicate routing rule for tenant %s, event %s, receiver %s", rule.TenantID, rule.EventType, rule.ReceiverID)
		}
		r.routes[k] = rule.Destination
	}
	return r, nil
}

func (r *Router) Resolve(tenant, event, recipient string) string {
	if r == nil {
		return ""
	}
	return r.routes[key(tenant, event, recipient)]
}
