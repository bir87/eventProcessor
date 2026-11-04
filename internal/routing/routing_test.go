package routing

import (
	"os"
	"testing"
	
)

func TestResolve(t *testing.T) {
	r, err := New(os.DirFS("../../config"), "routes.json")
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	tests := []struct {
		tenant, event, recipient string
		expected                 string
	}{
		{"tenant-demo", "transaction.authorized", "banking-app", "transactions"},
		{"tenant-demo", "monitoring.alert", "operations", "alerts"},
		{"tenant-demo", "user.created", "customer-app", "users"},
		{"tenant-demo", "integration.completed", "integration-hub", "integrations"},
		{"tenant-demo", "transaction.authorized", "unknown", ""},
	}

	for _, test := range tests {
		result := r.Resolve(test.tenant, test.event, test.recipient)
		if result != test.expected {
			t.Errorf("Resolve(%q, %q, %q) = %q; want %q", test.tenant, test.event, test.recipient, result, test.expected)
		}
	}

}