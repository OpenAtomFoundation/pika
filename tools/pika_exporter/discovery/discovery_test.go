package discovery

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewCodisDiscovery(t *testing.T) {
	jsonFile := "mockCodisTopom.json"
	jsonData, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		t.Fatalf("failed to read test data: %v", err)
	}

	var result CodisTopomInfo
	err = json.Unmarshal(jsonData, &result)
	if err != nil {
		t.Fatalf("failed to parse test data: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer ts.Close()

	url := ts.URL
	password := "password1,password2"
	alias := ""

	discovery, err := NewCodisDiscovery(url, password, alias)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedAddrs := []string{
		"1.1.1.6:1100",
		"1.1.1.7:1100",
	}
	expectedPasswords := []string{
		"password1",
		"password2",
	}
	expectedAddrsForProxy := []string{
		"1.2.3.4:1234",
		"1.2.3.4:4321",
	}

	if len(discovery.instances) != len(expectedAddrs) {
		t.Errorf("expected %d instances but got %d", len(expectedAddrs), len(discovery.instances))
	}

	if len(discovery.instanceProxy) != len(expectedAddrsForProxy) {
		t.Errorf("expected %d instances but got %d", len(expectedAddrs), len(discovery.instances))
	}

	for i := range expectedAddrs {
		if discovery.instances[i].Addr != expectedAddrs[i] {
			t.Errorf("instance %d address: expected %s but got %s", i, expectedAddrs[i], discovery.instances[i].Addr)
		}
		if discovery.instances[i].Password != expectedPasswords[i] {
			t.Errorf("instance %d password: expected %s but got %s", i, expectedPasswords[i], discovery.instances[i].Password)
		}
	}

	for i := range expectedAddrsForProxy {
		if expectedAddrsForProxy[i] != discovery.instanceProxy[i].Addr {
			t.Errorf("instance %d address: expected %s but got %s", i, expectedAddrs[i], discovery.instances[i].Addr)
		}
	}
}
