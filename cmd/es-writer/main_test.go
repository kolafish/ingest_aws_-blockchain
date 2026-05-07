package main

import (
	"reflect"
	"testing"
)

func TestParseURLs(t *testing.T) {
	got := parseURLs(" http://es-1:9200,https://es-2:9200, ,http://es-3:9200 ")
	want := []string{"http://es-1:9200", "https://es-2:9200", "http://es-3:9200"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseURLs() = %#v, want %#v", got, want)
	}
}
