package ddbstreams

import (
	"encoding/json"
	"reflect"
	"testing"
)

func dec(t *testing.T, s string) any {
	t.Helper()
	v, err := decodeAttr(json.RawMessage(s))
	if err != nil {
		t.Fatalf("decodeAttr(%s): %v", s, err)
	}
	return v
}

func TestDecodeScalars(t *testing.T) {
	if got := dec(t, `{"S":"hi"}`); got != "hi" {
		t.Errorf("S = %v", got)
	}
	if got := dec(t, `{"N":"42"}`); got != "42" { // numbers stay canonical strings
		t.Errorf("N = %v", got)
	}
	if got := dec(t, `{"Bool":true}`); got != true {
		t.Errorf("Bool = %v", got)
	}
	if got := dec(t, `"Null"`); got != nil {
		t.Errorf("Null = %v", got)
	}
}

func TestDecodeBinaryAndSets(t *testing.T) {
	if got := dec(t, `{"B":[0,1,255]}`); !reflect.DeepEqual(got, []byte{0, 1, 255}) {
		t.Errorf("B = %v", got)
	}
	if got := dec(t, `{"Ss":["a","b"]}`); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("Ss = %v", got)
	}
	if got := dec(t, `{"Ns":["1","2.5"]}`); !reflect.DeepEqual(got, []string{"1", "2.5"}) {
		t.Errorf("Ns = %v", got)
	}
	if got := dec(t, `{"Bs":[[1,2],[3]]}`); !reflect.DeepEqual(got, [][]byte{{1, 2}, {3}}) {
		t.Errorf("Bs = %v", got)
	}
}

func TestDecodeNested(t *testing.T) {
	got := dec(t, `{"M":{"x":{"N":"1"},"y":{"S":"z"},"n":"Null"}}`)
	want := map[string]any{"x": "1", "y": "z", "n": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("M = %v, want %v", got, want)
	}
	gotL := dec(t, `{"L":[{"S":"a"},"Null",{"Bool":false}]}`)
	wantL := []any{"a", nil, false}
	if !reflect.DeepEqual(gotL, wantL) {
		t.Errorf("L = %v, want %v", gotL, wantL)
	}
}

func TestRecordFromWire(t *testing.T) {
	line := `{"event_name":"MODIFY","sequence_number":"100","stream_view_type":"NEW_AND_OLD_IMAGES",` +
		`"keys":{"pk":{"S":"k1"}},"new_image":{"pk":{"S":"k1"},"active":{"Bool":true}},"old_image":null}`
	var w wireRecord
	if err := json.Unmarshal([]byte(line), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	r, err := recordFromWire("shardId-1", w, RecordFormatNative)
	if err != nil {
		t.Fatalf("recordFromWire: %v", err)
	}
	if r.ShardID != "shardId-1" || r.EventName != "MODIFY" || r.SequenceNumber != "100" {
		t.Errorf("scalars: %+v", r)
	}
	if !reflect.DeepEqual(r.Keys, map[string]any{"pk": "k1"}) {
		t.Errorf("keys = %v", r.Keys)
	}
	if !reflect.DeepEqual(r.NewImage, map[string]any{"pk": "k1", "active": true}) {
		t.Errorf("new_image = %v", r.NewImage)
	}
	if r.OldImage != nil {
		t.Errorf("old_image = %v, want nil", r.OldImage)
	}
}

func TestRecordFromWireDDBJSON(t *testing.T) {
	line := `{"keys":{"pk":{"S":"k1"},"sk":{"N":"42"}},` +
		`"new_image":{"active":{"Bool":true},"note":"Null","blob":{"B":[1,2,3]},` +
		`"tags":{"Ss":["a","b"]},"nested":{"M":{"inner":{"N":"7"}}},"list":{"L":[{"S":"x"},"Null"]}}}`
	var w wireRecord
	if err := json.Unmarshal([]byte(line), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	r, err := recordFromWire("shardId-1", w, RecordFormatDDBJSON)
	if err != nil {
		t.Fatalf("recordFromWire: %v", err)
	}
	// Canonical DynamoDB JSON shape (consumable by the AWS SDK).
	wantKeys := map[string]any{"pk": map[string]any{"S": "k1"}, "sk": map[string]any{"N": "42"}}
	if !reflect.DeepEqual(r.Keys, wantKeys) {
		t.Errorf("keys = %v", r.Keys)
	}
	ni := r.NewImage
	if !reflect.DeepEqual(ni["active"], map[string]any{"BOOL": true}) {
		t.Errorf("active = %v", ni["active"])
	}
	if !reflect.DeepEqual(ni["note"], map[string]any{"NULL": true}) {
		t.Errorf("note = %v", ni["note"])
	}
	if !reflect.DeepEqual(ni["blob"], map[string]any{"B": "AQID"}) {
		t.Errorf("blob = %v", ni["blob"])
	}
	if !reflect.DeepEqual(ni["tags"], map[string]any{"SS": []string{"a", "b"}}) {
		t.Errorf("tags = %v", ni["tags"])
	}
	if !reflect.DeepEqual(ni["nested"], map[string]any{"M": map[string]any{"inner": map[string]any{"N": "7"}}}) {
		t.Errorf("nested = %v", ni["nested"])
	}
	if !reflect.DeepEqual(ni["list"], map[string]any{"L": []any{map[string]any{"S": "x"}, map[string]any{"NULL": true}}}) {
		t.Errorf("list = %v", ni["list"])
	}
}
