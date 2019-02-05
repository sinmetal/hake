package hake_test

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/hake"
)

func TestRow_MarshalJSON(t *testing.T) {

	cases := []struct {
		name string
		row  *spanner.Row
		want string
	}{
		{"null", row(t, nil), toJSON(t, nil)},
		{"empty", row(t, R{}), toJSON(t, R{})},
		{"single", row(t, R{"col1": 100}), toJSON(t, R{"col1": 100})},
		{"multiple", row(t, R{"col1": 100, "col2": 10.5}), toJSON(t, R{"col1": 100, "col2": 10.5})},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			got := toJSON(t, (*hake.Row)(tt.row))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}

func TestRows(t *testing.T) {

	cases := []struct {
		name string
		rows []*spanner.Row
		want string
	}{
		{"null", rows(t, nil), toJSON(t, nil)},
		{"empties", rows(t, []R{{}, {}}), toJSON(t, []R{{}, {}})},
		{"singles", rows(t, []R{{"col1": 100}, {"col2": 10.5}}), toJSON(t, []R{{"col1": 100}, {"col2": 10.5}})},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			got := toJSON(t, hake.Rows(tt.rows))
			if got != tt.want {
				t.Errorf("want %s but got %s", tt.want, got)
			}
		})
	}
}

func TestRow_ToStringArray(t *testing.T) {
	cases := []struct {
		name  string
		rows  []*spanner.Row
		wants [][]string
	}{
		{"null", rows(t, nil), [][]string{[]string{}}},
		{"empties", rows(t, []R{{}, {}}), [][]string{[]string{}, []string{}}},
		{"singles", rows(t, []R{{"col1": 100}, {"col2": 10.5}}), [][]string{[]string{"100"}, []string{"10.5"}}},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			rows := hake.Rows(tt.rows)
			for i, row := range rows {
				ll, err := row.ToStringArray()
				if err != nil {
					t.Errorf("unexpected err: %+v", err)
				}
				got := ll
				if !reflect.DeepEqual(got, tt.wants[i]) {
					t.Errorf("want %s but got %s", tt.wants[i], got)
				}
			}
		})
	}
}
