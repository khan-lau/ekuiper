package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var commonCases = []any{
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 1, "b": 2}},                                       // common a,b
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}},                         // common a,b,c
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, // nested data
	// &xsql.Tuple{Emitter: "test", Message: map[string]any{}},                                                     // empty tuple
	&xsql.WindowTuples{Content: []xsql.Row{&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 1, "b": 2}}, &xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
	&xsql.WindowTuples{Content: []xsql.Row{&xsql.Tuple{Emitter: "test", Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, &xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
	&xsql.WindowTuples{Content: []xsql.Row{}}, // empty data should be omitted if omitempty is true
}

func TestTransformRun(t *testing.T) {
	testcases := []struct {
		name      string
		sc        *SinkConf
		templates []string
		cases     []any
		expects   []any
	}{
		{
			name: "field transform",
			sc: &SinkConf{
				Omitempty:    true,
				Fields:       []string{"a", "b"},
				DataField:    "data",
				Format:       "json",
				DataTemplate: "",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				errors.New("fail to TransItem data map[a:1 b:2] for error fail to decode data <nil> for error unsupported type <nil>"),
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
				&xsql.Tuple{Message: map[string]any{"a": 5, "b": 6}},
				errors.New("fail to TransItem data map[a:1 b:2] for error fail to decode data <nil> for error unsupported type <nil>"),
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
				&xsql.Tuple{Message: map[string]any{"a": 5, "b": 6}},
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
			},
		},
		{
			name: "only fields without omit empty",
			sc: &SinkConf{
				Omitempty:    false,
				Fields:       []string{"a", "b"},
				Format:       "json",
				DataTemplate: "",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}},
				&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4}},
				&xsql.Tuple{Message: map[string]any{"a": nil, "b": nil}},

				&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}},
				&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4}},

				&xsql.Tuple{Message: map[string]any{"a": nil, "b": nil}},
				&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4}},

				// Even no omit empty, the empty data should be omitted due to sendSingle
				&xsql.Tuple{Message: map[string]any{}},
			},
		},
		{
			name: "allow empty",
			sc: &SinkConf{
				Omitempty:  false,
				Format:     "json",
				SendSingle: false,
			},
			cases: commonCases,
			expects: []any{
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 1, "b": 2}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}}}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 1, "b": 2}, {"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}}, &xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}, {"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, &xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
				&xsql.TransformedTupleList{Maps: []map[string]any{}, Content: []api.MessageTuple{}},
			},
		},
		{
			name: "only data field",
			sc: &SinkConf{
				Omitempty:  false,
				DataField:  "data",
				Format:     "json",
				SendSingle: false,
			},
			cases: commonCases,
			expects: []any{
				nil,
				nil,
				&xsql.Tuple{Message: map[string]any{"a": 5, "b": 6, "c": "world"}},
				nil,
				&xsql.Tuple{Message: map[string]any{"a": 5, "b": 6, "c": "world"}},
				nil,
			},
		},
		{
			name: "data template with text format single",
			sc: &SinkConf{
				Omitempty:    true,
				Format:       "custom",
				DataTemplate: "{\"ab\":{{index . 0 \"a\"}}}",
				SendSingle:   false,
			},
			cases: commonCases,
			expects: []any{
				&xsql.Tuple{Message: map[string]any{"ab": 1.0}},
				&xsql.Tuple{Message: map[string]any{"ab": 3.0}},
				errors.New("fail to decode data {\"ab\":<no value>} after applying dataTemplate for error invalid character '<' looking for beginning of value"),

				&xsql.Tuple{Message: map[string]any{"ab": 1.0}},

				errors.New("fail to decode data {\"ab\":<no value>} after applying dataTemplate for error invalid character '<' looking for beginning of value"),
			},
		},
		{
			name: "data template collection",
			sc: &SinkConf{
				Omitempty:    true,
				Fields:       []string{"ab"},
				Format:       "json",
				DataTemplate: "{\"ab\":{{.a}},\"bb\":{{.b}}}",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				&xsql.Tuple{Message: map[string]any{"ab": 1.0}},
				&xsql.Tuple{Message: map[string]any{"ab": 3.0}},
				errors.New("fail to TransItem data map[data:map[a:5 b:6 c:world]] for error fail to decode data {\"ab\":<no value>,\"bb\":<no value>} for error invalid character '<' looking for beginning of value"),

				&xsql.Tuple{Message: map[string]any{"ab": 1.0}},
				&xsql.Tuple{Message: map[string]any{"ab": 3.0}},

				errors.New("fail to TransItem data map[data:map[a:5 b:6 c:world]] for error fail to decode data {\"ab\":<no value>,\"bb\":<no value>} for error invalid character '<' looking for beginning of value"),
				&xsql.Tuple{Message: map[string]any{"ab": 3.0}},
			},
		},
		{
			name: "props of single",
			sc: &SinkConf{
				Omitempty:  true,
				Format:     "json",
				SendSingle: true,
				SchemaId:   "schema_{{.a}}",
				Delimiter:  "{{.b}}_comma",
			},
			templates: []string{"schema_{{.a}}", "{{.b}}_comma"},
			cases:     commonCases,
			expects: []any{
				&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}, Props: map[string]string{"schema_{{.a}}": "schema_1", "{{.b}}_comma": "2_comma"}},
				&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}, Props: map[string]string{"schema_{{.a}}": "schema_3", "{{.b}}_comma": "4_comma"}},
				&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}, Props: map[string]string{"schema_{{.a}}": "schema_<no value>", "{{.b}}_comma": "<no value>_comma"}},

				&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}, Props: map[string]string{"schema_{{.a}}": "schema_1", "{{.b}}_comma": "2_comma"}},
				&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}, Props: map[string]string{"schema_{{.a}}": "schema_3", "{{.b}}_comma": "4_comma"}},

				&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}, Props: map[string]string{"schema_{{.a}}": "schema_<no value>", "{{.b}}_comma": "<no value>_comma"}},
			},
		},
		{
			name: "props of list",
			sc: &SinkConf{
				Omitempty:  false,
				Format:     "json",
				SendSingle: false,
				SchemaId:   "t_{{index . 0 \"a\"}}_t",
			},
			templates: []string{"t_{{index . 0 \"a\"}}_t"},
			cases:     commonCases,
			expects: []any{
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 1, "b": 2}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}}}, Props: map[string]string{"t_{{index . 0 \"a\"}}_t": "t_1_t"}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}, Props: map[string]string{"t_{{index . 0 \"a\"}}_t": "t_3_t"}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}}, Props: map[string]string{"t_{{index . 0 \"a\"}}_t": "t_<no value>_t"}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"a": 1, "b": 2}, {"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"a": 1, "b": 2}}, &xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}, Props: map[string]string{"t_{{index . 0 \"a\"}}_t": "t_1_t"}},
				&xsql.TransformedTupleList{Maps: []map[string]any{{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}, {"a": 3, "b": 4, "c": "hello"}}, Content: []api.MessageTuple{&xsql.Tuple{Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, &xsql.Tuple{Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}, Props: map[string]string{"t_{{index . 0 \"a\"}}_t": "t_<no value>_t"}},
				errors.New("fail to calculate props t_{{index . 0 \"a\"}}_t through data [] with dataTemplate for error template: sink:1:4: executing \"sink\" at <index . 0 \"a\">: error calling index: reflect: slice index out of range"),
			},
		},
		{
			name: "props of data template",
			sc: &SinkConf{
				Format:       "json",
				DataTemplate: "{\"ab\":{{.a}},\"bb\":{{.b}}}",
				SendSingle:   true,
				SchemaId:     "{{.a}}",
			},
			templates: []string{"{{.a}}"},
			cases:     commonCases,
			expects: []any{
				&xsql.RawTuple{Rawdata: []byte(`{"ab":1,"bb":2}`), Props: map[string]string{"{{.a}}": "1"}},
				&xsql.RawTuple{Rawdata: []byte(`{"ab":3,"bb":4}`), Props: map[string]string{"{{.a}}": "3"}},
				&xsql.RawTuple{Rawdata: []byte(`{"ab":<no value>,"bb":<no value>}`), Props: map[string]string{"{{.a}}": "<no value>"}},

				&xsql.RawTuple{Rawdata: []byte(`{"ab":1,"bb":2}`), Props: map[string]string{"{{.a}}": "1"}},
				&xsql.RawTuple{Rawdata: []byte(`{"ab":3,"bb":4}`), Props: map[string]string{"{{.a}}": "3"}},
			},
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			timex.Set(0)
			op, err := NewTransformOp("test", &def.RuleOption{BufferLength: 10, SendError: true}, tt.sc, tt.templates)
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "transform_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			for i, c := range tt.cases {
				op.input <- c
				if i < len(tt.expects) {
					r := <-out
					assert.Equal(t, tt.expects[i], r, "case %d", i)
				}
			}
		})
	}
}