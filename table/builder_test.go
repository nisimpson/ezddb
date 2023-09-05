package table

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/nisimpson/ezddb/procedure"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "creates new bulder",
			args: args{tableName: "my-test-table"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(tt.args.tableName)
			assert.NotNil(t, got)
			assert.EqualValues(t, tt.args.tableName, got.tableName)
		})
	}
}

func TestBuilder_WithMapMarshaler(t *testing.T) {
	type fields struct{}
	type args struct {
		marshaler mapMarshaler
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantNil bool
	}{
		{
			name:    "assigns marshaler",
			args:    args{marshaler: attributevalue.MarshalMap},
			wantNil: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{}
			got := b.WithMapMarshaler(tt.args.marshaler)
			assert.Equal(t, tt.wantNil, got.marshalMap == nil)
		})
	}
}

func TestBuilder_WithMapUnmarshaler(t *testing.T) {
	b := Builder{}
	got := b.WithMapUnmarshaler(attributevalue.UnmarshalMap)
	assert.NotNil(t, got.unmarshalMap)
}

func TestBuilder_WithMapListUnmarshaler(t *testing.T) {
	b := Builder{}
	got := b.WithMapListUnmarshaler(attributevalue.UnmarshalListOfMaps)
	assert.NotNil(t, got.unmarshalList)
}

func TestBuilder_WithStartKeyProvider(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		provider procedure.StartKeyProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Builder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.WithStartKeyProvider(tt.args.provider); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.WithStartKeyProvider() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_WithStartTokenProvider(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		provider procedure.StartKeyTokenProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Builder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.WithStartTokenProvider(tt.args.provider); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.WithStartTokenProvider() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Put(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		data any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   PutBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Put(tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Get(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		key ItemKeyProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   GetBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Update(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		key ItemKeyProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   UpdateBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Update(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Delete(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		key ItemKeyProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   DeleteBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Delete(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Query(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		attribute string
		value     any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   QueryBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Query(tt.args.attribute, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Query() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuilder_Check(t *testing.T) {
	type fields struct {
		tableName          string
		marshalMap         mapMarshaler
		unmarshalMap       mapUnmarshaler
		unmarshalList      mapListUnmarshaler
		startKeyProvider   procedure.StartKeyProvider
		startTokenProvider procedure.StartKeyTokenProvider
	}
	type args struct {
		key ItemKeyProvider
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ConditionCheckBuilder
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Builder{
				tableName:          tt.fields.tableName,
				marshalMap:         tt.fields.marshalMap,
				unmarshalMap:       tt.fields.unmarshalMap,
				unmarshalList:      tt.fields.unmarshalList,
				startKeyProvider:   tt.fields.startKeyProvider,
				startTokenProvider: tt.fields.startTokenProvider,
			}
			if got := b.Check(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Builder.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
