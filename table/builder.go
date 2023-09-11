package table

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/procedure"
)

var (
	emptyUpdateBuilder    = &expression.UpdateBuilder{}
	emptyConditionBuilder = &expression.ConditionBuilder{}
)

type Builder struct {
	tableName          string
	marshalMap         mapMarshaler
	unmarshalMap       mapUnmarshaler
	unmarshalList      mapListUnmarshaler
	startKeyProvider   procedure.StartKeyProvider
	startTokenProvider procedure.StartKeyTokenProvider
	buildExpression    expressionBuilder
}

type ItemKeyProvider interface {
	DynamoItemKey() ezddb.Item
}

func New(tableName string, options ...Option) Builder {
	builder := Builder{
		tableName:     tableName,
		marshalMap:    attributevalue.MarshalMap,
		unmarshalMap:  attributevalue.UnmarshalMap,
		unmarshalList: attributevalue.UnmarshalListOfMaps,
		buildExpression: func(builder expression.Builder) (expression.Expression, error) {
			return builder.Build()
		},
	}
	for _, apply := range options {
		apply(&builder)
	}
	return builder
}

type Option func(*Builder)

func WithMapMarshaler(marshaler mapMarshaler) Option {
	return func(b *Builder) {
		b.marshalMap = marshaler
	}
}

func WithMapUnmarshaler(unmarshaler mapUnmarshaler) Option {
	return func(b *Builder) {
		b.unmarshalMap = unmarshaler
	}
}

func WithMapListUnmarshaler(unmarshaler mapListUnmarshaler) Option {
	return func(b *Builder) {
		b.unmarshalList = unmarshaler
	}
}

func WithStartKeyProvider(provider procedure.StartKeyProvider) Option {
	return func(b *Builder) {
		b.startKeyProvider = provider
	}
}

func WithStartTokenProvider(provider procedure.StartKeyTokenProvider) Option {
	return func(b *Builder) {
		b.startTokenProvider = provider
	}
}

func WithExpressionBuilder(builder expressionBuilder) Option {
	return func(b *Builder) {
		b.buildExpression = builder
	}
}

func (b Builder) TableName() string {
	return b.tableName
}


func (b Builder) Get(key ItemKeyProvider) GetBuilder {
	return GetBuilder{
		builder: b,
		key:     key,
	}
}

func (b Builder) Update(key ItemKeyProvider) UpdateBuilder {
	return UpdateBuilder{
		builder:          b,
		conditionBuilder: emptyConditionBuilder,
		updateBuilder:    emptyUpdateBuilder,
	}
}

func (b Builder) Delete(key ItemKeyProvider) DeleteBuilder {
	return DeleteBuilder{builder: b, conditionBuilder: emptyConditionBuilder}
}


func (b Builder) Check(key ItemKeyProvider) ConditionCheckBuilder {
	return ConditionCheckBuilder{
		builder: b,
		key:     key,
	}
}

type mapMarshaler = func(item any) (ezddb.Item, error)

type mapUnmarshaler = func(item ezddb.Item, out any) error

type mapListUnmarshaler = func(items []ezddb.Item, out any) error

type expressionBuilder = func(builder expression.Builder) (expression.Expression, error)
