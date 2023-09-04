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
}

type ItemKeyProvider interface {
	DynamoItemKey() ezddb.Item
}

func New(tableName string) Builder {
	return Builder{
		tableName:     tableName,
		marshalMap:    attributevalue.MarshalMap,
		unmarshalMap:  attributevalue.UnmarshalMap,
		unmarshalList: attributevalue.UnmarshalListOfMaps,
	}
}

func (b Builder) WithMapMarshaler(marshaler mapMarshaler) Builder {
	b.marshalMap = marshaler
	return b
}

func (b Builder) WithMapUnmarshaler(unmarshaler mapUnmarshaler) Builder {
	b.unmarshalMap = unmarshaler
	return b
}

func (b Builder) WithMapListUnmarshaler(unmarshaler mapListUnmarshaler) Builder {
	b.unmarshalList = unmarshaler
	return b
}

func (b Builder) WithStartKeyProvider(provider procedure.StartKeyProvider) Builder {
	b.startKeyProvider = provider
	return b
}

func (b Builder) WithStartTokenProvider(provider procedure.StartKeyTokenProvider) Builder {
	b.startTokenProvider = provider
	return b
}

func (b Builder) Put(data any) PutBuilder {
	return PutBuilder{builder: b, data: data}
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

func (b Builder) Query(attribute string, value any) QueryBuilder {
	return QueryBuilder{
		builder:          b,
		hashKeyCondition: expression.Key(attribute).Equal(expression.Value(value)),
	}
}

type mapMarshaler = func(item any) (ezddb.Item, error)

type mapUnmarshaler = func(item ezddb.Item, out any) error

type mapListUnmarshaler = func(items []ezddb.Item, out any) error
