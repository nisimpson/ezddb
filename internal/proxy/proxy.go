package proxy

import (
	"encoding/gob"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var Default = proxy{
	GobEncoderDecoder: gobProxy{},
	MapMarshaler:      mapMarshaler{},
}

type proxy struct {
	GobEncoderDecoder
	MapMarshaler
}

type GobEncoder interface {
	Encode(*gob.Encoder, any) error
}

type GobDecoder interface {
	Decode(*gob.Decoder, any) error
}

type GobEncoderDecoder interface {
	GobEncoder
	GobDecoder
}

type gobProxy struct{}

func (p gobProxy) Encode(e *gob.Encoder, value any) error {
	return e.Encode(e)
}

func (p gobProxy) Decode(d *gob.Decoder, value any) error {
	return d.Decode(value)
}

type MapMarshaler interface {
	MarshalMap(in interface{}) (map[string]types.AttributeValue, error)
	UnmarshalMap(m map[string]types.AttributeValue, out interface{}) error
}

var (
	AttributeValueMapMarshalerProxy = mapMarshaler{}
)

type mapMarshaler struct{}

func (p mapMarshaler) MarshalMap(in any) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(in)
}

func (p mapMarshaler) UnmarshalMap(m map[string]types.AttributeValue, out any) error {
	return attributevalue.UnmarshalMap(m, out)
}
