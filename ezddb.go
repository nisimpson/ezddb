package ezddb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableRow = map[string]types.AttributeValue

type RowMarshaler = func(any) (TableRow, error)

type RowUnmarshaler = func(TableRow, any) error
