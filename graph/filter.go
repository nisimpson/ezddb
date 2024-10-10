package graph

import (
	"time"

	"github.com/nisimpson/ezddb/filter"
)

var (
	FilterPK            = filter.AttributeOf(AttributePartitionKey)
	FilterSK            = filter.AttributeOf(AttributeSortKey)
	FilterCreatedAt     = filter.AttributeOf(AttributeCreatedAt)
	FilterUpdatedAt     = filter.AttributeOf(AttributeUpdatedAt)
	FilterExpires       = filter.AttributeOf(AttributeExpires)
	FilterItemType      = filter.AttributeOf(AttributeItemType)
	FilterCollection    = filter.AttributeOf(AttributeCollectionQuerySortKey)
	FilterReverseLookup = filter.AttributeOf(AttributeReverseLookupSortKey)
)

type nodefilter[T Node] struct{ node T }

func FilterOf[T Node](item T) nodefilter[T] { return nodefilter[T]{item} }

func (f nodefilter[T]) CreatedBetween(start, end time.Time) filter.Builder {
	return filter.TimestampBetween(FilterCreatedAt, start.UTC(), end.UTC())
}

func (f nodefilter[T]) UpdatedBetween(start, end time.Time) filter.Builder {
	return filter.TimestampBetween(FilterUpdatedAt, start.UTC(), end.UTC())
}

func (f nodefilter[T]) ExpiresAfter(ts time.Time) filter.Builder {
	return filter.ExpiresAfter(FilterExpires, ts.UTC())
}

func (f nodefilter[T]) IsType() filter.Builder {
	return filter.Equals(FilterItemType, f.node.DynamoItemType())
}

func (f nodefilter[T]) IsRef(relation string) filter.Builder {
	return filter.HasPrefix(FilterItemType, relation)
}

func (f nodefilter[T]) IncludeRefs(relation string, relations ...string) filter.Builder {
	expr := f.IsType()
	expr = expr.And(f.IsRef(relation))
	for _, rel := range relations {
		expr = expr.Or(f.IsRef(rel))
	}
	return expr
}

func (f nodefilter[T]) Attribute(key string) filter.Attribute {
	return filter.AttributeOf(AttributeData, key)
}
