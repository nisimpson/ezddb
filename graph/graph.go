package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	AttributePartitionKey           = "pk"
	AttributeSortKey                = "sk"
	AttributeEdgeType               = "edge_type"
	AttributeCollectionQuerySortKey = "gsi2_sk"
	AttributeReverseLookupSortKey   = "gsi1_sk"
)

type Data interface {
	DynamoGraphEdgeType() string
	DynamoGraphMarshal(createdAt, updatedAt time.Time) error
	DynamoGraphUnmarshal(createdAt, updatedAt time.Time) error
}

type Edge[T Data] struct {
	NodeID    string    `dynamodbav:"pk"`
	EdgeID    string    `dynamodbav:"sk"`
	EdgeType  string    `dynamodbav:"edge_type"`
	GSI1SK    string    `dynamodbav:"gsi1_sk"`
	GSI2SK    string    `dynamodbav:"gsi2_sk"`
	CreatedAt time.Time `dynamodbav:"created_at"`
	UpdatedAt time.Time `dynamodbav:"updated_at"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      T         `dynamodbav:"data"`
}

func (e Edge[T]) IsNodeEdge() bool {
	return e.NodeID == e.EdgeID
}

func tableRowKey(pk, sk string) ezddb.TableRow {
	return ezddb.TableRow{
		AttributePartitionKey: &types.AttributeValueMemberS{Value: pk},
		AttributeSortKey:      &types.AttributeValueMemberS{Value: sk},
	}
}

func (e Edge[T]) TableRowKey() ezddb.TableRow {
	return tableRowKey(e.NodeID, e.EdgeID)
}

func (e *Edge[T]) asPutProcedure(g Graph) ezddb.PutProcedure {
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		item, err := e.marshal(g.MarshalEdge)
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.TableName,
			Item:      item,
		}, nil
	}
}

func (e Edge[T]) asGetProcedure(g Graph) ezddb.GetProcedure {
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &g.TableName,
			Key:       e.TableRowKey(),
		}, nil
	}
}

func (e *Edge[T]) marshal(m EdgeMarshaler) (ezddb.TableRow, error) {
	err := e.Data.DynamoGraphMarshal(e.CreatedAt, e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return m(e)
}

func (e *Edge[T]) unmarshal(r ezddb.TableRow, u EdgeUnmarshaler) error {
	err := u(r, e)
	if err != nil {
		return err
	}
	return e.Data.DynamoGraphUnmarshal(e.CreatedAt, e.UpdatedAt)
}

type EdgeVisitor interface {
	VisitEdge(ctx context.Context, row ezddb.TableRow) error
}

type PageCursorProvider interface {
	PageCursor(ctx context.Context, key ezddb.TableRow) (string, error)
}

type StartKeyProvider interface {
	StartKey(ctx context.Context, token string) (ezddb.TableRow, error)
}

type ConditionFilterProvider interface {
	FilterCondition(ctx context.Context, base expression.ConditionBuilder) expression.ConditionBuilder
}

type EdgeMarshaler = func(any) (ezddb.TableRow, error)

type EdgeUnmarshaler = func(ezddb.TableRow, any) error

type DynamoClient interface {
	ezddb.Querier
	ezddb.Putter
	ezddb.Getter
	ezddb.BatchWriter
	ezddb.BatchGetter
}

type Options struct {
	DynamoClient
	PageCursorProvider
	StartKeyProvider
	TableName                string
	CollectionQueryIndexName string
	ReverseLookupIndexName   string
	ApplyDynamoDBOptions     func(*dynamodb.Options)
	BuildExpression          ExpressionBuilder
	MarshalEdge              EdgeMarshaler
	UnmarshalEdge            EdgeUnmarshaler
}

type OptionsFunc = func(*Options)

func (o *Options) apply(opts ...OptionsFunc) {
	for _, opt := range opts {
		opt(o)
	}
}

type Graph struct {
	Options
}

func New(tableName string, client DynamoClient, opts ...OptionsFunc) *Graph {
	options := Options{
		DynamoClient:             client,
		CollectionQueryIndexName: "collection-query-index",
		ReverseLookupIndexName:   "reverse-lookup-index",
		ApplyDynamoDBOptions:     dynamoDBOptionsNoOp,
		BuildExpression:          buildExpression,
		MarshalEdge:              attributevalue.MarshalMap,
		UnmarshalEdge:            attributevalue.UnmarshalMap,
	}
	options.apply(opts...)
	return &Graph{Options: options}
}

type QueryOutputVisitor interface {
	VisitQueryOutput(context.Context, *dynamodb.QueryOutput) error
}

type SearchEdgesInput struct {
	EdgeType          string
	PageCursor        PageCursor
	Limit             Limit
	CreatedAfterDate  time.Time
	CreatedBeforeDate time.Time
	Filter            ConditionFilterProvider
	OutputVisitor     QueryOutputVisitor
}

func SearchEdges(ctx context.Context, g Graph, input SearchEdgesInput, opts ...OptionsFunc) error {
	g.Options.apply(opts...)
	expressionBuilder := expression.NewBuilder().
		WithKeyCondition(edgesCreatedBetween(input.EdgeType, input.CreatedAfterDate, input.CreatedBeforeDate))

	condition := expression.ConditionBuilder{}
	if input.Filter != nil {
		condition = input.Filter.FilterCondition(ctx, condition)
	}

	if condition.IsSet() {
		expressionBuilder = expressionBuilder.WithFilter(condition)
	}

	expr, err := g.BuildExpression(expressionBuilder)
	if err != nil {
		return fmt.Errorf("build expression failed: %w", err)
	}

	startKey, err := input.PageCursor.StartKey(ctx, g)
	if err != nil {
		return fmt.Errorf("start key failed: %w", err)
	}

	output, err := g.DynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &g.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ExclusiveStartKey:         startKey,
		Limit:                     input.Limit.Value(),
	}, g.ApplyDynamoDBOptions)

	return input.OutputVisitor.VisitQueryOutput(ctx, output)
}

type TraverseEdgesInput struct {
	StartNodeID        string
	TraverseEdgePrefix string
	ReverseLookup      bool
	Limit              Limit
	PageCursor         PageCursor
	Filter             ConditionFilterProvider
	OutputVisitor      QueryOutputVisitor
}

func TraverseEdges(ctx context.Context, g Graph, input TraverseEdgesInput, opts ...OptionsFunc) error {
	g.Options.apply(opts...)

	expressionBuilder := expression.NewBuilder().
		WithKeyCondition(nodeEdgesWithPrefix(input.StartNodeID, input.TraverseEdgePrefix))

	if input.ReverseLookup {
		expressionBuilder = expressionBuilder.WithKeyCondition(
			reverseNodeEdgesWithPrefix(input.StartNodeID, input.TraverseEdgePrefix))
	}

	condition := expression.ConditionBuilder{}
	if input.Filter != nil {
		condition = input.Filter.FilterCondition(ctx, condition)
	}

	if condition.IsSet() {
		expressionBuilder = expressionBuilder.WithFilter(condition)
	}

	expr, err := g.BuildExpression(expressionBuilder)
	if err != nil {
		return fmt.Errorf("build expression failed: %w", err)
	}

	startKey, err := input.PageCursor.StartKey(ctx, g)
	if err != nil {
		return fmt.Errorf("start key failed: %w", err)
	}

	output, err := g.DynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &g.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ExclusiveStartKey:         startKey,
		Limit:                     input.Limit.Value(),
	}, g.ApplyDynamoDBOptions)

	return input.OutputVisitor.VisitQueryOutput(ctx, output)
}

type Putable interface {
	asPutProcedure(Graph) ezddb.PutProcedure
}

type PutEdgesInput struct {
	Edges []Putable
}

func PutEdges(ctx context.Context, g Graph, input PutEdgesInput, opts ...OptionsFunc) error {
	batcher := ezddb.NewBatchWriteProcedure()
	for _, edge := range input.Edges {
		batcher.Modify(edge.asPutProcedure(g))
	}
	_, err := batcher.Execute(ctx, g, g.ApplyDynamoDBOptions)
	return err
}

type GetEdgesInput struct {
	EdgeIDs []string
	Visitor EdgeVisitor
}

func GetEdges(ctx context.Context, g Graph, input GetEdgesInput, opts ...OptionsFunc) error {
	procedure := ezddb.NewBatchGetProcedure()
	for _, edgeID := range input.EdgeIDs {
		procedure = procedure.Modify(
			ezddb.GetProcedure(func(ctx context.Context) (*dynamodb.GetItemInput, error) {

			}))
	}
	out, err := procedure.Execute(ctx, g, g.ApplyDynamoDBOptions)
	if err != nil {
		return fmt.Errorf("get edges failed: %w", err)
	}
	responses := out.Responses[g.TableName]
	for _, response := range responses {
		if err := input.Visitor.VisitEdge(ctx, response); err != nil {
			return fmt.Errorf("edge visitor failed: %w", err)
		}
	}
	return nil
}

type ExpressionBuilder = func(expression.Builder) (expression.Expression, error)

func buildExpression(builder expression.Builder) (expression.Expression, error) {
	return builder.Build()
}

type PageCursor string

func (p PageCursor) StartKey(ctx context.Context, provider StartKeyProvider) (ezddb.TableRow, error) {
	token := string(p)
	if token == "" {
		return nil, nil
	}
	return provider.StartKey(ctx, token)
}

type Limit int

func (l Limit) Value() *int32 {
	if l > 0 {
		return aws.Int32(int32(l))
	}
	return nil
}

type StartKey ezddb.TableRow

func (s StartKey) PageCursor(ctx context.Context, provider PageCursorProvider) (string, error) {
	if s == nil {
		return "", nil
	}
	return provider.PageCursor(ctx, s)
}

func edgesWithPrefix(primary, sort, nodeid, edgeprefix string) expression.KeyConditionBuilder {
	condition := expression.Key(primary).Equal(expression.Value(nodeid))
	if edgeprefix != "" {
		condition = condition.And(expression.KeyBeginsWith(
			expression.Key(sort),
			edgeprefix,
		))
	}
	return condition
}

func nodeEdgesWithPrefix(nodeid, edgeprefix string) expression.KeyConditionBuilder {
	return edgesWithPrefix(AttributePartitionKey, AttributeSortKey, nodeid, edgeprefix)
}

func reverseNodeEdgesWithPrefix(nodeid, edgeprefix string) expression.KeyConditionBuilder {
	return edgesWithPrefix(AttributeSortKey, AttributeReverseLookupSortKey, nodeid, edgeprefix)
}

func edgesCreatedBetween(edgetype string, start, end time.Time) expression.KeyConditionBuilder {
	condition := expression.Key(AttributePartitionKey).Equal(expression.Value(edgetype))
	if start.IsZero() || end.IsZero() {
		return condition
	}
	condition = expression.KeyBetween(
		expression.Key(AttributeCollectionQuerySortKey),
		expression.Value(start.Format(time.RFC3339)),
		expression.Value(start.Format(time.RFC3339)),
	)

	return condition
}

func dynamoDBOptionsNoOp(*dynamodb.Options) {}
