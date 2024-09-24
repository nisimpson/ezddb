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
	AttributeAssociation            = "association"
	AttributeCollectionQuerySortKey = "gsi2_sk"
	AttributeReverseLookupSortKey   = "gsi1_sk"
)

type Data interface {
	DynamoGraphNodeID() string
	DynamoGraphEdgeType() string
	DynamoGraphMarshal(createdAt, updatedAt time.Time) error
	DynamoGraphUnmarshal(createdAt, updatedAt time.Time) error
}

type EdgeIdentifier interface {
	EdgeID() (pk string, sk string)
}

type Edge[T Data] struct {
	SourceID    string    `dynamodbav:"pk"`
	TargetID    string    `dynamodbav:"sk"`
	Association string    `dynamodbav:"association"`
	GSI1SK      string    `dynamodbav:"gsi1_sk"`
	GSI2SK      string    `dynamodbav:"gsi2_sk"`
	CreatedAt   time.Time `dynamodbav:"created_at"`
	UpdatedAt   time.Time `dynamodbav:"updated_at"`
	Expires     time.Time `dynamodbav:"expires,unixtime"`
	Data        T         `dynamodbav:"data"`
}

func (e Edge[T]) IsNode() bool {
	return e.SourceID == e.TargetID
}

func (e Edge[T]) EdgeID() (string, string) {
	return e.SourceID, e.TargetID
}

func (e Edge[T]) TableRowKey() ezddb.TableRow {
	return tableRowKey(e.SourceID, e.TargetID)
}

func (e *Edge[T]) Marshal(m ezddb.RowMarshaler) (ezddb.TableRow, error) {
	err := e.Data.DynamoGraphMarshal(e.CreatedAt, e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return m(e)
}

func (e *Edge[T]) Unmarshal(r ezddb.TableRow, u ezddb.RowUnmarshaler) error {
	err := u(r, e)
	if err != nil {
		return err
	}
	return e.Data.DynamoGraphUnmarshal(e.CreatedAt, e.UpdatedAt)
}

type EdgeVisitor interface {
	VisitEdge(ctx context.Context, row ezddb.TableRow) error
}

type EdgeVisitorFunc func(ctx context.Context, row ezddb.TableRow) error

func (f EdgeVisitorFunc) VisitEdge(ctx context.Context, row ezddb.TableRow) error {
	return f(ctx, row)
}

type NodeVisitor struct {
	mux map[string]EdgeVisitor
}

func NewNodeVisitor() *NodeVisitor {
	return &NodeVisitor{
		mux: make(map[string]EdgeVisitor),
	}
}

func (v *NodeVisitor) AddAssociation(association string, visitor EdgeVisitor) {
	v.mux[association] = visitor
}

func (v NodeVisitor) VisitEdge(ctx context.Context, row ezddb.TableRow) error {
	association := row[AttributeAssociation].(*types.AttributeValueMemberS).Value
	visitor, ok := v.mux[association]
	if !ok {
		return fmt.Errorf("no visitor found for association %s", association)
	}
	return visitor.VisitEdge(ctx, row)
}

type ConditionFilterProvider interface {
	FilterCondition(ctx context.Context, base expression.ConditionBuilder) expression.ConditionBuilder
}

type DynamoClient interface {
	ezddb.Querier
	ezddb.Putter
	ezddb.Getter
	ezddb.BatchWriter
	ezddb.BatchGetter
}

type Options struct {
	DynamoClient
	ezddb.StartKeyTokenProvider
	ezddb.StartKeyProvider
	TableName                string
	CollectionQueryIndexName string
	ReverseLookupIndexName   string
	ApplyDynamoDBOptions     func(*dynamodb.Options)
	BuildExpression          ExpressionBuilder
	MarshalEdge              ezddb.RowMarshaler
	UnmarshalEdge            ezddb.RowUnmarshaler
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

type QueryNodesInput struct {
	Association       string
	PageCursor        string
	Limit             Limit
	CreatedAfterDate  time.Time
	CreatedBeforeDate time.Time
	Filter            ConditionFilterProvider
	SortAscending     bool
}

func QueryNodes(ctx context.Context, g Graph, input QueryNodesInput, opts ...OptionsFunc) (*dynamodb.QueryOutput, error) {
	g.Options.apply(opts...)
	expressionBuilder := expression.NewBuilder().
		WithKeyCondition(edgesCreatedBetween(input.Association, input.CreatedAfterDate, input.CreatedBeforeDate))

	condition := expression.ConditionBuilder{}
	if input.Filter != nil {
		condition = input.Filter.FilterCondition(ctx, condition)
	}

	if condition.IsSet() {
		expressionBuilder = expressionBuilder.WithFilter(condition)
	}

	expr, err := g.BuildExpression(expressionBuilder)
	if err != nil {
		return nil, fmt.Errorf("build expression failed: %w", err)
	}

	startKey, err := ezddb.GetStartKey(ctx, g, input.PageCursor)
	if err != nil {
		return nil, fmt.Errorf("start key failed: %w", err)
	}

	return g.DynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &g.TableName,
		IndexName:                 &g.CollectionQueryIndexName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ExclusiveStartKey:         startKey,
		Limit:                     input.Limit.Value(),
		ScanIndexForward:          aws.Bool(input.SortAscending),
	}, g.ApplyDynamoDBOptions)
}

func VisitNodes[T Data](ctx context.Context, g Graph, output *dynamodb.QueryOutput) ([]T, string, error) {
	nextToken, err := ezddb.GetStartKeyToken(ctx, g, output.LastEvaluatedKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get start key token: %w", err)
	}

	nodes := make([]T, 0, len(output.Items))
	for _, item := range output.Items {
		node := Edge[T]{}
		err := node.Unmarshal(item, g.UnmarshalEdge)
		if err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal node: %w", err)
		}
		nodes = append(nodes, node.Data)
	}

	return nodes, nextToken, nil
}

type QueryEdgesInput struct {
	StartNodeID   string
	EdgePrefix    string
	ReverseLookup bool
	Limit         Limit
	PageCursor    string
	Filter        ConditionFilterProvider
}

func QueryEdges(ctx context.Context, g Graph, input QueryEdgesInput, opts ...OptionsFunc) (*dynamodb.QueryOutput, error) {
	g.Options.apply(opts...)

	expressionBuilder := expression.NewBuilder().
		WithKeyCondition(nodeEdgesWithPrefix(input.StartNodeID, input.EdgePrefix))

	if input.ReverseLookup {
		expressionBuilder = expressionBuilder.WithKeyCondition(
			reverseNodeEdgesWithPrefix(input.StartNodeID, input.EdgePrefix))
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
		return nil, fmt.Errorf("build expression failed: %w", err)
	}

	startKey, err := ezddb.GetStartKey(ctx, g, input.PageCursor)
	if err != nil {
		return nil, fmt.Errorf("start key failed: %w", err)
	}

	return g.DynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &g.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ExclusiveStartKey:         startKey,
		Limit:                     input.Limit.Value(),
	}, g.ApplyDynamoDBOptions)
}

func VisitEdges(ctx context.Context, g Graph, visitor EdgeVisitor, output *dynamodb.QueryOutput) (string, error) {
	nextToken, err := ezddb.GetStartKeyToken(ctx, g, output.LastEvaluatedKey)
	if err != nil {
		return "", fmt.Errorf("failed to get start key token: %w", err)
	}
	for _, item := range output.Items {
		err := visitor.VisitEdge(ctx, item)
		if err != nil {
			return "", fmt.Errorf("visit edge failed: %w", err)
		}
	}
	return nextToken, nil
}

type PuttableEdge interface {
	Marshal(ezddb.RowMarshaler) (ezddb.TableRow, error)
}

func PutEdge(g Graph, edge PuttableEdge) ezddb.PutOperation {
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		item, err := edge.Marshal(g.MarshalEdge)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal edge: %w", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.TableName,
			Item:      item,
		}, nil
	}
}

func GetEdge(g Graph, id EdgeIdentifier) ezddb.GetOperation {
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		pk, sk := id.EdgeID()
		return &dynamodb.GetItemInput{
			TableName: &g.TableName,
			Key:       tableRowKey(pk, sk),
		}, nil
	}
}

type ExpressionBuilder = func(expression.Builder) (expression.Expression, error)

func buildExpression(builder expression.Builder) (expression.Expression, error) {
	return builder.Build()
}

type Limit int

func (l Limit) Value() *int32 {
	if l > 0 {
		return aws.Int32(int32(l))
	}
	return nil
}

func tableRowKey(pk, sk string) ezddb.TableRow {
	return ezddb.TableRow{
		AttributePartitionKey: &types.AttributeValueMemberS{Value: pk},
		AttributeSortKey:      &types.AttributeValueMemberS{Value: sk},
	}
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
