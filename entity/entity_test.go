package entity_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb/entity"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

type Customer struct {
	ID     string
	Name   string
	Orders []*Order `dynamodbav:"-"`
}

func (c Customer) DynamoID() string              { return c.ID }
func (c Customer) DynamoItemType() string        { return "customer" }
func (c Customer) DynamoRelationships() []string { return []string{"orders"} }

func (c Customer) DynamoGetRelationship(name string) []entity.Data {
	if name == "order" {
		entities := make([]entity.Data, 0, len(c.Orders))
		for _, order := range c.Orders {
			entities = append(entities, order)
		}
		return entities
	}
	return nil
}

func (c Customer) DynamoIsReverseRelationship(name string) bool {
	// customer -> order relationships are stored on the order partition.
	return name == "orders"
}

func (c Customer) DynamoGetRelationshipSortKey(name string) string {
	if name == "orders" {
		return "customer/orders"
	}
	return ""
}

func (c *Customer) UnmarshalRelationship(name string, startID, endID string) error {
	if name == "order" {
		// reverse lookup -> order_id, customer_id
		c.Orders = append(c.Orders, &Order{ID: startID, Customer: c})
		return nil
	}
	return fmt.Errorf("unknown relationship %s", name)
}

type Order struct {
	ID       string
	Customer *Customer `dynamodbav:"-"`
}

func (o Order) DynamoID() string              { return o.ID }
func (o Order) DynamoItemType() string        { return "order" }
func (o Order) DynamoRelationships() []string { return []string{"customer"} }

func (o Order) DynamoGetRelationship(name string) []entity.Data {
	if name == "customer" {
		return []entity.Data{o.Customer}
	}
	return nil
}

func (o Order) DynamoGetRelationshipSortKey(name string) string {
	// customer is the reverse lookup for customer -> order, so we want to ensure
	// the sort keys are the same.
	if name == "customer" {
		return "customer/orders"
	}
	return ""
}

func (o Order) DynamoIsReverseRelationship(name string) bool {
	// order -> customer relationships are stored on this partition.
	return name != "customer"
}

func (o *Order) UnmarshalRelationship(name string, startID, endID string) error {
	if name == "customer" {
		// forward lookup -> order_id, customer_id
		o.Customer = &Customer{ID: endID}
		return nil
	}
	return fmt.Errorf("unknown relationship %s", name)
}

type fixture struct{}

func (fixture) awscfg(t *testing.T) aws.Config {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("DUMMYID", "DUMMYKEY", "dynamodb-local"),
		),
		config.WithRegion("us-west-2"),
	)

	if err != nil {
		t.Fatalf("failed to load config, %v", err)
	}

	return cfg
}

func (fixture) createTable(t *testing.T, client *dynamodb.Client, g entity.Graph) {
	throughput := &types.OnDemandThroughput{
		MaxReadRequestUnits:  aws.Int64(100),
		MaxWriteRequestUnits: aws.Int64(100),
	}
	ctx := context.TODO()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:          &g.Options.TableName,
		BillingMode:        types.BillingModePayPerRequest,
		OnDemandThroughput: throughput,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(entity.AttributeNameHK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(entity.AttributeNameSK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(entity.AttributeNameItemType),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(entity.AttributeNameReverseLookupSortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(entity.AttributeNameCollectionSortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(entity.AttributeNameHK),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(entity.AttributeNameSK),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:          &g.Options.ReverseLookupIndexName,
				OnDemandThroughput: throughput,
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(entity.AttributeNameSK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(entity.AttributeNameReverseLookupSortKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName:          &g.Options.CollectionQueryIndexName,
				OnDemandThroughput: throughput,
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(entity.AttributeNameItemType),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(entity.AttributeNameCollectionSortKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	})

	var riue *types.ResourceInUseException
	if err != nil && errors.As(err, &riue) {
		return
	}

	if err != nil {
		t.Fatalf("failed to create table, %v", err)
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: &g.Options.TableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String(entity.AttributeNameExpires),
			Enabled:       aws.Bool(true),
		},
	})

	if err != nil {
		t.Fatalf("failed to set ttl, %v", err)
	}
}

func (f fixture) runInteg(t *testing.T, fn func(*testing.T, *dynamodb.Client, entity.Graph)) {
	cfg := f.awscfg(t)
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000/")
	})
	graph := entity.New("ezddb-integration-test-table", func(o *entity.TableOptions) {
		o.IDGenerator = f
	})
	f.createTable(t, client, graph)
	fn(t, client, graph)
}

func (fixture) GenerateID(ctx context.Context) string { return ulid.Make().String() }

func TestGraphIntegration(t *testing.T) {
	fixture := fixture{}
	fixture.runInteg(t, func(t *testing.T, client *dynamodb.Client, graph entity.Graph) {
		t.Run("put customer", func(t *testing.T) {
			customer := Customer{
				ID:   "customer-1",
				Name: "Bob",
				Orders: []*Order{
					{ID: "order-1"},
					{ID: "order-2"},
				},
			}
			_, err := graph.AddEntity(customer).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to put entity, %v", err)
			}
		})

		t.Run("put order", func(t *testing.T) {
			order := Order{
				ID: "order-3",
				Customer: &Customer{
					ID: "customer-2",
				},
			}
			_, err := graph.AddEntity(order).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to put entity, %v", err)
			}
		})

		t.Run("get customer", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			got, err := graph.GetEntity(customer).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to get entity, %v", err)
			}

			gotCustomer, err := entity.UnmarshalEntity[Customer](got.Item)
			if err != nil {
				t.Errorf("failed to unmarshal entity, %v", err)
			}
			assert.Equal(t, customer.ID, gotCustomer.ID)
			assert.Equal(t, "Bob", gotCustomer.Name)
		})

		t.Run("get order", func(t *testing.T) {
			order := Order{ID: "order-3"}
			_, err := graph.GetEntity(order).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to get entity, %v", err)
			}
		})

		t.Run("put relationships", func(t *testing.T) {
			customer := Customer{
				ID: "customer-1",
				Orders: []*Order{
					{ID: "order-1"},
					{ID: "order-2"},
				},
			}
			got, err := graph.AddRelationships(customer).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to put entity, %v", err)
			}
			assert.NotNil(t, got)

			order := Order{
				ID: "order-3",
				Customer: &Customer{
					ID: "customer-2",
				},
			}
			got, err = graph.AddRelationships(order).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to put entity, %v", err)
			}
			assert.NotNil(t, got)
		})

		t.Run("get relationships", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			got, err := graph.ListRelationships(customer, func(lrq *entity.ListRelationshipsQuery) {
				lrq.Reverse = true
				lrq.Relationship = "orders"
			}).Execute(context.TODO(), client)
			if err != nil {
				t.Errorf("failed to get entity, %v", err)
				return
			}
			assert.NotNil(t, got)
			assert.Len(t, got, 2)
		})
	})
}
