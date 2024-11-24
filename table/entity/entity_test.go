package entity_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb/query"
	"github.com/nisimpson/ezddb/table"
	"github.com/nisimpson/ezddb/table/entity"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

type Customer struct {
	ID     string
	Name   string
	Orders []*Order `dynamodbav:"-"`
}

func (c Customer) EntityID() string   { return c.ID }
func (c Customer) EntityType() string { return "customer" }

func (c Customer) EntityRelationships() map[string]entity.Relationship {
	return map[string]entity.Relationship{
		// customer -> order relationships are stored on the order partition.
		"customer-order": {
			IsOneToMany: true,
			SortKey:     "customer/orders",
		},
	}
}

func (c Customer) EntityRef(name string) []entity.Data {
	if name == "customer-order" {
		return entity.DataSlice(c.Orders...)
	}
	return nil
}

func (c *Customer) UnmarshalEntityRef(name string, startID, endID string) error {
	if name == "customer-order" {
		// reverse lookup -> order_id, customer_id
		c.Orders = append(c.Orders, &Order{ID: startID, Customer: c})
		return nil
	}
	return fmt.Errorf("unknown relationship %s", name)
}

type Order struct {
	ID       string
	Item     string
	Customer *Customer `dynamodbav:"-"`
}

func (o Order) EntityID() string   { return o.ID }
func (o Order) EntityType() string { return "order" }

func (o Order) EntityRelationships() map[string]entity.Relationship {
	return map[string]entity.Relationship{
		// customer is stored on "this" side/partition, so we want to ensure
		// the sort keys are the same as the customer side.
		"customer-order": {
			IsOneToMany: false,
			SortKey:     "customer/orders",
		},
	}
}

func (o Order) EntityRef(name string) []entity.Data {
	if name == "customer-order" {
		return entity.DataSlice(o.Customer)
	}
	return nil
}

func (o *Order) UnmarshalEntityRef(name string, startID, endID string) error {
	if name == "customer-order" {
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
				AttributeName: aws.String(table.AttributeNameHK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(table.AttributeNameSK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(table.AttributeNameItemType),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(table.AttributeNameReverseLookupSortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(table.AttributeNameCollectionSortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(table.AttributeNameHK),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(table.AttributeNameSK),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:          &g.Options.ReverseLookupIndexName,
				OnDemandThroughput: throughput,
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(table.AttributeNameSK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(table.AttributeNameReverseLookupSortKey),
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
						AttributeName: aws.String(table.AttributeNameItemType),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(table.AttributeNameCollectionSortKey),
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
			AttributeName: aws.String(table.AttributeNameExpires),
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
	graph := entity.NewGraph("ezddb-integration-test-table", func(o *table.Options) {
		o.IDGenerator = f
	})
	f.createTable(t, client, graph)
	t.Cleanup(func() {
		_, err := client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
			TableName: &graph.Options.TableName,
		})
		if err != nil {
			t.Fatalf("failed to delete table, %v", err)
		}
	})
	fn(t, client, graph)
}

func (fixture) GenerateID(ctx context.Context) string { return ulid.Make().String() }

func TestGraphIntegration(t *testing.T) {
	fixture := fixture{}
	fixture.runInteg(t, func(t *testing.T, client *dynamodb.Client, g entity.Graph) {
		t.Run("put customer", func(t *testing.T) {
			_, err := g.AddEntity(Customer{
				ID:   "customer-1",
				Name: "Bob",
				Orders: []*Order{
					{ID: "order-1"},
					{ID: "order-2"},
				},
			}, func(to *table.Options) {
				to.MarshalOptions = append(to.MarshalOptions, func(mo *table.MarshalOptions) {})
			}).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to put entity: %v", err)
		})

		t.Run("put order", func(t *testing.T) {
			order := Order{
				ID: "order-3",
				Customer: &Customer{
					ID: "customer-2",
				},
			}
			_, err := g.AddEntity(order).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to put entity: %v", err)
		})

		t.Run("get customer", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			got, err := g.GetEntity(customer).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to get entity: %v", err)

			gotCustomer, err := entity.UnmarshalEntity[Customer](got.Item)
			assert.NoError(t, err, "failed to unmarshal entity: %v", err)
			assert.Equal(t, customer.ID, gotCustomer.ID)
			assert.Equal(t, "Bob", gotCustomer.Name)
		})

		t.Run("get order", func(t *testing.T) {
			order := Order{ID: "order-3"}
			_, err := g.GetEntity(order).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to get entity: %v", err)
		})

		t.Run("put relationships", func(t *testing.T) {
			customer := Customer{
				ID: "customer-1",
				Orders: []*Order{
					{ID: "order-1"},
					{ID: "order-2"},
				},
			}
			got, err := g.AddRelationships(customer).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to put entity: %v", err)
			assert.NotNil(t, got)

			order := Order{
				ID: "order-3",
				Customer: &Customer{
					ID: "customer-2",
				},
			}
			got, err = g.AddRelationships(order).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to put entity: %v", err)
			assert.NotNil(t, got)
		})

		t.Run("get customer orders (list relationships)", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			input, err := g.ListRelationships(customer, func(lrq *entity.ListRelationshipsQuery) {
				lrq.Reverse = true
				lrq.Relationship = "customer-order"
			}).Invoke(context.TODO())

			assert.NoError(t, err, "failed to generate input: %v", err)
			data, err := json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err := client.Query(context.TODO(), input)
			assert.NoError(t, err, "failed to get entity: %v", err)
			assert.NotNil(t, got.Items)

			for _, item := range got.Items {
				entity.UnmarshalEntityRef(item, &customer)
			}

			assert.Len(t, customer.Orders, 2)
		})

		t.Run("get customer order (get relationship)", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			order := Order{ID: "order-1"}
			input, err := g.GetRelationship(customer, order, "customer-order").Invoke(context.TODO())
			assert.NoError(t, err)
			data, err := json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err := client.GetItem(context.TODO(), input)
			assert.NoError(t, err, "failed to get entity: %v", err)
			assert.NotNil(t, got.Item)

			err = entity.UnmarshalEntityRef(got.Item, &customer)
			assert.NoError(t, err, "failed to unmarshal entity: %v", err)

			assert.Len(t, customer.Orders, 1)
			assert.Equal(t, "order-1", customer.Orders[0].ID)
		})

		t.Run("get order relationships", func(t *testing.T) {
			order := Order{ID: "order-3"}
			input, err := g.ListRelationships(order, func(lrq *entity.ListRelationshipsQuery) {
				lrq.Relationship = "customer-order"
			}).Invoke(context.TODO())

			assert.NoError(t, err, "failed to generate input: %v", err)
			data, err := json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err := client.Query(context.TODO(), input)
			assert.NoError(t, err, "failed to get entity: %v", err)
			assert.NotNil(t, got.Items)

			for _, item := range got.Items {
				entity.UnmarshalEntityRef(item, &order)
			}

			assert.NotNil(t, order.Customer)
			assert.Equal(t, "customer-2", order.Customer.ID)
		})

		t.Run("get order customer", func(t *testing.T) {
			order := Order{ID: "order-3"}
			input, err := g.GetRelationship(order, Customer{ID: "customer-2"}, "customer-order").Invoke(context.TODO())
			assert.NoError(t, err)
			data, err := json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err := client.GetItem(context.TODO(), input)
			assert.NoError(t, err, "failed to get entity: %v", err)
			assert.NotNil(t, got.Item)

			err = entity.UnmarshalEntityRef(got.Item, &order)
			assert.NoError(t, err, "failed to unmarshal entity: %v", err)

			assert.NotNil(t, order.Customer)
			assert.Equal(t, "customer-2", order.Customer.ID)
		})

		t.Run("list entities", func(t *testing.T) {
			input, err := g.ListEntities("customer").Invoke(context.TODO())
			assert.NoError(t, err, "failed to generate input: %v", err)
			data, err := json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err := client.Query(context.TODO(), input)
			assert.NoError(t, err, "failed to list entities: %v", err)
			assert.NotNil(t, got, "result is nil")
			assert.Len(t, got.Items, 1)

			gotCustomer, err := entity.UnmarshalEntity[Customer](got.Items[0])
			assert.NoError(t, err, "failed to unmarshal entity: %v", err)
			assert.Equal(t, "customer-1", gotCustomer.ID)

			input, err = g.ListEntities("customer", func(leq *entity.ListEntitiesQuery) {
				leq.Filter = leq.Filter.And(query.Equals(g.EntityAttribute("Name"), "Bob").Condition())
			}).Invoke(context.TODO())
			assert.NoError(t, err)
			data, err = json.MarshalIndent(input, "", " ")
			assert.NoError(t, err)
			t.Log(string(data))

			got, err = client.Query(context.TODO(), input)
			assert.NoError(t, err)
			assert.NotNil(t, got, "result is nil")
			assert.Len(t, got.Items, 1)
		})

		t.Run("delete customer relationship", func(t *testing.T) {
			customer := Customer{
				ID: "customer-1",
				Orders: []*Order{
					{ID: "order-1"},
				},
			}
			_, err := g.DeleteRelationship(customer, "customer-order").Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to delete relationship: %v", err)
		})

		t.Run("delete order customer", func(t *testing.T) {
			order := Order{
				ID: "order-3",
				Customer: &Customer{
					ID: "customer-2",
				},
			}
			_, err := g.DeleteRelationship(order, "customer-order").Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to delete relationship: %v", err)
		})

		t.Run("delete customer", func(t *testing.T) {
			customer := Customer{ID: "customer-1"}
			_, err := g.DeleteEntity(customer).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to delete entity: %v", err)
		})

		t.Run("pagination", func(t *testing.T) {
			_, err := g.AddEntity(Customer{
				ID:   "customer-2",
				Name: "Jim",
				Orders: []*Order{
					{ID: "order-4"},
					{ID: "order-5"},
				},
			}).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to put entity: %v", err)

			paginator := g.Paginator(client)
			items, err := g.ListEntities("customer", func(lrq *entity.ListEntitiesQuery) {
				lrq.Limit = 1
				lrq.StartKeyProvider = paginator
			}).Execute(context.TODO(), client)
			assert.NoError(t, err, "failed to list entities: %v", err)
			assert.NotNil(t, items.LastEvaluatedKey)
			cursor, err := paginator.GetStartKeyToken(context.TODO(), items.LastEvaluatedKey)
			assert.NoError(t, err)
			assert.NotEmpty(t, cursor, "page cursor should not be empty")
		})
	})
}
