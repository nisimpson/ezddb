# ezddb - Easy DynamoDB for Go

`ezddb` is a type-safe, ergonomic wrapper around AWS DynamoDB for Go applications. It simplifies common DynamoDB operations while maintaining type safety and providing a fluent interface for queries and data operations.

## Features

- Type-safe operations using Go generics
- Fluent interface for queries and conditions
- Simplified CRUD operations
- Support for transactions and batch operations
- Pagination helpers
- Expression builders for complex queries

## Installation

To add ezddb to your Go project:

```bash
go get github.com/nisimpson/ezddb
```

## Package Overview

### Main Packages

- `ezddb`: Core interfaces and types for DynamoDB operations
- `table`: Main entry point for opinionated table operations with type-safe record handling
- `query`: Expression builders for DynamoDB queries and conditions
- `stored`: Implementation of various DynamoDB stored procedures (Put, Get, Query, etc.)

## Usage Examples

### Defining a Table Record

```go
type User struct {
    ID        string
    Name      string
    Email     string
    CreatedAt time.Time
}

// Implement RecordMarshaler for User
func (u User) DynamoMarshalRecord(o *table.MarshalOptions) {
    o.HashKeyID = u.ID
}

// Create a new table client
users := table.New[User]("users-table")
```

### Basic Operations

```go
// Create a new user

// Put operation
putOp := users.Put(user)
_, err := putOp.Execute(ctx, dynamoClient)

// Get operation
getOp := users.Get(User{ID: "123"})
result, err := getOp.Execute(ctx, dynamoClient)
if err != nil {
    // Handle error
}

// Unmarshal the result
record, err := users.Unmarshal(result.Item)
user = record.Data
```

### Querying

```go
// Build a query using the expression builder
query := users.Query(table.LookupQuery{
    KeyCondition: table.AttributeHK.Equals("john@example.com"),
})

// Execute the query
result, err := query.Execute(ctx, dynamoClient)
```

### Conditional Updates

```go
// Update a user's email if it exists
updateOp := users.Update(User{ID: "123"},
    table.UpdateAttributes{
      Attributes: []string{"email"},
      Updates: map[string]table.UpdateAttribute{
        "email": func(update expression.Update) expression.UpdateBuilder {
          return update.Set(users.DataAttribute("email"), expression.Value("newemail@example.com"))
        }
      }
    },
)
_, err = updateOp.Execute(ctx, dynamoClient)
```

### Working with Transactions

```go
// Create a transaction with multiple operations
tx := stored.TransactWrite(
    users.Put(user1),
    users.Put(user2),
    users.Delete(user3),
)
_, err = tx.Execute(ctx, dynamoClient)
```

### Pagination

```go
// Create a paginator
paginator := users.Paginator(dynamoClient)

// Use pagination token
procedure := users.Query(table.LookupQuery{
    KeyCondition: query.Attribute("type").Equals("active"),
    StartKeyProvider: paginator,
    Cursor: "page-token"
})

// Execute the query
out, err := procedure.Execute(ctx, dynamoClient)

// Retrieve the next page token
token, err := paginator.GetStartKeyToken(ctx, out.LastEvaluatedKey)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
