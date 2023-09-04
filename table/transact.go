package table

import (
	"github.com/nisimpson/ezddb/internal/xslices"
	"github.com/nisimpson/ezddb/procedure"
)

const (
	MaxTransactWriteItems = 100
	MaxTransactReadItems  = 100
)

type TransactWriteBuilder struct {
	modifiers []procedure.TransactionWriteModifier
}

func TransactWrite(writers ...procedure.TransactionWriteModifier) TransactWriteBuilder {
	builder := TransactWriteBuilder{modifiers: writers}
	return builder
}

func (t TransactWriteBuilder) Build() procedure.MultiTransactionWrite {
	chunks := xslices.Chunk(t.modifiers, MaxTransactWriteItems)
	procs := make([]procedure.TransactionWrite, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewTransactionWriteProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}

type TransactGetBuilder struct {
	modifiers []procedure.TransactionGetModifier
}

func TransactGet(readers ...procedure.TransactionGetModifier) TransactGetBuilder {
	builder := TransactGetBuilder{modifiers: readers}
	return builder
}

func (t TransactGetBuilder) Build() procedure.MultiTransactionGet {
	chunks := xslices.Chunk(t.modifiers, MaxTransactWriteItems)
	procs := make([]procedure.TransactionGet, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewTransactionGetProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}
