package table

import (
	"github.com/nisimpson/ezddb/internal/xslices"
	"github.com/nisimpson/ezddb/procedure"
)

const (
	MaxBatchWritesPerRequest = 25
	MaxBatchReadsPerRequest  = 100
)

type BatchWriteBuilder struct {
	modifiers []procedure.BatchWriteModifier
}

func BatchWrite(writers ...procedure.BatchWriteModifier) BatchWriteBuilder {
	builder := BatchWriteBuilder{modifiers: writers}
	return builder
}

func (b BatchWriteBuilder) Build() procedure.MultiBatchWrite {
	chunks := xslices.Chunk(b.modifiers, MaxBatchWritesPerRequest)
	procs := make([]procedure.BatchWrite, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewBatchWriteProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}

type BatchGetBuilder struct {
	modifiers []procedure.BatchGetModifier
}

func BatchGet(readers ...procedure.BatchGetModifier) BatchGetBuilder {
	builder := BatchGetBuilder{modifiers: readers}
	return builder
}

func (b BatchGetBuilder) Build() procedure.MultiBatchGet {
	chunks := xslices.Chunk(b.modifiers, MaxBatchReadsPerRequest)
	procs := make([]procedure.BatchGet, 0, len(chunks))
	for _, chunk := range chunks {
		proc := procedure.NewBatchGetProcedure()
		proc = proc.Modify(chunk...)
		procs = append(procs, proc)
	}
	return procs
}
