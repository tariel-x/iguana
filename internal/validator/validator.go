package validator

import (
	"context"

	"github.com/riferrei/srclient"
)

type Validator struct {
	client  srclient.ISchemaRegistryClient
	schemas map[int]srclient.Schema
}

func New(sr string) *Validator {
	c := srclient.CreateSchemaRegistryClient(sr)
	return &Validator{
		client:  c,
		schemas: map[int]srclient.Schema{},
	}
}

func (v *Validator) Validate(ctx context.Context, msg []byte, topic string, schemaID int) (bool, error) {
	schema, ok := v.schemas[schemaID]
	if ok {
		return v.validate(ctx, msg, schema)
	}

	//TODO: load scheme from registry

	return false, nil
}

func (v *Validator) validate(ctx context.Context, msg []byte, schema srclient.Schema) (bool, error) {
	//TODO: Make validation for protobuf

	return false, nil
}
