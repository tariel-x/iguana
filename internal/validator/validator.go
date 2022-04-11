package validator

import (
	"context"
	"fmt"

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

	registrySchema, err := v.client.GetSchema(schemaID)
	if err != nil {
		return false, fmt.Errorf("can not load scheme from registry: %w", err)
	}
	v.schemas[schemaID] = *registrySchema

	return v.validate(ctx, msg, v.schemas[schemaID])
}

func (v *Validator) validate(ctx context.Context, msg []byte, schema srclient.Schema) (bool, error) {
	//TODO: Make validation for protobuf
	//hardcode validation for protobuf
	return false, nil
}
