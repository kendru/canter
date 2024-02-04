//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type integrationSuite struct {
	suite.Suite
	ctx context.Context
}

func (suite *integrationSuite) SetupSuite() {
	ctx := context.Background()
	suite.ctx = ctx

	// TODO start things up.
}

func (suite *integrationSuite) TearDownSuite() {
	fmt.Printf("Tearing down test suite\n")
	// TODO: Perform any cleanup here.
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(integrationSuite))
}
