/*
Copyright 2024 Andrew Meredith

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
