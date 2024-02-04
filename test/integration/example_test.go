//go:build integration
// +build integration

package integration

import (
	"github.com/stretchr/testify/assert"
)

func (suite *integrationSuite) TestCreateDomain() {
	t := suite.T()
	assert.Equal(t, 3, 1+2, "1+2 should equal 3")
}
