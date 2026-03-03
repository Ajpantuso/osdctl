package client

import (
	logrus "github.com/sirupsen/logrus"
)

type WithLogger struct {
	Logger *logrus.Logger
}
func (w WithLogger) ConfgiureDefaultClusterContextGetter(c *DefaultClusterContextGetterConfig) {
	c.Logger = w.Logger
}
func (w WithLogger) ConfigureDefaultMCClient(c *DefaultMCClientConfig) {
	c.Logger = w.Logger
}
