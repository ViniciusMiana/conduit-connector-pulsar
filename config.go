package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
type Config struct {
	// GlobalConfigParam is named global_config_param_name and needs to be
	// provided by the user.
	GlobalConfigParam string `json:"global_config_param_name" validate:"required"`

	// TODO add all config params that are relevant to prod use of Pulsar
	// URL is the Pulsar URL.
	URL string `json:"url" validate:"required"`

	// Topic is the Pulsar topic.
	Topic string `json:"topic" validate:"required"`

	SubscriptionName string `json:"subscription" validate:"required"`

	// ClientID is a unique identifier for client connections established by
	// this connector.
	ClientID string `json:"clientID" default:"conduit-connector-pulsar"`

	pulsarClientOpts []pulsar.ClientOptions
}
