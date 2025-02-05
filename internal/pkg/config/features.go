package config

const (
	DeltaReplication = "delta"
)

// Checks if a feature is enabled from the list of available features.
// The default return value is false.
func IsFeatureEnabled(feature string) bool {
	if enabled, found := Current.Repl.FeaturesEnabled[feature]; found {
		return enabled
	}
	return false
}
