package utils

var REQUIRED_ENV_VAR = []string{"S3API_SERVICE_PORT", "KEYCLOAK_PUBLIC_KEYS_URL"}

// Check if a string is in string slice
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
