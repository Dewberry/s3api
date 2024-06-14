package envcheck

import (
	"fmt"
	"log"
	"os"
)

var REQUIRED_ENV_VAR = []string{"S3API_SERVICE_PORT", "KEYCLOAK_PUBLIC_KEYS_URL"}

func init() {
	if err := CheckEnvVariablesExist(REQUIRED_ENV_VAR); err != nil {
		log.Fatal(err)
	}
}

func CheckEnvVariablesExist(envVars []string) error {
	var missingVars []string

	for _, envVar := range envVars {
		_, exists := os.LookupEnv(envVar)
		if !exists {
			missingVars = append(missingVars, envVar)
		}
	}

	if len(missingVars) > 0 {
		errMsg := fmt.Sprintf("The following environment variables are missing: %v", missingVars)
		return fmt.Errorf(errMsg)
	}

	return nil
}
