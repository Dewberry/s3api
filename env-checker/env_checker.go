package envcheck

import (
	"errors"
	"fmt"
	"os"
)

var REQUIRED_ENV_VAR = []string{"KEYCLOAK_ID", "KEYCLOAK_SECRET", "KEYCLOAK_ISSUER", "KEYCLOAK_PUBLIC_KEYS_URL", "URL_EXP_DAYS", "TEMP_PREFIX"}

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
		return errors.New(errMsg)
	}

	return nil
}
