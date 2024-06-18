package configberry

import (
	"fmt"
	"os"
)

func CheckEnvVariablesExist(envVars []string) error {
	var missingVars []string

	for _, envVar := range envVars {
		_, exists := os.LookupEnv(envVar)
		if !exists {
			missingVars = append(missingVars, envVar)
		}
	}

	if len(missingVars) > 0 {
		errMsg := fmt.Errorf("the following environment variables are missing: %v", missingVars)
		return errMsg
	}

	return nil
}
