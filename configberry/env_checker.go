package configberry

import (
	"fmt"
	"os"
)

func CheckEnvVariablesExist(envVars []string) error {
	// Check if we should use the mock implementation
	if os.Getenv("USE_MOCK_CHECK_ENV") == "true" {
		return mockCheckEnvVariablesExist(envVars)
	}

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

func mockCheckEnvVariablesExist(envVars []string) error {
	var missingVars []string

	for _, envVar := range envVars {
		if os.Getenv(envVar) == "" {
			missingVars = append(missingVars, envVar)
		}
	}

	if len(missingVars) > 0 {
		return fmt.Errorf("the following environment variables are missing: %v", missingVars)
	}

	return nil
}
