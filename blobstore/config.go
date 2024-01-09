package blobstore

import (
	"log"
	"os"
	"strconv"
)

// these are the default values of limits if not predefined by user from env
const (
	defaultZipDownloadSizeLimit           = 5                //gb
	defaultScriptDownloadSizeLimit        = 50               //gb
	defaultUploadPresignedUrlExpiration   = 15               //minutes
	defaultDownloadPresignedUrlExpiration = 7                //days
	defaultTempPrefix                     = "downloads-temp" //prefix
)

func newConfig(authLvl int) *Config {
	c := &Config{
		AuthLevel:                             authLvl,
		LimitedWriterRoleName:                 os.Getenv("AUTH_LIMITED_WRITER_ROLE"),
		DefaultTempPrefix:                     getEnvOrDefault("TEMP_PREFIX", defaultTempPrefix),
		DefaultDownloadPresignedUrlExpiration: getIntEnvOrDefault("DOWNLOAD_URL_EXP_DAYS", defaultDownloadPresignedUrlExpiration),
		DefaultUploadPresignedUrlExpiration:   getIntEnvOrDefault("UPLOAD_URL_EXP_MIN", defaultUploadPresignedUrlExpiration),
		DefaultScriptDownloadSizeLimit:        getIntEnvOrDefault("SCRIPT_DOWNLOAD_SIZE_LIMIT", defaultScriptDownloadSizeLimit),
		DefaultZipDownloadSizeLimit:           getIntEnvOrDefault("ZIP_DOWNLOAD_SIZE_LIMIT", defaultZipDownloadSizeLimit),
	}
	return c
}

// getEnvOrDefault returns the value of an environment variable or a default value if not set
func getEnvOrDefault(envKey string, defaultValue string) string {
	if value, exists := os.LookupEnv(envKey); exists {
		return value
	}
	return defaultValue
}

// getIntEnvOrDefault does the same as getEnvOrDefault but for integer values
func getIntEnvOrDefault(envKey string, defaultValue int) int {
	valueStr, exists := os.LookupEnv(envKey)
	if !exists {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Printf("Error parsing %s, defaulting to %v: %v", envKey, defaultValue, err)
		return defaultValue
	}
	return value
}
