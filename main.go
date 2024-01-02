package main

import (
	"os"
	"strconv"

	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/blobstore"
	envcheck "github.com/Dewberry/s3api/env-checker"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	log "github.com/sirupsen/logrus"
)

func main() {
	if err := envcheck.CheckEnvVariablesExist(envcheck.REQUIRED_ENV_VAR); err != nil {
		log.Fatal(err)
	}
	log.SetFormatter(&log.JSONFormatter{})
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.WithError(err).Error("Invalid log level")
		level = log.InfoLevel
	}
	log.SetLevel(level)
	log.SetReportCaller(true)
	log.Infof("level level set to: %s", level)
	// administrator := []string{"administrator", "read", "write"}
	admin := []string{"s3_admin"}
	allUsers := []string{"s3_admin", "s3_reader", "s3_writer"}
	writers := []string{"s3_admin", "s3_writer"}

	var authLvl int
	authLvlString := os.Getenv("AUTH_LEVEL")
	if authLvlString == "" {
		authLvl = 0
		log.Warn("Fine Grained Access Control disabled")
	} else {
		authLvl, err = strconv.Atoi(authLvlString)
		if err != nil {
			log.Fatal(err)
		}
		s3LimitWriterRoleName, ok := os.LookupEnv("AUTH_LIMITED_WRITER_ROLE")
		if !ok {
			log.Fatal("AUTH_S3_LIMITED_WRITER env variable not set")
		}
		allUsers = append(allUsers, s3LimitWriterRoleName)
		writers = append(writers, s3LimitWriterRoleName)
	}

	envJson := "/app/.env.json"

	bh, err := blobstore.NewBlobHandler(envJson, authLvl)
	if err != nil {
		log.Fatalf("error initializing a new blobhandler: %v", err)
	}

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowCredentials: true,
		AllowOrigins:     []string{"*"},
	}))

	e.GET("/ping_with_auth", auth.Authorize(bh.PingWithAuth, allUsers...))
	e.GET("/ping", bh.Ping)

	// object content
	e.GET("/object/metadata", auth.Authorize(bh.HandleGetMetaData, allUsers...))
	e.GET("/object/content", auth.Authorize(bh.HandleObjectContents, allUsers...))
	e.PUT("/object/move", auth.Authorize(bh.HandleMoveObject, admin...))
	e.GET("/object/download", auth.Authorize(bh.HandleGetPresignedDownloadURL, allUsers...))
	e.POST("/object/upload", auth.Authorize(bh.HandleMultipartUpload, writers...))
	e.DELETE("/object/delete", auth.Authorize(bh.HandleDeleteObject, admin...))
	e.GET("/object/exists", auth.Authorize(bh.HandleGetObjExist, allUsers...))
	e.GET("/object/presigned_upload", auth.Authorize(bh.HandleGetPresignedUploadURL, allUsers...))
	// prefix
	e.GET("/prefix/list", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/list_with_details", auth.Authorize(bh.HandleListByPrefixWithDetail, allUsers...))
	e.GET("/prefix/download", auth.Authorize(bh.HandleGetPresignedURLMultiObj, allUsers...))
	e.GET("/prefix/download/script", auth.Authorize(bh.HandleGenerateDownloadScript, allUsers...))
	e.PUT("/prefix/move", auth.Authorize(bh.HandleMovePrefix, writer...))
	e.DELETE("/prefix/delete", auth.Authorize(bh.HandleDeletePrefix, admin...))
	e.GET("/prefix/size", auth.Authorize(bh.HandleGetSize, allUsers...))

	// universal
	e.DELETE("/delete_keys", auth.Authorize(bh.HandleDeleteObjectsByList, admin...))

	// multi-bucket
	e.GET("/list_buckets", auth.Authorize(bh.HandleListBuckets, allUsers...))
	// multi-bucket -- not implemented
	// e.PUT("/object/cross-bucket/copy", auth.Authorize(bh., writer...))
	// e.PUT("/prefix/cross-bucket/copy", auth.Authorize(bh., writer...))

	e.Logger.Fatal(e.Start(":" + os.Getenv("S3API_SERVICE_PORT")))
}
