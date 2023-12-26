package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/blobstore"
	envcheck "github.com/Dewberry/s3api/env-checker"
	"github.com/joho/godotenv"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	log "github.com/sirupsen/logrus"
)

var (
	envFP   string
	authSvc string
	authLvl string
)

func init() {
	// The order of precedence as Flag > Environment variable > Default value

	// Manually parse command line arguments to find the -e value since flag.Parse() can't be used
	for i, arg := range os.Args {
		if arg == "-e" && i+1 < len(os.Args) {
			envFP = os.Args[i+1]
			break
		}
	}

	if envFP != "" {
		err := godotenv.Load(envFP)
		if err != nil {
			log.Fatalf("could not read environment file: %s", err.Error())
		}
	}

	// Only variables that are needed at startup and will not be used after startup are available as CLI flags
	flag.StringVar(&envFP, "e", "", "specify the path of the dot env file to load")
	flag.StringVar(&authSvc, "au", resolveValue("AUTH_SERVICE", ""), "specify the auth service")
	flag.StringVar(&authLvl, "al", resolveValue("AUTH_LEVEL", "0"), "specify the authorization striction level")

	flag.Parse()
}

// Checks if there's an environment variable for this configuration,
// if yes, return the env value, if not, return the default value.
func resolveValue(envVar string, defaultValue string) string {
	if value, exists := os.LookupEnv(envVar); exists {
		return value
	}
	return defaultValue
}

const (
	authLevelNone = 0
	authLevelAll  = 1
)

func applyAuthMiddleware(e *echo.Echo, as auth.AuthStrategy, authLevel int) {
	switch authLevel {
	case authLevelAll:
		// Apply the Authorize middleware to all routes
		e.Use(auth.Authorize(as))
	}
}

func initAuth(e *echo.Echo) int {
	var as auth.AuthStrategy
	var err error

	authLvlInt, err := strconv.Atoi(authLvl)
	if err != nil {
		log.Fatalf("Error converting AUTH_LEVEL to number: %s", err.Error())
	}

	if authLvlInt == 0 {
		log.Warn("No authentication set up.")
		return 0
	} else {
		switch authSvc {
		case "keycloak":
			as, err = auth.NewKeycloakAuthStrategy()
			if err != nil {
				log.Fatalf("Error creating KeyCloak auth service: %s", err.Error())
			}
		default:
			log.Fatal("unsupported auth service provider type")
		}
	}

	applyAuthMiddleware(e, as, authLvlInt)
	return authLvlInt
}

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

	envJson := "/app/.env.json"

	bh, err := blobstore.NewBlobHandler(envJson)
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

	authLvl := initAuth(e)
	bh.Config.AuthLevel = authLvl

	e.GET("/ping_with_auth", bh.PingWithAuth)
	e.GET("/ping", bh.Ping)

	// object content
	e.GET("/object/metadata", bh.HandleGetMetaData)
	e.GET("/object/content", bh.HandleObjectContents)
	e.PUT("/object/move", bh.HandleMoveObject)
	e.GET("/object/download", bh.HandleGetPresignedDownloadURL)
	e.POST("/object/upload", bh.HandleMultipartUpload)
	e.DELETE("/object/delete", bh.HandleDeleteObject)
	e.GET("/object/exists", bh.HandleGetObjExist)
	e.GET("/object/presigned_upload", bh.HandleGetPresignedUploadURL)

	// prefix
	e.GET("/prefix/list", bh.HandleListByPrefix)
	e.GET("/prefix/list_with_details", bh.HandleListByPrefixWithDetail)
	e.GET("/prefix/download", bh.HandleGetPresignedURLMultiObj)
	e.PUT("/prefix/move", bh.HandleMovePrefix)
	e.DELETE("/prefix/delete", bh.HandleDeletePrefix)
	e.GET("/prefix/size", bh.HandleGetSize)

	// universal
	e.DELETE("/delete_keys", bh.HandleDeleteObjectsByList)

	// multi-bucket
	e.GET("/list_buckets", bh.HandleListBuckets)
	// multi-bucket -- not implemented
	// e.PUT("/object/cross-bucket/copy", bh.)
	// e.PUT("/prefix/cross-bucket/copy", bh.)

	// Start server
	go func() {
		log.Info("server starting on port: ", os.Getenv("S3API_SERVICE_PORT"))
		if err := e.Start(":" + os.Getenv("S3API_SERVICE_PORT")); err != nil && err != http.ErrServerClosed {
			log.Error("server error : ", err.Error())
			log.Fatal("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
	// Use a buffered channel to avoid missing signals as recommended for signal.Notify
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	<-quit
	log.Info("gracefully shutting down the server")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			log.Error(err)
		}
	}()

	// Shutdown the server
	// By default, Docker provides a grace period of 10 seconds with the docker stop command.

	// Kill any running docker containers (clean up resources)
	if err := bh.DB.Close(); err != nil {
		log.Error(err)
	} else {
		log.Info("closed connection to database")
	}

	log.Info("server gracefully shutdown")
}
