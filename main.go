package main

import (
	"app/auth"
	"app/config"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
)

func main() {

	// administrator := []string{"administrator", "read", "write"}
	admin := []string{"admin"}
	allUsers := []string{"admin", "reader", "writer"}
	writer := []string{"admin", "writer"}

	apiConfig := config.Init()
	bh := apiConfig.BH

	e := echo.New()
	e.Logger.SetLevel(log.ERROR)
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowCredentials: true,
		AllowOrigins:     []string{"*"},
	}))

	//health check
	e.GET("/ping", bh.Ping)
	//bucket endpoints
	e.GET("/s3/buckets", auth.Authorize(bh.HandleListBuckets, allUsers...))
	e.POST("/s3/buckets", auth.Authorize(bh.HandleCreateBucket, admin...))
	e.DELETE("/s3/buckets", auth.Authorize(bh.HandleDeleteBucket, admin...))
	e.GET("/s3/buckets/acl", auth.Authorize(bh.HandleGetBucketACL, admin...))
	//object/s metadata
	e.GET("/s3/get_metadata", auth.Authorize(bh.HandleGetMetaData, allUsers...))
	e.GET("/s3/get_size", auth.Authorize(bh.HandleGetSize, allUsers...))
	//object content
	e.GET("/s3/file_contents", auth.Authorize(bh.HandleObjectContents, allUsers...))
	//listing
	e.GET("/prefix/list", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/list_with_details", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	//downloading
	e.GET("/s3/download", auth.Authorize(bh.HandleGetPresignedURL, allUsers...))
	e.GET("/s3/download_folder", auth.Authorize(bh.HandleGetPresignedURLMultiObj, allUsers...))
	//uploading
	e.POST("/s3/stream_upload", auth.Authorize(bh.HandleMultipartUpload, writer...))
	//deleting objects
	e.DELETE("/s3/delete", auth.Authorize(bh.HandleDeleteObjects, admin...))
	e.DELETE("/s3/delete/list", auth.Authorize(bh.HandleDeleteObjectsByList, admin...))

	// multi-bucket
	e.PUT("/object/copy", auth.Authorize(bh.HandleCopyObject, writer...))
	e.PUT("/prefix/copy", auth.Authorize(bh.HandleCopyObject, writer...))

	e.Logger.Fatal(e.Start(":" + apiConfig.Port))
	e.Logger.SetLevel(log.DEBUG)

}
