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

	// metadata for objects
	e.GET("/object/metadata", auth.Authorize(bh.HandleGetMetaData, allUsers...))
	e.GET("/object/size", auth.Authorize(bh.HandleGetSize, allUsers...))

	// object content
	e.GET("/object/content", auth.Authorize(bh.HandleObjectContents, allUsers...))
	e.PUT("/object/move", auth.Authorize(bh.HandleCopyObject, writer...))
	e.GET("/object/download", auth.Authorize(bh.HandleGetPresignedURL, allUsers...))
	e.POST("/object/upload", auth.Authorize(bh.HandleMultipartUpload, writer...))
	e.DELETE("/object/delete", auth.Authorize(bh.HandleDeleteObjects, admin...))

	// listing
	e.GET("/prefix/list", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/list_with_details", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/download", auth.Authorize(bh.HandleGetPresignedURLMultiObj, allUsers...))
	e.DELETE("/prefix/delete", auth.Authorize(bh.HandleDeleteObjectsByList, admin...))

	// multi-bucket
	e.PUT("/object/copy", auth.Authorize(bh.HandleCopyObject, writer...))
	e.PUT("/prefix/copy", auth.Authorize(bh.HandleCopyObject, writer...))

	e.Logger.Fatal(e.Start(":" + apiConfig.Port))
	e.Logger.SetLevel(log.DEBUG)

}
