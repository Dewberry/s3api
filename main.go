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
	e.Logger.SetLevel(log.DEBUG)

	e.GET("/ping", auth.Authorize(bh.Ping, allUsers...))

	// object content
	e.GET("/object/metadata", auth.Authorize(bh.HandleGetMetaData, allUsers...))
	e.GET("/object/content", auth.Authorize(bh.HandleObjectContents, allUsers...))
	e.PUT("/object/move", auth.Authorize(bh.HandleMoveObject, writer...))
	e.GET("/object/download", auth.Authorize(bh.HandleGetPresignedURL, allUsers...))
	e.POST("/object/upload", auth.Authorize(bh.HandleMultipartUpload, writer...))
	e.DELETE("/object/delete", auth.Authorize(bh.HandleDeleteObjects, admin...))

	// prefix
	e.GET("/prefix/list", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/list_with_details", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/download", auth.Authorize(bh.HandleGetPresignedURLMultiObj, allUsers...))
	e.PUT("/prefix/move", auth.Authorize(bh.HandleMovePrefix, writer...))
	e.DELETE("/prefix/delete", auth.Authorize(bh.HandleDeleteObjects, admin...))

	// universal
	e.GET("/size", auth.Authorize(bh.HandleGetSize, allUsers...))
	e.DELETE("/delete_keys", auth.Authorize(bh.HandleDeleteObjectsByList, admin...))
	// multi-bucket -- not implemented
	// e.PUT("/object/cross-bucket/copy", auth.Authorize(bh., writer...))
	// e.PUT("/prefix/cross-bucket/copy", auth.Authorize(bh., writer...))

	e.Logger.Fatal(e.Start(":" + apiConfig.Port))
}
