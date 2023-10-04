package main

import (
	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/config"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {

	// administrator := []string{"administrator", "read", "write"}
	admin := []string{"admin"}
	allUsers := []string{"admin", "reader", "writer"}
	writer := []string{"admin", "writer"}

	apiConfig := config.Init()
	bh := apiConfig.BH

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowCredentials: true,
		AllowOrigins:     []string{"*"},
	}))

	e.GET("/ping", bh.Ping)
	e.GET("/ping_with_auth", auth.Authorize(bh.PingWithAuth, allUsers...))
	// object content
	e.GET("/object/metadata", auth.Authorize(bh.HandleGetMetaData, allUsers...))
	e.GET("/object/content", auth.Authorize(bh.HandleObjectContents, allUsers...))
	e.PUT("/object/move", auth.Authorize(bh.HandleMoveObject, writer...))
	e.GET("/object/download", auth.Authorize(bh.HandleGetPresignedURL, allUsers...))
	e.POST("/object/upload", auth.Authorize(bh.HandleMultipartUpload, writer...))
	e.DELETE("/object/delete", auth.Authorize(bh.HandleDeleteObject, admin...))
	e.GET("/object/exists", auth.Authorize(bh.HandleGetObjExist, allUsers...))
	// prefix
	e.GET("/prefix/list", auth.Authorize(bh.HandleListByPrefix, allUsers...))
	e.GET("/prefix/list_with_details", auth.Authorize(bh.HandleListByPrefixWithDetail, allUsers...))
	e.GET("/prefix/download", auth.Authorize(bh.HandleGetPresignedURLMultiObj, allUsers...))
	e.PUT("/prefix/move", auth.Authorize(bh.HandleMovePrefix, writer...))
	e.DELETE("/prefix/delete", auth.Authorize(bh.HandleDeletePrefix, admin...))
	e.GET("/prefix/size", auth.Authorize(bh.HandleGetSize, allUsers...))

	// universal
	e.DELETE("/delete_keys", auth.Authorize(bh.HandleDeleteObjectsByList, admin...))
	// multi-bucket -- not implemented
	// e.PUT("/object/cross-bucket/copy", auth.Authorize(bh., writer...))
	// e.PUT("/prefix/cross-bucket/copy", auth.Authorize(bh., writer...))

	e.Logger.Fatal(e.Start(":" + apiConfig.Port))
}
