package main

import (
	"app/config"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
)

func main() {

	// administrator := []string{"administrator", "read", "write"}
	// writer := []string{"read", "write"}
	// reader := []string{"read"}

	apiConfig := config.Init()
	bh := apiConfig.BH

	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowCredentials: true,
		AllowOrigins:     []string{"*"},
	}))

	// e.GET("/get_size", auth.Authorize(bh.HandleGetSize, reader...))
	e.GET("/get_size", bh.HandleGetSize)

	e.Logger.Fatal(e.Start(":" + apiConfig.Port))
	e.Logger.SetLevel(log.DEBUG)

}
