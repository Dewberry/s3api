package config

import (
	"fmt"
	"log"
	"os"

	"github.com/Dewberry/s3api/blobstore"
)

const CSI string = `
------------------------------------------------------------------------
    ____               __                               ___________ ____
   / __ \___ _      __/ /_  ___  ____________  __      / ____/ ___//  _/
  / / / / _ \ | /| / / __ \/ _ \/ ___/ ___/ / / /_____/ /    \__ \ / /
 / /_/ /  __/ |/ |/ / /_/ /  __/ /  / /  / /_/ /_____/ /___ ___/ // /
/_____/\___/|__/|__/_.___/\___/_/  /_/   \__, /      \____//____/___/
                                        /____/
-----------------------------------------------------------------------`

type APIConfig struct {
	Name    string
	Port    string
	Host    string
	Address string
	Build   string
	BH      *blobstore.BlobHandler
}

func Init() *APIConfig {
	return newAPI()
}

func newAPI() *APIConfig {
	bh, err := blobstore.NewBlobHandler()
	if err != nil {
		log.Fatalf("error initializing a new blobhandler: %v", err)
	}
	api := new(APIConfig)
	api.Name = SERVICE_NAME
	api.Port = SERVICE_PORT
	api.Host = DEFAULT_HOST
	api.Address = fmt.Sprintf("%s:%s", DEFAULT_HOST, SERVICE_PORT)
	api.Build = os.Getenv("BUILD_TYPE")
	api.BH = bh
	return api
}
