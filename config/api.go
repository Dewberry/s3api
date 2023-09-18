package config

import (
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
	BH *blobstore.BlobHandler
}

func Init() *APIConfig {
	return newAPI()
}

func newAPI() *APIConfig {
	bh := blobstore.NewBlobHandler()
	api := new(APIConfig)
	api.BH = bh
	return api
}
