//go:build !test
// +build !test

package auth

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

func init() {
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		log.Println("Skipping authentication initialization")
		return
	}

	var err error
	publicKeys, err = getPublicKeys()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize authentication: %v", err))
	}
}
