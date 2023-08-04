package blobstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func (bh *BlobHandler) HandleMultipartUpload(c echo.Context) error {
	// Add overwrite check and parameter
	key := c.QueryParam("key")
	if key == "" {
		err := errors.New("parameter 'key' is required")
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	bucket := c.QueryParam("bucket")
	if bucket == "" {
		if os.Getenv("S3_BUCKET") == "" {
			err := errors.New("error: `bucket` parameter was not provided by the user and is not a global env variable")
			log.Info("HandleMultipartUpload: " + err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		bucket = os.Getenv("S3_BUCKET")
	}

	overrideParam := c.QueryParam("override")

	var override bool

	if overrideParam == "true" || overrideParam == "false" {
		var err error
		override, err = strconv.ParseBool(c.QueryParam("override"))
		if err != nil {
			log.Info("HandleMultipartUpload: Error parsing 'override' parameter:", err.Error())
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

	} else {
		err := errors.New("request must include a `override`, options are `true` or `false`")
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	keyExist, err := bh.keyExists(bucket, key)
	if err != nil {
		log.Info("HandleMultipartUpload: Error checking if key exists:", err.Error())
		return c.JSON(http.StatusBadRequest, err)
	}
	if keyExist && !override {
		err := fmt.Errorf("object %s already exists and override is set to %t", key, override)
		log.Info("HandleMultipartUpload: " + err.Error())
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	body := c.Request().Body
	defer func() { _ = body.Close() }()

	err = bh.UploadS3Obj(bucket, key, body)
	if err != nil {
		log.Info("HandleMultipartUpload: Error uploading S3 object:", err.Error())
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	log.Info("HandleMultipartUpload: Successfully uploaded file with key:", key)
	return c.JSON(http.StatusOK, "Successfully uploaded file")
}
