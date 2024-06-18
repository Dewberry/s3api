package blobstore

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/utils"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

//Utility Methods for Endpoint Handlers

func (bh *BlobHandler) getS3ReadPermissions(c echo.Context, bucket string) ([]string, bool, int, error) {
	permissions, fullAccess, err := bh.getUserS3ReadListPermission(c, bucket)
	if err != nil {
		//TEMP solution before error library is implimented and string check ups become redundant
		httpStatus := http.StatusInternalServerError
		if strings.Contains(err.Error(), "this endpoint requires authentication information that is unavailable when authorization is disabled.") {
			httpStatus = http.StatusForbidden
		}
		return nil, false, httpStatus, fmt.Errorf("error fetching user permissions: %s", err.Error())
	}
	if !fullAccess && len(permissions) == 0 {
		return nil, false, http.StatusForbidden, fmt.Errorf("user does not have permission to read the %s bucket", bucket)
	}
	return permissions, fullAccess, http.StatusOK, nil
}

func (bh *BlobHandler) getUserS3ReadListPermission(c echo.Context, bucket string) ([]string, bool, error) {
	permissions := make([]string, 0)

	if bh.Config.AuthLevel > 0 {
		initAuth := os.Getenv("INIT_AUTH")
		if initAuth == "0" {
			errMsg := fmt.Errorf("this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality")
			return permissions, false, errMsg
		}
		fullAccess := false
		claims, ok := c.Get("claims").(*auth.Claims)
		if !ok {
			return permissions, fullAccess, fmt.Errorf("could not get claims from request context")
		}
		roles := claims.RealmAccess["roles"]

		// Check if user has the limited reader role
		isLimitedReader := utils.StringInSlice(bh.Config.LimitedReaderRoleName, roles)

		// If user is not a limited reader, assume they have full read access
		if !isLimitedReader {
			fullAccess = true // Indicating full access
			return permissions, fullAccess, nil
		}

		// If user is a limited reader, fetch specific permissions
		ue := claims.Email
		permissions, err := bh.DB.GetUserAccessiblePrefixes(ue, bucket, []string{"read", "write"})
		if err != nil {
			return permissions, fullAccess, err
		}
		return permissions, fullAccess, nil
	}

	return permissions, true, nil
}

func (bh *BlobHandler) validateUserAccessToPrefix(c echo.Context, bucket, prefix string, permissions []string) (int, error) {
	if bh.Config.AuthLevel > 0 {
		initAuth := os.Getenv("INIT_AUTH")
		if initAuth == "0" {
			errMsg := fmt.Errorf("this requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality")
			return http.StatusForbidden, errMsg
		}
		claims, ok := c.Get("claims").(*auth.Claims)
		if !ok {
			return http.StatusInternalServerError, fmt.Errorf("could not get claims from request context")
		}
		roles := claims.RealmAccess["roles"]
		ue := claims.Email

		// Check for required roles
		isLimitedWriter := utils.StringInSlice(bh.Config.LimitedWriterRoleName, roles)
		// Ensure the prefix ends with a slash
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		// We assume if someone is limited_writer, they should never be admin or super_writer
		if isLimitedWriter {
			if !bh.DB.CheckUserPermission(ue, bucket, prefix, permissions) {
				return http.StatusForbidden, fmt.Errorf("forbidden")
			}
		}
	}
	return 0, nil
}

func (bh *BlobHandler) HandleCheckS3UserPermission(c echo.Context) error {
	if bh.Config.AuthLevel == 0 {
		log.Info("Checked user permissions successfully")
		return c.JSON(http.StatusOK, true)
	}
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		errMsg := fmt.Errorf("this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusForbidden, errMsg.Error())
	}
	prefix := c.QueryParam("prefix")
	bucket := c.QueryParam("bucket")
	operation := c.QueryParam("operation")
	claims, ok := c.Get("claims").(*auth.Claims)
	if !ok {
		errMsg := fmt.Errorf("could not get claims from request context")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusInternalServerError, errMsg.Error())
	}
	userEmail := claims.Email
	if operation == "" || prefix == "" || bucket == "" {
		errMsg := fmt.Errorf("`prefix`,  `operation` and 'bucket are required params")
		log.Error(errMsg.Error())
		return c.JSON(http.StatusUnprocessableEntity, errMsg.Error())
	}
	isAllowed := bh.DB.CheckUserPermission(userEmail, bucket, prefix, []string{operation})
	log.Info("Checked user permissions successfully")
	return c.JSON(http.StatusOK, isAllowed)
}
