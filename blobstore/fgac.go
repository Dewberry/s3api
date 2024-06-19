package blobstore

import (
	"fmt"
	"os"
	"strings"

	"github.com/Dewberry/s3api/auth"
	"github.com/Dewberry/s3api/configberry"
	"github.com/Dewberry/s3api/utils"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

//Utility Methods for Endpoint Handlers

func (bh *BlobHandler) getS3ReadPermissions(c echo.Context, bucket string) ([]string, bool, *configberry.AppError) {
	permissions, fullAccess, appError := bh.getUserS3ReadListPermission(c, bucket)
	if appError != nil {
		return nil, false, appError
	}
	if !fullAccess && len(permissions) == 0 {
		return nil, false, configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have permission to read the %s bucket", bucket), nil)
	}
	return permissions, fullAccess, nil
}

func (bh *BlobHandler) getUserS3ReadListPermission(c echo.Context, bucket string) ([]string, bool, *configberry.AppError) {
	permissions := make([]string, 0)

	if bh.Config.AuthLevel > 0 {
		initAuth := os.Getenv("INIT_AUTH")
		if initAuth == "0" {
			return permissions, false, configberry.NewAppError(configberry.ForbiddenError, "this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality", nil)
		}
		fullAccess := false
		claims, ok := c.Get("claims").(*auth.Claims)
		if !ok {
			return permissions, fullAccess, configberry.NewAppError(configberry.InternalServerError, "could not get claims from request context", nil)
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
			return permissions, fullAccess, configberry.HandleSQLError(err, "error getting common prefix that the user can read and write to")
		}
		return permissions, fullAccess, nil
	}

	return permissions, true, nil
}

func (bh *BlobHandler) validateUserAccessToPrefix(c echo.Context, bucket, prefix string, permissions []string) *configberry.AppError {
	if bh.Config.AuthLevel > 0 {
		initAuth := os.Getenv("INIT_AUTH")
		if initAuth == "0" {
			return configberry.NewAppError(configberry.ForbiddenError, "this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality", nil)
		}
		claims, ok := c.Get("claims").(*auth.Claims)
		if !ok {
			return configberry.NewAppError(configberry.InternalServerError, "could not get claims from request context", nil)
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
				return configberry.NewAppError(configberry.ForbiddenError, fmt.Sprintf("user does not have %+q access to %s", permissions, prefix), nil)
			}
		}
	}
	return nil
}

func (bh *BlobHandler) HandleCheckS3UserPermission(c echo.Context) error {
	if bh.Config.AuthLevel == 0 {
		log.Info("Checked user permissions successfully")
		return configberry.HandleSuccessfulResponse(c, true)
	}
	initAuth := os.Getenv("INIT_AUTH")
	if initAuth == "0" {
		appErr := configberry.NewAppError(configberry.ForbiddenError, "this endpoint requires authentication information that is unavailable when authorization is disabled. Please enable authorization to use this functionality", nil)
		log.Error(configberry.LogErrorFormatter(appErr, false))
		return configberry.HandleErrorResponse(c, appErr)
	}

	params := map[string]string{
		"prefix":    c.QueryParam("prefix"),
		"bucket":    c.QueryParam("bucket"),
		"operation": c.QueryParam("operation"),
	}
	if appErr := configberry.CheckRequiredParams(params); appErr != nil {
		log.Error(configberry.LogErrorFormatter(appErr, false))
		return configberry.HandleErrorResponse(c, appErr)
	}
	claims, ok := c.Get("claims").(*auth.Claims)
	if !ok {
		appErr := configberry.NewAppError(configberry.InternalServerError, "could not get claims from request context", nil)
		log.Error(configberry.LogErrorFormatter(appErr, false))
		return configberry.HandleErrorResponse(c, appErr)
	}
	userEmail := claims.Email

	isAllowed := bh.DB.CheckUserPermission(userEmail, params["bucket"], params["prefix"], []string{params["operation"]})
	log.Info("Checked user permissions successfully")
	return configberry.HandleSuccessfulResponse(c, isAllowed)
}
