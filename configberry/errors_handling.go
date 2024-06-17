package configberry

import (
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/go-playground/validator"
	"github.com/jackc/pgx"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

// ErrorType is a struct that holds both the numeric value and the name of the error type.
type ErrorType struct {
	Value uint
	Name  string
}

// Define constants for different types of errors as structs.
var (
	DatabaseError       = ErrorType{Value: 1, Name: "Database Error"}
	ValidationError     = ErrorType{Value: 2, Name: "Validation Error"}
	NotFoundError       = ErrorType{Value: 3, Name: "Not Found Error"}
	UnauthorizedError   = ErrorType{Value: 4, Name: "Unauthorized Error"}
	InternalServerError = ErrorType{Value: 5, Name: "Internal Server Error"}
	AWSError            = ErrorType{Value: 6, Name: "AWS Error"}
	Fatal               = ErrorType{Value: 7, Name: "Fatal Error"}
)

// AppError includes the error type, message, and the original error.
type AppError struct {
	Type    ErrorType
	Message string
	Err     error
}

// NewAppError creates a new AppError.
func NewAppError(errorType ErrorType, message string, err error) *AppError {
	return &AppError{
		Type:    errorType,
		Message: message,
		Err:     err,
	}
}

// ErrorFormatter formats the error message.
func LogErrorFormatter(err *AppError, withOrgError bool) string {
	if withOrgError && err.Err != nil {
		return fmt.Sprintf("Type: %s, Error: %s, Original error: %v", err.Type.Name, err.Message, err.Err)
	}
	return fmt.Sprintf("Type: %s, Error: %s", err.Type.Name, err.Message)
}

func ErrorFormatter(err *AppError, withOrgError bool) string {
	if withOrgError && err.Err != nil {
		return fmt.Sprintf("%s, Original error: %v", err.Message, err.Err)
	}
	return err.Message
}

// HandleErrorResponse sends a JSON response with the error message and status code.
func HandleErrorResponse(c echo.Context, err *AppError) error {
	if err == nil {
		log.Error("Attempted to handle a nil *AppError")
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "An unexpected error occurred"})
	}

	responseMessage := ErrorFormatter(err, err.Type.Value != DatabaseError.Value)
	statusCode := http.StatusInternalServerError // Default status

	switch err.Type.Value {
	case ValidationError.Value:
		statusCode = http.StatusBadRequest
	case NotFoundError.Value:
		statusCode = http.StatusNotFound
	case UnauthorizedError.Value:
		statusCode = http.StatusUnauthorized
	}
	return c.JSON(statusCode, map[string]string{"Type": err.Type.Name, "Error": responseMessage})
}

// HandleStructValidationErrors checks if the error is a validation error, formats it, and returns an AppError.
func HandleStructValidationErrors(err error) *AppError {
	// Attempt to type-assert the error to a validator.ValidationErrors type
	if ve, ok := err.(validator.ValidationErrors); ok {
		// If the assertion is successful, process the validation errors
		errMsgs := make([]string, 0)
		for _, err := range ve {
			errMsgs = append(errMsgs, fmt.Sprintf("Field '%s' failed validation for rule '%s'", err.Field(), err.Tag()))
		}
		errMsg := strings.Join(errMsgs, ", ")
		return NewAppError(ValidationError, "Validation failed: "+errMsg, nil)
	}
	return NewAppError(InternalServerError, "error handeling validation error", err)
}

// HandleVarValidationErrors is similar to HandleStructValidationErrors but for standalone variables
func HandleVarValidationErrors(err error, variableName string) *AppError {
	// Attempt to type-assert the error to a validator.ValidationErrors type
	if ve, ok := err.(validator.ValidationErrors); ok {
		// If the assertion is successful, process the validation errors
		errMsgs := make([]string, 0)
		for _, err := range ve {
			// Use the provided variableName since ve.Field() won't be useful for single variable validation
			errMsgs = append(errMsgs, fmt.Sprintf("Variable '%s' failed validation for rule '%s'", variableName, err.Tag()))
		}
		errMsg := strings.Join(errMsgs, ", ")
		return NewAppError(ValidationError, "Validation failed: "+errMsg, nil)
	}
	return NewAppError(InternalServerError, "Error handling validation error", err)
}

func HandleSQLError(err error, errMsg string) *AppError {
	if err == sql.ErrNoRows {
		return NewAppError(NotFoundError, fmt.Sprintf("%s: %s", errMsg, pgx.ErrNoRows), nil) // Set the message as needed
	}

	if pqErr, ok := err.(pgx.PgError); ok {
		switch pqErr.Code {
		case "23503":
			return NewAppError(NotFoundError, fmt.Sprintf("%s: %s, SQLSTATE code is %s", errMsg, pqErr.Message, pqErr.Code), nil)
		case "22P02":
			return NewAppError(ValidationError, fmt.Sprintf("%s: %s, SQLSTATE code is %s", errMsg, pqErr.Message, pqErr.Code), nil)
		}
	}
	return NewAppError(DatabaseError, errMsg, err)
}

// HandleAWSError processes AWS-specific errors and returns an appropriate AppError.
// referencing https://github.com/aws/aws-sdk-go/blob/70ea45043fd9021c223e79de5755bc1b4b3af0aa/models/apis/cloudformation/2010-05-15/api-2.json
func HandleAWSError(err error, errMsg string) *AppError {
	if aerr, ok := err.(awserr.Error); ok {
		formattedMessage := fmt.Sprintf("%s: %s (AWS Error Code: %s)", errMsg, aerr.Message(), aerr.Code())
		switch aerr.Code() {
		case "AccessDenied", "InvalidCredentials":
			return NewAppError(UnauthorizedError, formattedMessage, err)
		case "NotFound":
			return NewAppError(NotFoundError, formattedMessage, err)
		case "NotUpdatable", "InvalidRequest", "AlreadyExists", "ResourceConflict", "Throttling", "ServiceLimitExceeded", "NotStabilized", "GeneralServiceException", "NetworkFailure", "InvalidTypeConfiguration", "NonCompliant", "Unknown", "UnsupportedTarget":
			return NewAppError(AWSError, formattedMessage, err)
		case "ServiceInternalError", "InternalFailure", "HandlerInternalFailure":
			return NewAppError(InternalServerError, formattedMessage, err)
		default:
			return NewAppError(AWSError, formattedMessage, err)
		}
	}
	return NewAppError(AWSError, errMsg, err)
}

// CheckRequiredParams checks if the required parameters are present and returns an error if any are missing.
func CheckRequiredParams(params map[string]string) *AppError {
	var missingParams []string
	for paramName, paramValue := range params {
		if paramValue == "" {
			missingParams = append(missingParams, paramName)
		}
	}
	if len(missingParams) > 0 {
		errMsg := fmt.Sprintf("The following required parameters are missing: %s", strings.Join(missingParams, ", "))
		return NewAppError(ValidationError, errMsg, nil)
	}
	return nil
}

// isEmpty checks if the provided data is considered empty.
func isEmpty(data interface{}) bool {
	if data == nil {
		return true
	}

	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		// Collections are empty if they have no elements.
		return v.Len() == 0
	case reflect.Ptr:
		// Pointers are empty if they are nil.
		return v.IsNil()
	case reflect.Struct:
		return false
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool, reflect.String:
		// For basic types, consider them non-empty even if they are zero values.
		return false
	default:
		// For all other data types, check if they are their zero value.
		return v.IsZero()
	}
}

// HandleSuccessfulResponse sends an appropriate JSON response with the given data or status code.
func HandleSuccessfulResponse(c echo.Context, data interface{}, statusCode ...int) error {
	// Determine the status code.
	code := http.StatusOK // Default status code is 200 OK.
	if len(statusCode) > 0 {
		code = statusCode[0] // Use provided status code if any.
	}
	// For all other data types, marshal as JSON.
	// Check if the data is struct or empty, then return an empty struct.
	//if data is an array it will automatically return an empty array with how we're defining array
	if isEmpty(data) && reflect.ValueOf(data).Kind() != reflect.Slice && reflect.ValueOf(data).Kind() != reflect.Array {
		return c.JSON(code, struct{}{})
	}
	switch v := data.(type) {
	case []byte:
		// If data is of type []byte, send it as a JSON blob.
		return c.JSONBlob(code, v)
	default:
		return c.JSON(code, data)
	}
}
