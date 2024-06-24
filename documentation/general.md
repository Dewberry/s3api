# Kinds of Functions and Methods

### Normal Functions

Functions that return native Go errors and are used for utility logic.

### Methods Defined on `S3Ctrl`

Methods defined on `S3Ctrl` have two primary uses:

- **Methods defined on `S3Ctrl` that return native Go errors:** These methods encapsulate native AWS SDK Go functions to interact with S3. They can be used by both endpoint handlers and for package/library purposes. These methods can also encapsulate other `S3Ctrl` methods that perform specific utilities.

- **Methods defined on `S3Ctrl` that return return a ConfigBerry `AppError`:** These methods usually encapsulate other `S3Ctrl` methods that perform specific utilities. These methods are usually private.

### Methods Defined on `BlobHandler`

Methods defined on `BlobHandler` have three primary uses:

- **Endpoint Handlers:** Methods that communicate with HTTP requests and always return a ConfigBerry `AppError`. These methods' names should always start with `Handle`.

- **Utility Methods for Endpoint Handlers:** Private methods that encapsulate reusable logic for endpoints. These methods also always return a ConfigBerry `AppError`.

- **Utility Methods for Both Endpoint and External Use:** Methods that can be used by both endpoint handlers and external packages/libraries, returning native Go errors. (Note: Only one method currently falls under this category, `GetController`.)

# deprecated functions/methods

deprecated functions will be in /utils/deprecated.txt, the file should consist of file_name where the function/methods were deprectaed from.

# errors

## Error Handling Best Practices

Here are the error handling best practices you described, formatted in markdown and with a focus on clarity and conciseness:

**Formatting:**

- **Use Backticks `` ` `` to refrence parameters:** parameters refrenced in errors should be encapsulated inside backticks
- **Lowercase Custom Errors:** Start custom error messages with lowercase letters (unless the message is an acronym).

**Content:**

- **Descriptive Messages:** Craft error messages that are clear and informative, explaining the issue encountered.
- **Relevant Information:** Include details about the failing operation, such as key parameters, to assist with debugging.
- **No Punctuation at End:** Avoid ending error messages with periods or exclamation points.

**Error Creation:**

- **`errors.New` for Static Errors:** Use `errors.New` for static error messages that don't require dynamic information.

```go
var errInvalidInput = errors.New("invalid input")
```

- **`fmt.Errorf` for Dynamic Errors:** Use `fmt.Errorf` when you need to include dynamic information or wrap an existing error with additional context.

```go
err := fmt.Errorf("failed to process user ID %d: %w", userID, errInvalidInput)
```

**Error Return in Packages:**

- Methods in packages should return native Go error types.

**Wrapping Errors:**

- Don't wrap errors returned from your `s3Ctrl` functions unless the error originates from functionality unrelated to the method's main purpose. For example, in `s3Ctrl.DeleteObject`, wrap the existence check error because it doesn't directly concern deletion.

# Logging

Rules:

- **Only log errors in main methods/functions**: Do not log errors in the util function and teh util caller, restrict error logging to the function that is external and communicates with client (With teh exception of non breaking errors).
