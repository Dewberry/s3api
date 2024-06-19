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

Rules:

- **Use Backticks `` ` `` to refrence parameters:** parameters refrenced in errors should be encapsulated inside backticks
- **Avoid Capital Letters:** Custom errors should not start with a capital letter (unless they begin with an acronym).
- **Error Messages Should Be Descriptive:** Ensure that error messages are clear and provide enough context to understand the issue.
- **Include Relevant Information:** Include information about the operation that failed, such as key parameters, to aid in debugging.
- **Avoid Punctuation:** Do not end error messages with punctuation marks like periods or exclamation points.
- **Use `errors.New` for Static Errors:** Use `errors.New` when the error message is static and doesn't need any additional context.
  ```go
  var ErrInvalidInput = errors.New("invalid input")
  ```
- **Use `fmt.Errorf` for Dynamic Errors:** Use `fmt.Errorf` when you need to include dynamic information or wrap an existing error with additional context.
  ```go
  err := fmt.Errorf("failed to process user ID %d: %w", userID, ErrInvalidInput)
  ```
- Methods that will be used for packages should return naitive go error

# Logging

Rules:

- **Only log errors in main methods/functions**: Do not log errors in the util function and teh util caller, restrict error logging to the function that is external and communicates with client (With teh exception of non breaking errors).
