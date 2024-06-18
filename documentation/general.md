# Kinds of Functions and Methods

### Normal Functions

Functions that return native Go errors and are used for utility logic.

### Methods Defined on `S3Ctrl`

Methods defined on `S3Ctrl` that return native Go errors. These methods encapsulate native AWS SDK Go functions to interact with S3. They can be used by both endpoint handlers and for package/library purposes. These methods can also encapsulate other `S3Ctrl` methods that perform specific utilities. For example, `RecursivelyDeleteObjects` uses `GetListWithCallBack` and `DeleteList`, which both utilize native AWS SDK Go functions to perform a common prefix (or folder) deletion.

### Methods Defined on `BlobHandler`

Methods defined on `BlobHandler` have three primary uses:

- **Endpoint Handlers:** Methods that communicate with HTTP requests and always return a ConfigBerry `AppError`. These methods' names should always start with `Handle`.

- **Utility Methods for Endpoint Handlers:** Private methods that encapsulate reusable logic for endpoints, such as `checkAndAdjustPrefix`. These methods also always return a ConfigBerry `AppError`.

- **Utility Methods for Both Endpoint and External Use:** Methods that can be used by both endpoint handlers and external packages/libraries, returning native Go errors. (Note: Only one method currently falls under this category, `GetController`.)

# deprecated functions/methods

deprecated functions will be in /utils/deprecated.txt, the file should consist of file_name where the function/methods were deprectaed from.
