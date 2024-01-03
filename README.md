# s3api

[![E2E Tests](https://github.com/Dewberry/s3api/actions/workflows/e2e-tests.yml/badge.svg?event=push)](https://github.com/Dewberry/s3api/actions/workflows/e2e-tests.yml)

API for providing web applications endpoints for interacting with data stored in S3

## Fine Grain Access Control:

For testing fine grain access control, add an entry to the permissions table like:

```
INSERT INTO public.permissions (user_email, operation, allowed_s3_prefixes) VALUES ('sputnam@dewberry.com', 'write', ARRAY['/ffrd-trinity/sputnam/']);
```
