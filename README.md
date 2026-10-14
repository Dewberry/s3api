# s3api

[![E2E Tests](https://github.com/Dewberry/s3api/actions/workflows/e2e-tests.yml/badge.svg?event=push)](https://github.com/Dewberry/s3api/actions/workflows/e2e-tests.yml)

API for providing web applications endpoints for interacting with data stored in S3

## Fine Grain Access Control:

For testing fine grain access control, add an entry to the permissions table like:

```
INSERT INTO public.permissions (user_email, operation, allowed_s3_prefixes) VALUES ('sputnam@dewberry.com', 'write', ARRAY['/ffrd-trinity/sputnam/']);
```

## AWS credentials (`.env.json`)

Static keys (unchanged):
```json
{
  "accounts": [{ "AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..." }],
  "bucket_allow_list": ["*"],
  "bucket_block_list": []
}
```

EC2 instance role (no keys):
```json
{
  "use_iam_role": true,
  "bucket_allow_list": ["bucket-a", "bucket-b"],
  "bucket_block_list": []
}
```

- IAM mode is used when `use_iam_role` is true, `accounts` is empty, or the file is empty. Credentials then come only from EC2 instance metadata; static keys and `AWS_*` env vars are ignored.
- `bucket_block_list`: buckets that are never exposed, even when `bucket_allow_list` is `["*"]`.
- With named buckets, s3api checks each with HeadBucket at startup (no `s3:ListAllMyBuckets` needed). With `["*"]` the role needs `s3:ListAllMyBuckets`.
- Role permissions: `s3:ListBucket`, `s3:GetBucketLocation` on each bucket; `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` on `bucket/*`. Cross-account buckets also need a bucket-policy statement allowing the role.
- Docker on EC2 with IMDSv2: set the instance metadata hop limit to 2.

## Local S3 mock

Set `S3_MOCK=1` and `COMPOSE_PROFILES=s3mock` in `.env` to run against an in-memory Moto S3 (no AWS needed).
