//go:build test
// +build test

package auth

import (
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestCheckUserPermission(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "test-bucket"
	prefix := "test-prefix"
	operations := []string{"read", "write"}

	query := regexp.QuoteMeta(`
	SELECT EXISTS (
		SELECT 1
		FROM permissions,
			 UNNEST(allowed_s3_prefixes) AS allowed_prefix
		WHERE user_email = $1
		  AND operation = ANY($2)
		  AND $3 LIKE allowed_prefix || '%'
	);
	`)
	mock.ExpectQuery(query).
		WithArgs(userEmail, pq.Array(operations), "/"+bucket+"/"+prefix).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	hasPermission := pgDB.CheckUserPermission(userEmail, bucket, prefix, operations)
	require.True(t, hasPermission)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckUserPermissionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "test-bucket"
	prefix := "test-prefix"
	operations := []string{"read", "write"}

	query := regexp.QuoteMeta(`
	SELECT EXISTS (
		SELECT 1
		FROM permissions,
			 UNNEST(allowed_s3_prefixes) AS allowed_prefix
		WHERE user_email = $1
		  AND operation = ANY($2)
		  AND $3 LIKE allowed_prefix || '%'
	);
	`)
	mock.ExpectQuery(query).
		WithArgs(userEmail, pq.Array(operations), "/"+bucket+"/"+prefix).
		WillReturnError(sql.ErrConnDone)

	hasPermission := pgDB.CheckUserPermission(userEmail, bucket, prefix, operations)
	require.False(t, hasPermission)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckUserPermissionNoAccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "restricted-bucket"
	prefix := "restricted-prefix"
	operations := []string{"read", "write"}

	query := regexp.QuoteMeta(`
	SELECT EXISTS (
		SELECT 1
		FROM permissions,
			 UNNEST(allowed_s3_prefixes) AS allowed_prefix
		WHERE user_email = $1
		  AND operation = ANY($2)
		  AND $3 LIKE allowed_prefix || '%'
	);
	`)
	mock.ExpectQuery(query).
		WithArgs(userEmail, pq.Array(operations), "/"+bucket+"/"+prefix).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	hasPermission := pgDB.CheckUserPermission(userEmail, bucket, prefix, operations)
	require.False(t, hasPermission)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUserAccessiblePrefixes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "test-bucket"
	operations := []string{"read", "write"}
	expectedPrefixes := []string{"/test-bucket/prefix1", "/test-bucket/prefix2"}

	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)
	rows := sqlmock.NewRows([]string{"allowed_prefix"}).
		AddRow(expectedPrefixes[0]).
		AddRow(expectedPrefixes[1])
	mock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket, pq.Array(operations)).
		WillReturnRows(rows)

	prefixes, err := pgDB.GetUserAccessiblePrefixes(userEmail, bucket, operations)
	require.NoError(t, err)
	require.Equal(t, expectedPrefixes, prefixes)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUserAccessiblePrefixesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "test-bucket"
	operations := []string{"read", "write"}

	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)
	mock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket, pq.Array(operations)).
		WillReturnError(sql.ErrConnDone)

	prefixes, err := pgDB.GetUserAccessiblePrefixes(userEmail, bucket, operations)
	require.Error(t, err)
	require.Nil(t, prefixes)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetUserAccessiblePrefixesNoAccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pgDB := &PostgresDB{Handle: db}

	userEmail := "test@example.com"
	bucket := "restricted-bucket"
	operations := []string{"read", "write"}

	query := regexp.QuoteMeta(`
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `)
	rows := sqlmock.NewRows([]string{"allowed_prefix"})
	mock.ExpectQuery(query).
		WithArgs(userEmail, "/"+bucket, pq.Array(operations)).
		WillReturnRows(rows)

	prefixes, err := pgDB.GetUserAccessiblePrefixes(userEmail, bucket, operations)
	require.NoError(t, err)
	require.Empty(t, prefixes)
	require.NoError(t, mock.ExpectationsWereMet())
}
