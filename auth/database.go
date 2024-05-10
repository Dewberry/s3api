package auth

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/labstack/gommon/log"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// Database interface abstracts database operations
type Database interface {
	CheckUserPermission(userEmail, bucket, prefix string, operations []string) bool
	Close() error
	GetUserAccessiblePrefixes(userEmail, bucket string, operations []string) ([]string, error)
}

type PostgresDB struct {
	Handle *sql.DB
}

// Initialize the database and create tables if they do not exist.
func NewPostgresDB() (*PostgresDB, error) {
	connString, exist := os.LookupEnv("POSTGRES_CONN_STRING")
	if !exist {
		return nil, fmt.Errorf("env variable POSTGRES_CONN_STRING not set")
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %v", err)
	}

	pgDB := &PostgresDB{Handle: db}

	// Create tables
	if err := pgDB.createTables(); err != nil {
		return nil, err
	}

	return pgDB, nil
}

// Creates the necessary tables in the database.
func (db *PostgresDB) createTables() error {
	createPermissionsTable := `
	CREATE TABLE IF NOT EXISTS permissions (
		id SERIAL PRIMARY KEY,
		user_email TEXT NOT NULL,
		operation TEXT NOT NULL,
		allowed_s3_prefixes TEXT[] NOT NULL
	);

	-- All our queries will involve checking paths allowed for a specific combination of
	-- user and operation, a composite index is more beneficial for this use case
    CREATE INDEX IF NOT EXISTS idx_permissions_user_email_operation ON permissions(user_email, operation);
	`

	if _, err := db.Handle.Exec(createPermissionsTable); err != nil {
		return fmt.Errorf("error creating permissions table: %v", err)
	}

	return nil
}

func (db *PostgresDB) GetUserAccessiblePrefixes(userEmail, bucket string, operations []string) ([]string, error) {
	query := `
        WITH unnested_permissions AS (
            SELECT DISTINCT unnest(allowed_s3_prefixes) AS allowed_prefix
            FROM permissions
            WHERE user_email = $1 AND operation = ANY($3)
        )
        SELECT allowed_prefix
        FROM unnested_permissions
        WHERE allowed_prefix LIKE $2 || '/%'
        ORDER BY allowed_prefix;
    `

	rows, err := db.Handle.Query(query, userEmail, "/"+bucket, pq.Array(operations))
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}
	defer rows.Close()

	var prefixes []string
	var prefix string
	for rows.Next() {
		if err := rows.Scan(&prefix); err != nil {
			return nil, fmt.Errorf("scan error: %s", err)
		}
		prefixes = append(prefixes, prefix)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row error: %s", err)
	}

	return prefixes, nil
}

// CheckUserPermission checks if a user has permission for a specific request.
func (db *PostgresDB) CheckUserPermission(userEmail, bucket, prefix string, operations []string) bool {
	s3_prefix := fmt.Sprintf("/%s/%s", bucket, prefix)
	query := `
	SELECT EXISTS (
		SELECT 1
		FROM permissions,
			 UNNEST(allowed_s3_prefixes) AS allowed_prefix
		WHERE user_email = $1
		  AND operation = ANY($2)
		  AND $3 LIKE allowed_prefix || '%'
	);
	`

	var hasPermission bool
	if err := db.Handle.QueryRow(query, userEmail, pq.Array(operations), s3_prefix).Scan(&hasPermission); err != nil {
		log.Errorf("error querying user permissions: %v", err)
		return false
	}

	return hasPermission
}

// Close closes the database connection.
func (db *PostgresDB) Close() error {
	return db.Handle.Close()
}
