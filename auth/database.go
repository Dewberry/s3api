package auth

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/labstack/gommon/log"
	"github.com/lib/pq"
)

// Database interface abstracts database operations
type Database interface {
	CheckUserPermission(userEmail, operation, s3_prefix string) bool
	CheckUserPermissionMultiple(userEmail, operation string, s3_prefixes []string) bool
	Close() error
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
		fmt.Println(err)
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

// CheckUserPermission checks if a user has permission for a specific request.
func (db *PostgresDB) CheckUserPermission(userEmail, operation, s3_prefix string) bool {
	query := `
	SELECT EXISTS (
		SELECT 1
		FROM permissions
		WHERE user_email = $1 AND operation = $2 AND $3 = ANY(allowed_s3_prefixes)
	);`

	var hasPermission bool
	if err := db.Handle.QueryRow(query, userEmail, operation, s3_prefix).Scan(&hasPermission); err != nil {
		log.Errorf("error querying user permissions: %v", err)
		return false
	}

	return hasPermission
}

// CheckUserPermissionMultiple checks if a user has permission for a specific request with multiple S3 prefixes.
func (db *PostgresDB) CheckUserPermissionMultiple(userEmail, operation string, s3_prefixes []string) bool {
	query := `
	SELECT EXISTS (
		SELECT 1
		FROM permissions
		WHERE user_email = $1 AND operation = $2 AND
		$3 <@ allowed_s3_prefixes
	);`

	var hasPermission bool
	if err := db.Handle.QueryRow(query, userEmail, operation, pq.Array(s3_prefixes)).Scan(&hasPermission); err != nil {
		log.Errorf("error querying user permissions: %v", err)
		return false
	}

	return hasPermission
}

// Close closes the database connection.
func (db *PostgresDB) Close() error {
	return db.Handle.Close()
}
