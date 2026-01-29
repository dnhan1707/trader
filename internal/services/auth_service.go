package services

import (
	"context"
	"database/sql"

	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	db *sql.DB
}

func NewAuthService(db *sql.DB) *AuthService {
	return &AuthService{db: db}
}

type User struct {
	ID           string
	Username     string
	PasswordHash string
}

func (s *AuthService) GetByUsername(ctx context.Context, username string) (*User, error) {
	var u User
	err := s.db.QueryRowContext(ctx,
		`SELECT id, username, password FROM users WHERE username = $1`,
		username,
	).Scan(&u.ID, &u.Username, &u.PasswordHash)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *AuthService) CreateUser(ctx context.Context, username, password string) (*User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	var u User
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO users (username, password)
         VALUES ($1, $2)
         RETURNING id, username, password`,
		username, string(hash),
	).Scan(&u.ID, &u.Username, &u.PasswordHash)
	if err != nil {
		return nil, err
	}

	return &u, nil
}

func (s *AuthService) CheckPassword(u *User, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password))
}
