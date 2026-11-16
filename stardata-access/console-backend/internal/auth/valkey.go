package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sessionTTL    = 24 * time.Hour
	sessionPrefix = "sess:"
)

// Session holds the authenticated user's data stored in Valkey.
type Session struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	Email       string    `json:"email"`
	Name        string    `json:"name"`
	Roles       []string  `json:"roles"`
	AccessToken string    `json:"access_token"`
	CreatedAt   time.Time `json:"created_at"`
}

// ValkeyStore manages sessions in Valkey (Redis-compatible).
type ValkeyStore struct {
	client *redis.Client
}

func NewValkey(addr, password string) (*ValkeyStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("valkey ping: %w", err)
	}
	return &ValkeyStore{client: client}, nil
}

// Create stores a new session and returns its ID.
func (s *ValkeyStore) Create(ctx context.Context, sess Session) (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	sess.ID = id
	sess.CreatedAt = time.Now().UTC()

	data, err := json.Marshal(sess)
	if err != nil {
		return "", fmt.Errorf("marshal session: %w", err)
	}
	if err := s.client.Set(ctx, sessionPrefix+id, data, sessionTTL).Err(); err != nil {
		return "", fmt.Errorf("valkey set: %w", err)
	}
	return id, nil
}

// Get retrieves a session by ID, refreshing its TTL.
func (s *ValkeyStore) Get(ctx context.Context, id string) (*Session, error) {
	data, err := s.client.GetEx(ctx, sessionPrefix+id, sessionTTL).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("valkey get: %w", err)
	}
	var sess Session
	if err := json.Unmarshal(data, &sess); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}
	return &sess, nil
}

// Delete removes a session.
func (s *ValkeyStore) Delete(ctx context.Context, id string) error {
	return s.client.Del(ctx, sessionPrefix+id).Err()
}

func randomID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
