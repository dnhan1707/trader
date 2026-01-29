package services

import (
	"context"
	"database/sql"
	"time"
)

type DMThread struct {
	ID        string
	User1ID   string
	User2ID   string
	CreatedAt time.Time
}

type DMMessage struct {
	ID        int64
	ThreadID  string
	SenderID  string
	Content   string
	CreatedAt time.Time
}

type DMThreadSummary struct {
	ThreadID           string
	OtherUserID        string
	OtherUsername      string
	LastMessageContent string
	LastMessageAt      time.Time
	UnreadCount        int
	HasMessages        bool
}

type UserSummary struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

type DMService struct {
	db *sql.DB
}

func NewDMService(db *sql.DB) *DMService {
	return &DMService{db: db}
}

func normalizePair(a, b string) (string, string) {
	if a < b {
		return a, b
	}
	return b, a
}

// SearchUsers returns users whose username matches q (case-insensitive), limited.
func (s *DMService) SearchUsers(ctx context.Context, q string, limit int) ([]UserSummary, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(ctx, `
        SELECT id, username
        FROM users
        WHERE username ILIKE '%' || $1 || '%'
        ORDER BY username
        LIMIT $2
    `, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []UserSummary
	for rows.Next() {
		var u UserSummary
		if err := rows.Scan(&u.ID, &u.Username); err != nil {
			return nil, err
		}
		res = append(res, u)
	}
	return res, rows.Err()
}

// ListThreadSummariesForUser returns threads for a user with other user + last message.
func (s *DMService) ListThreadSummariesForUser(ctx context.Context, userID string) ([]DMThreadSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
	   SELECT
	        t.id,
	        CASE WHEN t.user1_id = $1 THEN t.user2_id ELSE t.user1_id END AS other_user_id,
	        u.username AS other_username,
	        COALESCE(last_msg.content, '') AS last_message_content,
	        COALESCE(last_msg.created_at, t.created_at) AS last_message_at,
	        COALESCE(ur.unread_count, 0) AS unread_count,
	        (last_msg.id IS NOT NULL) AS has_messages
	    FROM dm_threads t
	    JOIN users u
	      ON u.id = CASE WHEN t.user1_id = $1 THEN t.user2_id ELSE t.user1_id END
	    LEFT JOIN LATERAL (
	        SELECT id, content, created_at
	        FROM dm_messages
	        WHERE thread_id = t.id
	        ORDER BY created_at DESC
	        LIMIT 1
	    ) last_msg ON TRUE
	    LEFT JOIN LATERAL (
	        SELECT COUNT(*) AS unread_count
	        FROM dm_messages msg
	        LEFT JOIN dm_thread_reads r
	          ON r.thread_id = msg.thread_id AND r.user_id = $1
	        WHERE msg.thread_id = t.id
	          AND (r.last_read_at IS NULL OR msg.created_at > r.last_read_at)
	          AND msg.sender_id <> $1
	    ) ur ON TRUE
	    WHERE t.user1_id = $1 OR t.user2_id = $1
	    ORDER BY last_message_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []DMThreadSummary
	for rows.Next() {
		var ssum DMThreadSummary
		if err := rows.Scan(
			&ssum.ThreadID,
			&ssum.OtherUserID,
			&ssum.OtherUsername,
			&ssum.LastMessageContent,
			&ssum.LastMessageAt,
			&ssum.UnreadCount,
			&ssum.HasMessages,
		); err != nil {
			return nil, err
		}
		res = append(res, ssum)
	}
	return res, rows.Err()
}

func (s *DMService) MarkThreadRead(ctx context.Context, userID, threadID string, t time.Time) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO dm_thread_reads (user_id, thread_id, last_read_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, thread_id)
        DO UPDATE SET last_read_at = EXCLUDED.last_read_at
    `, userID, threadID, t)
	return err
}

// Create a DM thread between 2 users if not yet exists
func (s *DMService) GetOrCreateThreadForUsers(ctx context.Context, user1ID, user2ID string) (*DMThread, error) {
	var thread DMThread

	u1, u2 := normalizePair(user1ID, user2ID)

	// try to find an existing thread for this pair
	err := s.db.QueryRowContext(ctx,
		`SELECT id, user1_id, user2_id, created_at
         FROM dm_threads
         WHERE user1_id = $1 AND user2_id = $2
         LIMIT 1`,
		u1, u2,
	).Scan(&thread.ID, &thread.User1ID, &thread.User2ID, &thread.CreatedAt)

	if err == nil {
		return &thread, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	// not found -> create new
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO dm_threads (user1_id, user2_id)
         VALUES ($1, $2)
         RETURNING id, user1_id, user2_id, created_at`,
		u1, u2,
	).Scan(&thread.ID, &thread.User1ID, &thread.User2ID, &thread.CreatedAt)
	if err != nil {
		return nil, err
	}

	return &thread, nil
}

func (s *DMService) CreateMessage(ctx context.Context, threadID, senderID, content string) (*DMMessage, error) {
	var m DMMessage
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO dm_messages (thread_id, sender_id, content)
         VALUES ($1, $2, $3)
         RETURNING id, thread_id, sender_id, content, created_at`,
		threadID, senderID, content,
	).Scan(&m.ID, &m.ThreadID, &m.SenderID, &m.Content, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *DMService) ListMessages(ctx context.Context, threadID string, limit int) ([]DMMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, thread_id, sender_id, content, created_at
         FROM dm_messages
         WHERE thread_id = $1
         ORDER BY created_at DESC
         LIMIT $2`,
		threadID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []DMMessage
	for rows.Next() {
		var m DMMessage
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.SenderID, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (s *DMService) ListThreadsForUser(ctx context.Context, userID string) ([]DMThread, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, user1_id, user2_id, created_at
         FROM dm_threads
         WHERE user1_id = $1 OR user2_id = $1
         ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var threads []DMThread
	for rows.Next() {
		var t DMThread
		if err := rows.Scan(&t.ID, &t.User1ID, &t.User2ID, &t.CreatedAt); err != nil {
			return nil, err
		}
		threads = append(threads, t)
	}
	return threads, rows.Err()
}

func (s *DMService) IsUserInThread(ctx context.Context, threadID, userID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (
            SELECT 1
            FROM dm_threads
            WHERE id = $1 AND (user1_id = $2 OR user2_id = $2)
        )`,
		threadID, userID,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
