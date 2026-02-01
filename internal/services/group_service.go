package services

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
)

type GroupThread struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatorID string    `json:"creatorId"`
	CreatedAt time.Time `json:"createdAt"`
}

type GroupMessage struct {
	ID        int64     `json:"id"`
	ThreadID  string    `json:"threadId"`
	SenderID  string    `json:"senderId"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt"`
}

type GroupMember struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

type GroupThreadSummary struct {
	ThreadID           string        `json:"threadId"`
	Name               string        `json:"name"`
	Members            []GroupMember `json:"members"`
	LastMessageContent string        `json:"lastMessageContent"`
	LastMessageAt      time.Time     `json:"lastMessageAt"`
	UnreadCount        int           `json:"unreadCount"`
	HasMessages        bool          `json:"hasMessages"`
}

type GroupService struct {
	db *sql.DB
}

func NewGroupService(db *sql.DB) *GroupService {
	return &GroupService{db: db}
}

// CreateGroupThread creates a new group thread with the given members.
// The creator is automatically added as a member.
func (s *GroupService) CreateGroupThread(ctx context.Context, name, creatorID string, memberIDs []string) (*GroupThread, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Create the group thread
	var thread GroupThread
	err = tx.QueryRowContext(ctx,
		`INSERT INTO group_threads (name, creator_id)
         VALUES ($1, $2)
         RETURNING id, name, creator_id, created_at`,
		name, creatorID,
	).Scan(&thread.ID, &thread.Name, &thread.CreatorID, &thread.CreatedAt)
	if err != nil {
		return nil, err
	}

	// Add creator as a member
	_, err = tx.ExecContext(ctx,
		`INSERT INTO group_thread_members (thread_id, user_id)
         VALUES ($1, $2)`,
		thread.ID, creatorID,
	)
	if err != nil {
		return nil, err
	}

	// Add other members
	for _, memberID := range memberIDs {
		if memberID == creatorID {
			continue // Skip creator, already added
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO group_thread_members (thread_id, user_id)
             VALUES ($1, $2)
             ON CONFLICT DO NOTHING`,
			thread.ID, memberID,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &thread, nil
}

// GetGroupThread returns the group thread by ID.
func (s *GroupService) GetGroupThread(ctx context.Context, threadID string) (*GroupThread, error) {
	var thread GroupThread
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, creator_id, created_at
         FROM group_threads
         WHERE id = $1`,
		threadID,
	).Scan(&thread.ID, &thread.Name, &thread.CreatorID, &thread.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &thread, nil
}

// GetGroupMembers returns all members of a group thread.
func (s *GroupService) GetGroupMembers(ctx context.Context, threadID string) ([]GroupMember, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT u.id, u.username
         FROM group_thread_members m
         JOIN users u ON u.id = m.user_id
         WHERE m.thread_id = $1
         ORDER BY m.joined_at`,
		threadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []GroupMember
	for rows.Next() {
		var m GroupMember
		if err := rows.Scan(&m.UserID, &m.Username); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

// GetGroupMemberIDs returns just the user IDs of all members.
func (s *GroupService) GetGroupMemberIDs(ctx context.Context, threadID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT user_id FROM group_thread_members WHERE thread_id = $1`,
		threadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// IsUserInGroupThread checks if a user is a member of the group thread.
func (s *GroupService) IsUserInGroupThread(ctx context.Context, threadID, userID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (
            SELECT 1 FROM group_thread_members
            WHERE thread_id = $1 AND user_id = $2
        )`,
		threadID, userID,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ListGroupThreadsForUser returns all group threads the user is a member of.
func (s *GroupService) ListGroupThreadsForUser(ctx context.Context, userID string) ([]GroupThreadSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT
            t.id,
            t.name,
            COALESCE(last_msg.content, '') AS last_message_content,
            COALESCE(last_msg.created_at, t.created_at) AS last_message_at,
            COALESCE(ur.unread_count, 0) AS unread_count,
            (last_msg.id IS NOT NULL) AS has_messages
        FROM group_threads t
        JOIN group_thread_members m ON m.thread_id = t.id AND m.user_id = $1
        LEFT JOIN LATERAL (
            SELECT id, content, created_at
            FROM group_messages
            WHERE thread_id = t.id
            ORDER BY created_at DESC
            LIMIT 1
        ) last_msg ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS unread_count
            FROM group_messages msg
            LEFT JOIN group_thread_reads r
              ON r.thread_id = msg.thread_id AND r.user_id = $1
            WHERE msg.thread_id = t.id
              AND (r.last_read_at IS NULL OR msg.created_at > r.last_read_at)
              AND msg.sender_id <> $1
        ) ur ON TRUE
        ORDER BY last_message_at DESC
    `, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []GroupThreadSummary
	for rows.Next() {
		var s GroupThreadSummary
		if err := rows.Scan(
			&s.ThreadID,
			&s.Name,
			&s.LastMessageContent,
			&s.LastMessageAt,
			&s.UnreadCount,
			&s.HasMessages,
		); err != nil {
			return nil, err
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Fetch members for each thread
	for i := range summaries {
		members, err := s.GetGroupMembers(ctx, summaries[i].ThreadID)
		if err != nil {
			return nil, err
		}
		summaries[i].Members = members
	}

	return summaries, nil
}

// CreateGroupMessage creates a new message in a group thread.
func (s *GroupService) CreateGroupMessage(ctx context.Context, threadID, senderID, content string) (*GroupMessage, error) {
	var m GroupMessage
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO group_messages (thread_id, sender_id, content)
         VALUES ($1, $2, $3)
         RETURNING id, thread_id, sender_id, content, created_at`,
		threadID, senderID, content,
	).Scan(&m.ID, &m.ThreadID, &m.SenderID, &m.Content, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// ListGroupMessages returns messages from a group thread.
func (s *GroupService) ListGroupMessages(ctx context.Context, threadID string, limit int) ([]GroupMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, thread_id, sender_id, content, created_at
         FROM group_messages
         WHERE thread_id = $1
         ORDER BY created_at DESC
         LIMIT $2`,
		threadID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []GroupMessage
	for rows.Next() {
		var m GroupMessage
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.SenderID, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// MarkGroupThreadRead marks the group thread as read for the user.
func (s *GroupService) MarkGroupThreadRead(ctx context.Context, userID, threadID string, t time.Time) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO group_thread_reads (user_id, thread_id, last_read_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, thread_id)
        DO UPDATE SET last_read_at = EXCLUDED.last_read_at
    `, userID, threadID, t)
	return err
}

// AddMembersToGroup adds new members to an existing group thread.
func (s *GroupService) AddMembersToGroup(ctx context.Context, threadID string, memberIDs []string) error {
	if len(memberIDs) == 0 {
		return nil
	}

	// Use a single query with unnest for efficiency
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO group_thread_members (thread_id, user_id)
         SELECT $1, unnest($2::uuid[])
         ON CONFLICT DO NOTHING`,
		threadID, pq.Array(memberIDs),
	)
	return err
}

// RemoveMemberFromGroup removes a member from the group thread.
func (s *GroupService) RemoveMemberFromGroup(ctx context.Context, threadID, userID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM group_thread_members
         WHERE thread_id = $1 AND user_id = $2`,
		threadID, userID,
	)
	return err
}

// GetUsernameByID returns the username for a given user ID.
func (s *GroupService) GetUsernameByID(ctx context.Context, userID string) (string, error) {
	var username string
	err := s.db.QueryRowContext(ctx,
		`SELECT username FROM users WHERE id = $1`,
		userID,
	).Scan(&username)
	if err != nil {
		return "", err
	}
	return username, nil
}

// GetUsernamesByIDs returns usernames for multiple user IDs.
func (s *GroupService) GetUsernamesByIDs(ctx context.Context, userIDs []string) (map[string]string, error) {
	if len(userIDs) == 0 {
		return map[string]string{}, nil
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, username FROM users WHERE id = ANY($1)`,
		pq.Array(userIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var id, username string
		if err := rows.Scan(&id, &username); err != nil {
			return nil, err
		}
		result[id] = username
	}
	return result, rows.Err()
}
