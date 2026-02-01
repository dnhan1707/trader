-- Group chat tables (extends the existing DM chat system)

-- Group threads table
CREATE TABLE IF NOT EXISTS group_threads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    creator_id UUID NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Group thread members (many-to-many relationship)
CREATE TABLE IF NOT EXISTS group_thread_members (
    thread_id UUID NOT NULL REFERENCES group_threads(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id),
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, user_id)
);

-- Group messages table
CREATE TABLE IF NOT EXISTS group_messages (
    id BIGSERIAL PRIMARY KEY,
    thread_id UUID NOT NULL REFERENCES group_threads(id) ON DELETE CASCADE,
    sender_id UUID NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Group thread read tracking
CREATE TABLE IF NOT EXISTS group_thread_reads (
    user_id UUID NOT NULL REFERENCES users(id),
    thread_id UUID NOT NULL REFERENCES group_threads(id) ON DELETE CASCADE,
    last_read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, thread_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_group_thread_members_user
    ON group_thread_members (user_id);

CREATE INDEX IF NOT EXISTS idx_group_messages_thread_created
    ON group_messages (thread_id, created_at DESC);
