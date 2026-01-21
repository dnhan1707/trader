CREATE TABLE IF NOT EXISTS dm_threads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user1_id UUID NOT NULL REFERENCES users(id),
    user2_id UUID NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS dm_messages (
    id BIGSERIAL PRIMARY KEY,
    thread_id UUID NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
    sender_id UUID NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dm_thread_reads (
    user_id    UUID NOT NULL REFERENCES users(id),
    thread_id  UUID NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
    last_read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_dm_messages_thread_created
    ON dm_messages (thread_id, created_at DESC);