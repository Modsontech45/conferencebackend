const { Pool } = require('pg');

const ssl = process.env.DATABASE_URL?.includes('sslmode=require') ? { rejectUnauthorized: false } : false;
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl });

async function setup() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS meetings (
      id             TEXT        PRIMARY KEY,
      started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at       TIMESTAMPTZ,
      organizer_name TEXT
    );

    CREATE TABLE IF NOT EXISTS messages (
      id          SERIAL      PRIMARY KEY,
      meeting_id  TEXT        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
      sender_name TEXT        NOT NULL,
      text        TEXT        NOT NULL,
      sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS documents (
      meeting_id  TEXT        PRIMARY KEY REFERENCES meetings(id) ON DELETE CASCADE,
      title       TEXT        NOT NULL DEFAULT '',
      html        TEXT,
      version     INT         NOT NULL DEFAULT 0,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS messages_meeting_id_idx ON messages (meeting_id, sent_at);
  `);
}

async function ensureMeeting(id) {
  await pool.query(
    'INSERT INTO meetings (id) VALUES ($1) ON CONFLICT DO NOTHING',
    [id]
  );
}

async function setOrganizerName(meetingId, name) {
  await pool.query(
    'UPDATE meetings SET organizer_name = $1 WHERE id = $2 AND organizer_name IS NULL',
    [name, meetingId]
  );
}

async function closeMeeting(id) {
  await pool.query(
    'UPDATE meetings SET ended_at = NOW() WHERE id = $1 AND ended_at IS NULL',
    [id]
  );
}

async function saveMessage(meetingId, senderName, text) {
  await pool.query(
    'INSERT INTO messages (meeting_id, sender_name, text) VALUES ($1, $2, $3)',
    [meetingId, senderName, text]
  );
}

async function getMessages(meetingId, limit = 100) {
  const { rows } = await pool.query(
    `SELECT sender_name, text, sent_at
     FROM   messages
     WHERE  meeting_id = $1
     ORDER  BY sent_at DESC
     LIMIT  $2`,
    [meetingId, limit]
  );
  return rows.reverse().map((r) => ({
    id:   null,                            // no socket id for historical messages
    name: r.sender_name,
    text: r.text,
    ts:   new Date(r.sent_at).getTime(),
  }));
}

async function saveDocument(meetingId, title, html, version) {
  await pool.query(
    `INSERT INTO documents (meeting_id, title, html, version, updated_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (meeting_id) DO UPDATE
       SET title      = EXCLUDED.title,
           html       = EXCLUDED.html,
           version    = EXCLUDED.version,
           updated_at = NOW()`,
    [meetingId, title, html, version]
  );
}

module.exports = { setup, ensureMeeting, setOrganizerName, closeMeeting, saveMessage, getMessages, saveDocument };
