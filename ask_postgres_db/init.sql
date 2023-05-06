CREATE TABLE users
(
    user_name       VARCHAR NOT NULL,
    hashed_password TEXT NOT NULL,
    email           VARCHAR,
    full_name       VARCHAR,
    descriptions    TEXT,
    active          TEXT NOT NULL,
    apps            VARCHAR[],
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

ALTER TABLE users OWNER TO my_postgres;

CREATE UNIQUE INDEX users_username_uindex
    ON users (user_name);

ALTER TABLE users ADD CONSTRAINT users_pk PRIMARY KEY (user_name);



CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();
