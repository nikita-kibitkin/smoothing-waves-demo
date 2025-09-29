CREATE TABLE IF NOT EXISTS events (
    id bigserial PRIMARY KEY,
    generated_at_ms bigint NOT NULL,
    payload_text text,
    saved_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_events_generated_at on events(generated_at_ms);
CREATE INDEX IF NOT EXISTS ix_events_payload on events(payload_text);
CREATE INDEX IF NOT EXISTS ix_events_saved_at on events(saved_at);

CREATE TABLE if NOT EXISTS events_audit (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL,
    created_at timestamptz DEFAULT now()
);

CREATE or replace function trg_events_audit() returns trigger
language plpgsql as $$
begin
insert into events_audit(event_id) values (NEW.id);
return NEW;
end $$;

drop trigger if exists events_audit_trg on events;
CREATE trigger events_audit_trg
    after insert on events
    for each row execute function trg_events_audit();
                         ^;



CREATE OR REPLACE FUNCTION delay_20ms() RETURNS TRIGGER AS $$
BEGIN
 -- PERFORM pg_sleep(0.005);
RETURN NEW;
END;
$$ LANGUAGE plpgsql
^;

DROP TRIGGER IF EXISTS insert_delay ON events
^;

CREATE TRIGGER insert_delay
    BEFORE INSERT OR UPDATE ON events
                         FOR EACH ROW EXECUTE FUNCTION delay_20ms()
                         ^;


