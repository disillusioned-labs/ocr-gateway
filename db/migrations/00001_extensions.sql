-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    IF current_setting('server_version_num')::int >= 180000 THEN
        RETURN;
    END IF;

    EXECUTE $fn$
        CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid AS $body$
        DECLARE
            unix_ts_ms BYTEA;
            uuid_bytes BYTEA;
        BEGIN
            unix_ts_ms := substring(int8send((extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3);
            uuid_bytes := unix_ts_ms || substring(uuid_send(gen_random_uuid()) FROM 7 FOR 10);
            uuid_bytes := set_byte(uuid_bytes, 6, (b'0111' || get_byte(uuid_bytes, 6)::bit(4))::bit(8)::int);
            uuid_bytes := set_byte(uuid_bytes, 8, (b'10'   || get_byte(uuid_bytes, 8)::bit(6))::bit(8)::int);
            RETURN encode(uuid_bytes, 'hex')::uuid;
        END
        $body$ LANGUAGE plpgsql VOLATILE;
    $fn$;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF current_setting('server_version_num')::int < 180000 THEN
        DROP FUNCTION IF EXISTS uuidv7();
    END IF;
END
$$;
-- +goose StatementEnd
