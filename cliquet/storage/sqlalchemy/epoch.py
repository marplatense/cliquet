from pyramid_sqlalchemy import metadata
from sqlalchemy import event
from sqlalchemy.schema import DDL


AS_EPOCH = """
CREATE OR REPLACE FUNCTION as_epoch(ts TIMESTAMP) RETURNS BIGINT AS $$
  BEGIN
    RETURN (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT;
  END;
$$ LANGUAGE plpgsql
IMMUTABLE;
"""

FROM_EPOCH = """
CREATE OR REPLACE FUNCTION from_epoch(epoch BIGINT) RETURNS TIMESTAMP AS $$
BEGIN
    RETURN TIMESTAMP WITH TIME ZONE 'epoch' + epoch * INTERVAL '1 millisecond';
END;
$$ LANGUAGE plpgsql
IMMUTABLE;
"""

###
## Helper that returns the current collection timestamp.
###

COLLECTION_TIMESTAMP = """
CREATE OR REPLACE FUNCTION collection_timestamp(uid VARCHAR, resource VARCHAR)
RETURNS TIMESTAMP AS $$
DECLARE
    ts TIMESTAMP;
BEGIN
    ts := NULL;

    SELECT last_modified INTO ts
      FROM timestamps
     WHERE parent_id = uid
       AND collection_id = resource;

    IF ts IS NULL THEN
      ts := clock_timestamp();
      INSERT INTO timestamps (parent_id, collection_id, last_modified)
      VALUES (uid, resource, ts);
    END IF;

    RETURN ts;
END;
$$ LANGUAGE plpgsql;
"""

event.listen(metadata, "after_create", DDL(AS_EPOCH).execute_if(dialect='postgresql'))
event.listen(metadata, "after_create", DDL(FROM_EPOCH).execute_if(dialect='postgresql'))
event.listen(metadata, "after_create", DDL(COLLECTION_TIMESTAMP).execute_if(dialect='postgresql'))
