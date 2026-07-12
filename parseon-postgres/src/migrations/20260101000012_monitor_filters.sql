ALTER TABLE monitors
  ADD COLUMN filter_ast JSONB,
  ADD COLUMN filter_version SMALLINT,
  ADD CONSTRAINT monitors_filter_pair CHECK (
    (filter_ast IS NULL AND filter_version IS NULL)
    OR (
      filter_ast IS NOT NULL
      AND jsonb_typeof(filter_ast) = 'object'
      AND filter_version = 1
    )
  );
