-- The human-readable ABI declaration is needed only while creating a monitor.
-- Runtime matching uses the fixed-size function selector or event topic0, and
-- decoding uses param_schema.
ALTER TABLE monitors DROP COLUMN signature;
