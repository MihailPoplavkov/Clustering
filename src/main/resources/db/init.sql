CREATE OR REPLACE FUNCTION create_tables()
  RETURNS VOID AS $$
BEGIN
  EXECUTE
  'CREATE TABLE links (
    query    TEXT,
    document TEXT,
    count    INTEGER
  );';
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION drop_tables()
  RETURNS VOID AS $$
BEGIN
  EXECUTE
  'DROP TABLE IF EXISTS links;';
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION insert_line(q TEXT, doc TEXT, cou INTEGER)
  RETURNS VOID AS $$
BEGIN
  INSERT INTO links (query, document, count) VALUES (q, doc, cou);
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION select_queries(doc TEXT)
  RETURNS TABLE(q TEXT, cou INTEGER) AS $$
BEGIN
  RETURN QUERY SELECT
                 query,
                 count
               FROM links
               WHERE document = doc;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION select_documents(q TEXT)
  RETURNS TABLE(doc TEXT, cou INTEGER) AS $$
BEGIN
  RETURN QUERY SELECT
                 document,
                 count
               FROM links
               WHERE query = q;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION compact_links()
  RETURNS VOID AS $$
BEGIN
  EXECUTE
  'CREATE TABLE links_compact
   AS
     SELECT
       query,
       document,
       CAST(SUM(count) AS INTEGER) AS count
     FROM links
     GROUP BY query, document;';
  DROP TABLE links;
  ALTER TABLE links_compact RENAME TO links;
END;
$$ LANGUAGE plpgsql VOLATILE;
