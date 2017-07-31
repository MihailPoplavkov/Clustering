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
  DROP TABLE IF EXISTS links;
  DROP TABLE IF EXISTS links_cluster;
  DROP TABLE IF EXISTS clusters;
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

CREATE OR REPLACE FUNCTION create_cluster_tables()
  RETURNS VOID AS $$
BEGIN
  CREATE TABLE clusters (
    cluster TEXT
  );

  CREATE TABLE links_count
  AS
  SELECT
  query,
  SUM(count) count
  FROM links
  GROUP BY query;


  CREATE TABLE links_cluster
AS
  SELECT
    l1.query                                                      q1,
    l2.query                                                      q2,
    c1.count                                                      cou1,
    c2.count                                                      cou2,
    1.0 * (SUM(l1.count) + SUM(l2.count)) / (c1.count + c2.count) res
  FROM links l1
    INNER JOIN links l2 ON l1.query < l2.query
    INNER JOIN links_count c1 ON l1.query = c1.query
    INNER JOIN links_count c2 ON l2.query = c2.query
  WHERE l1.document = l2.document
  GROUP BY l1.query, l2.query, c1.count, c2.count;

  DROP TABLE links_count;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION combine_first(threshold DOUBLE PRECISION)
  RETURNS TEXT AS $$
DECLARE   query1     TEXT;
  DECLARE query2     TEXT;
  DECLARE allCount   INTEGER;
  DECLARE currentRes DOUBLE PRECISION;
  DECLARE concat     TEXT;
BEGIN
  SELECT
    q1,
    q2,
    cou1 + cou2,
    res
  INTO query1, query2, allCount, currentRes
  FROM links_cluster
  ORDER BY res DESC, cou1 DESC, cou2 DESC
  LIMIT 1;

  IF currentRes IS NULL OR currentRes < threshold
  THEN
    RETURN '';
  END IF;

  concat = query1 || ';' || query2;

  CREATE TABLE tmp_links AS SELECT
  q1,
  q2,
  cou1,
  cou2,
  res
  FROM links_cluster
  WHERE q1 IN (query1, query2);

  INSERT INTO tmp_links
  SELECT
  q2,
  q1,
  cou2,
  cou1,
  res
  FROM links_cluster
  WHERE q2 IN (query1, query2);

  DELETE FROM links_cluster
  WHERE q1 IN (query1, query2) OR q2 IN (query1, query2);

  DELETE FROM tmp_links
  WHERE (q1 = query1 AND q2 = query2)
    OR (q2 = query1 AND q1 = query2);

  UPDATE tmp_links
  SET q1 = concat;

  INSERT INTO links_cluster
    SELECT
      q1,
      q2,
      allCount,
      cou2,
      SUM((cou1 + cou2) * res) / (allCount + cou2)
    FROM tmp_links
    GROUP BY q1, q2, cou2;

  DELETE FROM clusters
    WHERE cluster IN (query1, query2);
  INSERT INTO clusters (cluster)
    VALUES (concat);

  DROP TABLE tmp_links;

  RETURN concat;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION select_clusters()
  RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY SELECT cluster
               FROM clusters;
END;
$$ LANGUAGE plpgsql STABLE;