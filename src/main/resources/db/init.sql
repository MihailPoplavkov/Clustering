--Инициализация БД, создание нужных таблиц
CREATE OR REPLACE FUNCTION create_tables() RETURNS VOID AS $$
BEGIN
  CREATE TABLE links (
    query    TEXT,
    document TEXT,
    count    INTEGER
  );
  CREATE TABLE links_cluster (
    q1 TEXT,
    q2 TEXT,
    cou1 INTEGER,
    cou2 INTEGER,
    res REAL
  );
  CREATE TABLE links_count (
    query TEXT,
    count INTEGER
  );
  CREATE TABLE clusters (
    cluster TEXT
  );
  CREATE TABLE pre_clust (
    query TEXT,
    document TEXT,
    count INTEGER
  );
  CREATE TABLE tmp (
    query TEXT,
    document TEXT,
    count INTEGER
  );
  CREATE TABLE docs (
    document TEXT
  );
  CREATE TABLE queries (
    query TEXT
  );
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION drop_tables() RETURNS VOID AS $$
BEGIN
  DROP TABLE IF EXISTS links;
  DROP TABLE IF EXISTS links_cluster;
  DROP TABLE IF EXISTS links_count;
  DROP TABLE IF EXISTS clusters;
  DROP TABLE IF EXISTS pre_clust;
  DROP TABLE IF EXISTS tmp;
  DROP TABLE IF EXISTS docs;
  DROP TABLE IF EXISTS queries;
  DROP TABLE IF EXISTS clusters;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION insert_line(q TEXT, doc TEXT, cou INTEGER) RETURNS VOID AS $$
BEGIN
  INSERT INTO links (query, document, count) VALUES (q, doc, cou);
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION select_queries(doc TEXT) RETURNS TABLE(q TEXT, cou INTEGER) AS $$
BEGIN
  RETURN QUERY SELECT query, count FROM links WHERE document = doc;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION select_documents(q TEXT) RETURNS TABLE(doc TEXT, cou INTEGER) AS $$
BEGIN
  RETURN QUERY SELECT document, count FROM links WHERE query = q;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION compact_links() RETURNS VOID AS $$
BEGIN
-- замена всех URL, встречающихся только у одного запроса на 'other', так как
-- единственный их смысл - увеличить общее количество документов для данного
-- запроса
  UPDATE links SET document = 'other'
    WHERE document IN (
      SELECT document FROM links GROUP BY document HAVING COUNT(DISTINCT query) = 1
    );
-- непосредственно сжатие
  CREATE TABLE links_compact AS
     SELECT query, document, CAST(SUM(count) AS INTEGER) AS count
     FROM links
     GROUP BY query, document;
  DROP TABLE links;
  ALTER TABLE links_compact RENAME TO links;
-- создание таблицы, состоящей из картежей исходной таблицы links, содержащей
-- только запросы, с которых есть ссылки только на один URL
  CREATE TABLE with_one_document AS
    WITH moved_rows AS (
      DELETE FROM links WHERE query IN (
        SELECT query FROM links GROUP BY query HAVING COUNT(*) = 1
      )
      RETURNING *
    )
    SELECT * FROM moved_rows;
-- если в этой таблице оказались запросы, все URL которых были признаны как
-- 'other', значит нет более ни одного запроса, имеющего с такими хоть один
-- общий URL, следовательно, они не войдут ни в один кластер
  DELETE FROM with_one_document WHERE document = 'other';
-- создание кластеров из запросов, схожих друг с другом на 100%, так как
-- имеют только по одному URL, который у них совпадает. Так же вставка в
-- таблицу clusters образовавшихся кластеров
  WITH inserted_rows AS (
  INSERT INTO links (query, document, count)
    SELECT string_agg(query, ';'), document, SUM(count)
    FROM with_one_document
    GROUP BY document
  RETURNING *
  ) INSERT INTO clusters
      SELECT query FROM inserted_rows
        WHERE query LIKE '%;%';

  DROP TABLE with_one_document;
-- повторение процедуры. Полезно в том случае, если были объединены единственные
-- запросы, ссылающиеся на какой-либо документ
  UPDATE links SET document = 'other'
    WHERE document IN (
      SELECT document FROM links GROUP BY document HAVING COUNT(DISTINCT query) = 1
    );
END;
$$ LANGUAGE plpgsql VOLATILE;

-- создание таблицы, содержащей только те строки, которые теоретически могли бы
-- оказаться в одном кластере, т.е. те строки, которые были бы в одном кластере,
-- если б был задан порог (threshold) = 0. Возвращает количество строк, отнесенных
-- к пре-кластеру
CREATE OR REPLACE FUNCTION pre_cluster() RETURNS INTEGER AS $$
  DECLARE largest TEXT;
  DECLARE cou INTEGER;
BEGIN
-- занесения в переменную largest запроса, имеющего наибольшее количество ссылок
-- на различные URL
  SELECT query INTO largest FROM links
    GROUP BY query ORDER BY COUNT(*) DESC
    LIMIT 1;

-- в промежутке между таблицами links и pre_cluster данные будут храниться в
-- таблице tmp. Тут удаляется запись из таблицы links и переносится в tmp
  WITH moved_rows AS (
    DELETE FROM links
      WHERE query = largest
      RETURNING *
  )
  INSERT INTO tmp
    SELECT * FROM moved_rows;

  WHILE TRUE LOOP
-- занесение в таблицу docs всех уникальных URL из таблицы tmp
    INSERT INTO docs
      SELECT DISTINCT document FROM tmp
        WHERE document != 'other';
-- перенесение записей из tmp в pre_clust
    WITH moved_rows AS (
      DELETE FROM tmp
      RETURNING *
    )
    INSERT INTO pre_clust
      SELECT * FROM moved_rows;
-- выборка из таблицы links тех записей, которые ссылаются на URL из таблицы docs
-- и перенесение их в tmp
    WITH moved_rows AS (
      DELETE FROM links
        WHERE document IN (SELECT * FROM docs)
        RETURNING *
    )
    INSERT INTO tmp
      SELECT * FROM moved_rows;

    TRUNCATE docs;
-- теперь то же самое, только с запросами. Выборка в таблицу queries уникальных
-- запросов из таблицы tmp
    INSERT INTO queries
      SELECT DISTINCT query FROM tmp;
-- перенесение строк из tmp в pre_clust
    WITH moved_rows AS (
      DELETE FROM tmp
      RETURNING *
    )
    INSERT INTO pre_clust
      SELECT * FROM moved_rows;
-- выборка из таблицы links тех записей с именами из таблицы queries и перенесение
-- их в tmp
    WITH moved_rows AS (
      DELETE FROM links
        WHERE query IN (SELECT * FROM queries)
        RETURNING *
    )
    INSERT INTO tmp
      SELECT * FROM moved_rows;

    TRUNCATE queries;
-- условие выхода из вечного цикла - если на очередной итерации в таблицу tmp не
-- было выбрано ни одной записи
    SELECT COUNT(*) INTO cou FROM tmp;
    IF cou = 0 THEN
      SELECT COUNT(*) INTO cou
        FROM pre_clust;
      RETURN cou;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- основная задача этой функции - создать таблицу links_cluster -
-- практически декартово произведение таблицы links, соединяющее строки,
-- имеющие одинаковый URL
CREATE OR REPLACE FUNCTION create_cluster_tables() RETURNS VOID AS $$
BEGIN
  TRUNCATE TABLE links_cluster;
-- таблица, содержащая сумму всех ссылок с каждого запроса
  INSERT INTO links_count
    SELECT query, SUM(count)
    FROM pre_clust
    GROUP BY query;
-- создание большой таблицы
-- q1 - первый запрос, cou1 - сумма ссылок с этого запроса
-- q2 - второй запрос, cou2 - сумма ссылок с этого запроса
-- res - результат, который дает для них функция схожести
  INSERT INTO links_cluster
    SELECT l1.query q1, l2.query q2, c1.count cou1, c2.count cou2,
      1.0 * (SUM(l1.count) + SUM(l2.count)) / (c1.count + c2.count) res
    FROM pre_clust l1
      INNER JOIN pre_clust l2 ON l1.query < l2.query
      INNER JOIN links_count c1 ON l1.query = c1.query
      INNER JOIN links_count c2 ON l2.query = c2.query
    WHERE l1.document != 'other' AND l1.document = l2.document
    GROUP BY l1.query, l2.query, c1.count, c2.count;

  TRUNCATE TABLE links_count;
  TRUNCATE TABLE pre_clust;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- создание кластеров на основе данных, содержащихся в таблице links_cluster
-- и запись их в таблицу clusters. Возвращает количество созданных кластеров
CREATE OR REPLACE FUNCTION combine_all(threshold REAL) RETURNS INTEGER AS $$
  DECLARE query1     TEXT;
  DECLARE query2     TEXT;
  DECLARE allCount   INTEGER;
  DECLARE currentRes REAL;
  DECLARE concat     TEXT;
  DECLARE clustCount INTEGER;
BEGIN
-- для отчетности, чтобы в конце замерить разницу между количеством кластеров
-- до выполнения функции и после
  SELECT COUNT(*) INTO clustCount FROM clusters;
  CREATE TABLE tmp_links (
    q1 TEXT,
    q2 TEXT,
    cou1 INTEGER,
    cou2 INTEGER,
    res REAL
  );
-- переменные query1 и query2 будут хранить текущие обрабатывающиеся запросы,
-- allCount - общее количество ссылок с этих запросов и currentRes - результат,
-- который дала функция схожести для этих запросов
  SELECT q1, q2, cou1 + cou2, res
    INTO query1, query2, allCount, currentRes
    FROM links_cluster
    ORDER BY res DESC, cou1 DESC, cou2 DESC LIMIT 1;
-- цикл до тех пор, пока в таблице не останется записей, либо максимальное
-- значение функции схожести для оставшихся записей не будет превышать
-- threshold
  WHILE NOT (currentRes IS NULL OR currentRes < threshold) LOOP
-- concat - имя нового кластера, состоящее из двух запросов (кластеров),
-- соединенных ';'
    concat = query1 || ';' || query2;
-- переносим все записи из таблицы links_cluster, содержащие query1 или
-- query2 в таблицу tmp_links (это делается в два запроса)
    WITH moved_rows AS (
      DELETE FROM links_cluster
      WHERE q1 IN (query1, query2)
    RETURNING *
    )
    INSERT INTO tmp_links
      SELECT q1, q2, cou1, cou2, res FROM moved_rows;
-- в таблице tmp_links в качестве q1 всегда будет один из запросов query1 или
-- query2, а в качестве q2 - запросы, связанные с этими
    WITH moved_rows AS (
      DELETE FROM links_cluster
      WHERE q2 IN (query1, query2)
    RETURNING *
    )
    INSERT INTO tmp_links
      SELECT q2, q1, cou2, cou1, res FROM moved_rows;

    DELETE FROM tmp_links
      WHERE (q1 = query1 AND q2 = query2) OR (q2 = query1 AND q1 = query2);
    UPDATE tmp_links SET q1 = concat;
-- высчитывание нового результата функции схожести и перенос данных обратно
-- из tmp_links в links_cluster
    INSERT INTO links_cluster
      SELECT q1, q2, allCount, cou2, SUM((cou1 + cou2) * res) / (allCount + cou2)
      FROM tmp_links
      GROUP BY q1, q2, cou2;
-- занесение записи о новом сформировавшемся кластере в таблицу clusters,
-- предварительно удалив перед этим записи о query1 и query2, если они там
-- были (ведь query1 или query2 могут оказаться кластерами, сформировавшимися
-- на предыдущих шагах)
    DELETE FROM clusters WHERE cluster IN (query1, query2);
    INSERT INTO clusters (cluster) VALUES (concat);
    TRUNCATE TABLE tmp_links;
-- выборка следующей пары наиболее похожих запросов
    SELECT q1, q2, cou1 + cou2, res
      INTO query1, query2, allCount, currentRes
      FROM links_cluster
      ORDER BY res DESC, cou1 DESC, cou2 DESC LIMIT 1;
  END LOOP;
  DROP TABLE tmp_links;
  SELECT COUNT(*) - clustCount INTO clustCount FROM clusters;
  RETURN clustCount;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION select_clusters() RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY SELECT cluster FROM clusters;
END;
$$ LANGUAGE plpgsql STABLE;