package ru.poplavkov.cluster;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cover all interactions with database. It's used to store some data
 * to or get some data from database.
 *
 * @author Mihail Poplavkov
 * @see Connection
 */
@Log4j2
class Store implements AutoCloseable {
    /**
     * Needful SQL queries that used to interact with database.
     */
    private static final String CREATE_TABLES = "SELECT create_tables()";
    private static final String DROP_TABLES = "SELECT drop_tables()";
    private static final String INSERT_INTO_LINKS = "SELECT insert_line(?, ?, ?)";
    private static final String SELECT_QUERIES = "SELECT q, cou FROM select_queries(?)";
    private static final String SELECT_DOCUMENTS = "SELECT doc, cou FROM select_documents(?)";
    private static final String CREATE_PRE_CLUSTER = "SELECT * FROM pre_cluster()";
    private static final String COMPACT_LINKS = "SELECT compact_links()";
    private static final String CREATE_CLUSTER_TABLES = "SELECT create_cluster_tables()";
    private static final String COMBINE_ALL = "SELECT * FROM combine_all(?)";
    private static final String SELECT_CLUSTERS = "SELECT * FROM select_clusters()";

    /**
     * Path to the directory containing config file {@code db.properties}
     * and SQL scripts, needed to interact with database.
     * <p>
     * <p>{@code db.properties} file contains data to configure connection
     * to database.
     */
    private String pathToConfig;

    /**
     * A c3po connection pool.
     *
     * @see <a href="http://www.mchange.com/projects/c3p0/">c3po</a>
     * @see ComboPooledDataSource
     */
    private ComboPooledDataSource cpds;

    /**
     * Constructs Store using specified {@code pathToConfig}. Tunes the
     * connection pool.
     *
     * @param pathToConfig path to the directory containing config file
     *                     {@code db.properties} and SQL scripts, needed
     *                     to interact with database
     */
    @SneakyThrows
    Store(String pathToConfig) {
        this.pathToConfig = pathToConfig;
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToConfig + "/db.properties"));

        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(properties.getProperty("driver"));
        cpds.setJdbcUrl(properties.getProperty("url"));
        cpds.setUser(properties.getProperty("user"));
        cpds.setPassword(properties.getProperty("password"));
        int minPoolSize;
        try {
            minPoolSize = Integer.parseInt(properties.getProperty("minPoolSize"));
        } catch (Exception e) {
            minPoolSize = 1;
        }
        int maxPoolSize;
        try {
            maxPoolSize = Integer.parseInt(properties.getProperty("maxPoolSize"));
        } catch (Exception e) {
            maxPoolSize = 5;
        }
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setAcquireIncrement(1);
    }

    /**
     * Extracts connections from connection pool.
     *
     * @return extracted connection
     */
    @SneakyThrows
    private Connection getConnection() {
        return cpds.getConnection();
    }

    /**
     * Creates database using the {@code init.sql} script from specified by
     * {@code pathToConfig} directory.
     */
    void createDB() {
        executeSQL(pathToConfig + "/init.sql");
        log.info("Database created");
    }

    /**
     * Drops database using the {@code drop.sql} script from specified by
     * {@code pathToConfig} directory.
     */
    void dropDB() {
        executeSQL(pathToConfig + "/drop.sql");
        log.info("Database dropped");
    }

    @SneakyThrows
    private void executeSQL(String file) {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            String query = Files.lines(Paths.get(file))
                    .filter(line -> !line.startsWith("--")) //comments
                    .collect(Collectors.joining(" "));
            statement.execute(query);
        }
    }

    /**
     * Creates tables required to correct work of application. Uses stored
     * procedure from {@code init.sql} script.
     */
    @SneakyThrows(SQLException.class)
    void createTables() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(CREATE_TABLES);
        }
    }

    /**
     * Drops all used tables. Uses stored procedure from {@code init.sql}
     * script.
     */
    @SneakyThrows(SQLException.class)
    void dropTables() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(DROP_TABLES);
        }
    }

    /**
     * Insert into database entry about specified row. Uses stored procedure
     * from {@code init.sql} script.
     *
     * @param query    query name
     * @param document document name
     * @param count    count of links between query and document
     */
    @SneakyThrows(SQLException.class)
    void insert(String query, String document, int count) {
        try (val connection = getConnection();
             val statement = connection.prepareStatement(INSERT_INTO_LINKS)) {
            statement.setString(1, query);
            statement.setString(2, document);
            statement.setInt(3, count);
            statement.execute();
        }
    }

    /**
     * Insert into database entries about specified rows. Uses stored procedure
     * from {@code init.sql} script.
     *
     * @param map map, each entry of which represents one row. Key is a tuple
     *            consist of two Strings: {@code query} and {@code document}.
     *            Value is the count of links between those query and document.
     * @see Tuple2
     */
    void insertAll(Map<Tuple2<String, String>, Integer> map) {
        try (val connection = getConnection();
             val statement = connection
                     .prepareStatement(INSERT_INTO_LINKS,
                             Statement.NO_GENERATED_KEYS)) {
            map.forEach((tuple, count) -> {
                try {
                    statement.setString(1, tuple._1);
                    statement.setString(2, tuple._2);
                    statement.setInt(3, count);
                    statement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            statement.executeBatch();
            log.info(String.format("%d rows inserted", map.size()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compacts entries in database, that is group entries with the same
     * query and document and sum their counts. Uses stored procedure from
     * {@code init.sql} script.
     * <p>
     * <p>For example, two rows before compact looks like
     * <table>
     * <tr>
     * <th>query</th>
     * <th>document</th>
     * <th>count</th>
     * </tr>
     * <tr>
     * <td>car</td>
     * <td>www.car.com</td>
     * <td>1000</td>
     * </tr>
     * <tr>
     * <td>car</td>
     * <td>www.car.com</td>
     * <td>500</td>
     * </tr>
     * </table>
     * <br>
     * after compact will looks like
     * <table>
     * <tr>
     * <th>query</th>
     * <th>document</th>
     * <th>count</th>
     * </tr>
     * <tr>
     * <td>car</td>
     * <td>www.car.com</td>
     * <td>1500</td>
     * </tr>
     * </table>
     */
    @SneakyThrows(SQLException.class)
    void compact() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(COMPACT_LINKS);
        }
    }

    /**
     * Creates pre-cluster table. This function moves all rows with
     * same documents into one table. It firstly takes one query
     * with the most count of documents and moves it to separate
     * table. After, it looks for all queries with the same document
     * and moves them to this table too. And so on, until there is
     * nothing to move.
     *
     * @return pre-cluster table's size
     */
    @SneakyThrows(SQLException.class)
    int createPreCluster() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            val rs = statement.executeQuery(CREATE_PRE_CLUSTER);
            if (!rs.next()) {
                log.error("pre_cluster() returns nothing");
                return -1;
            }
            val res = rs.getInt(1);
            log.info(String.format("Created pre cluster with %d rows", res));
            return res;
        }
    }

    /**
     * Creates tables required to clustering. Uses stored
     * procedure from {@code init.sql} script.
     * <p>
     * <p>It also creates table, containing almost result of CROSS JOIN of
     * the existing table, received in the previous steps (that's why those
     * tables creates not at start of application).
     */
    @SneakyThrows
    void createClusterTables() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(CREATE_CLUSTER_TABLES);
        }
    }

    /**
     * Cluster all existing entries. Stops when similarity function returns
     * value less than {@code threshold}. Uses stored procedure from
     * {@code init.sql} script.
     *
     * @param threshold threshold of the meaning "similar". If similarity
     *                  function returns value exceeding specified value
     *                  for two most similar queries then those queries
     *                  will be combined in one cluster. Otherwise, not
     */
    @SneakyThrows
    void combineAll(float threshold) {
        try (val connection = getConnection();
             val preparedStatement = connection.prepareStatement(COMBINE_ALL)) {
            preparedStatement.setFloat(1, threshold);
            preparedStatement.execute();
        }
    }

    /**
     * Selects formed clusters (only consists of at less two queries). Uses
     * stored procedure from {@code init.sql} script.
     *
     * @return set of formed clusters
     */
    @SneakyThrows
    Set<String> selectClusters() {
        try (val connection = getConnection();
             val statement = connection.createStatement();
             val rs = statement.executeQuery(SELECT_CLUSTERS)) {
            val result = new HashSet<String>();
            while (rs.next()) {
                result.add(rs.getString(1));
            }
            return result;
        }
    }

    /**
     * Selects document and count corresponding to specified {@code query}.
     * Uses stored procedure from {@code init.sql} script.
     *
     * @param query interesting query
     * @return map, consist of document and count
     */
    Map<String, Integer> selectSetOfDocuments(String query) {
        return selectMap(SELECT_DOCUMENTS, query);
    }

    /**
     * Selects query and count corresponding to specified {@code document}.
     * Uses stored procedure from {@code init.sql} script.
     *
     * @param document interesting document
     * @return map, consist of query and count
     */
    Map<String, Integer> selectSetOfQueries(String document) {
        return selectMap(SELECT_QUERIES, document);
    }

    @SneakyThrows(SQLException.class)
    private Map<String, Integer> selectMap(String query, String value) {
        try (val connection = getConnection();
             val preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, value);
            try (val rs = preparedStatement.executeQuery()) {
                val map = new HashMap<String, Integer>();
                while (rs.next()) {
                    String k = rs.getString(1);
                    int v = rs.getInt(2);
                    log.debug(String.format("Selected (%s, %d) from links table", k, v));
                    map.put(k, v);
                }
                return map;
            }
        }
    }

    /**
     * Closes all connections in connection pool.
     *
     * @throws SQLException if exception happened while closing.
     * @see ComboPooledDataSource
     */
    @Override
    public void close() throws SQLException {
        cpds.close(true);
    }
}
