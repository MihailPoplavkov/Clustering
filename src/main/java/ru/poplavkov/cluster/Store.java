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
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Log4j2
class Store implements AutoCloseable {
    private static final String CREATE_TABLES = "SELECT create_tables()";
    private static final String DROP_TABLES = "SELECT drop_tables()";
    private static final String INSERT_INTO_LINKS = "SELECT insert_line(?, ?, ?)";
    private static final String SELECT_QUERIES_FROM_LINKS = "SELECT q, cou FROM select_queries(?)";
    private static final String SELECT_DOCUMENTS_FROM_LINKS = "SELECT doc, cou FROM select_documents(?)";
    private static final String COMPACT_LINKS = "SELECT compact_links()";

    private String pathToConfig;
    private ComboPooledDataSource cpds;

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

    @SneakyThrows
    private Connection getConnection() {
        return cpds.getConnection();
    }

    void createDB() {
        executeSQL(pathToConfig + "/init.sql");
        log.info("Database created");
    }

    void dropDB() {
        executeSQL(pathToConfig + "/drop.sql");
        log.info("Database dropped");
    }

    @SneakyThrows
    private void executeSQL(String file) {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            String query = Files.lines(Paths.get(file))
                    .collect(Collectors.joining(" "));
            statement.execute(query);
        }
    }

    @SneakyThrows(SQLException.class)
    void createTables() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(CREATE_TABLES);
        }
    }

    @SneakyThrows(SQLException.class)
    void dropTables() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(DROP_TABLES);
        }
    }

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

    @SneakyThrows(SQLException.class)
    void compact() {
        try (val connection = getConnection();
             val statement = connection.createStatement()) {
            statement.execute(COMPACT_LINKS);
        }
    }

    Map<String, Integer> selectSetOfDocuments(String query) {
        return selectMap(SELECT_DOCUMENTS_FROM_LINKS, query);
    }

    Map<String, Integer> selectSetOfQueries(String document) {
        return selectMap(SELECT_QUERIES_FROM_LINKS, document);
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

    @Override
    public void close() throws Exception {
        cpds.close();
    }
}
