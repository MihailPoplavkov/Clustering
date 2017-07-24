package ru.poplavkov.cluster;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Log4j2
class Store {
    // TODO: сделать так, чтоб за один заход можно было несколько записей делать

    private static final String DROP_TABLE_QUERIES = "DROP TABLE IF EXISTS queries";
    private static final String DROP_TABLE_DOCUMENTS = "DROP TABLE IF EXISTS documents";
    private static final String DROP_TABLE_LINKS = "DROP TABLE IF EXISTS links";

    private static final String CREATE_TABLE_QUERIES =
            "CREATE TABLE queries (" +
                    "name STRING PRIMARY KEY)";

    private static final String CREATE_TABLE_DOCUMENTS =
            "CREATE TABLE documents (" +
                    "name STRING PRIMARY KEY)";

    private static final String CREATE_TABLE_LINKS =
            "CREATE TABLE links (" +
                    "query STRING REFERENCES queries(name), " +
                    "document STRING REFERENCES documents(name), " +
                    "count INTEGER)";

    private static final String SELECT_COUNT_FROM_LINKS =
            "SELECT count FROM links " +
                    "WHERE query=? AND document=?";

    private static final String UPDATE_LINKS =
            "UPDATE links " +
                    "SET count = ? " +
                    "WHERE query = ? AND document = ?";

    private static final String INSERT_INTO_QUERIES_IF_NOT_EXISTS =
            "INSERT INTO queries(name) " +
                    "SELECT ? " +
                    "WHERE NOT EXISTS " +
                    "(SELECT 1 FROM queries WHERE name = ?)";

    private static final String INSERT_INTO_DOCUMENTS_IF_NOT_EXISTS =
            "INSERT INTO documents(name) " +
                    "SELECT ? " +
                    "WHERE NOT EXISTS " +
                    "(SELECT 1 FROM documents WHERE name = ?)";

    private static final String INSERT_INTO_LINKS =
            "INSERT INTO links(query, document, count) " +
                    "VALUES (?, ?, 1)";

    private static final String SELECT_QUERIES_FROM_LINKS =
            "SELECT query, count FROM links " +
                    "WHERE document = ?";

    private static final String SELECT_DOCUMENTS_FROM_LINKS =
            "SELECT document, count FROM links " +
                    "WHERE query = ?";

    static {
        try {
            Class.forName("org.sqlite.JDBC");
            log.info("JDBC driver successfully loaded");
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private Connection connection;

    @SneakyThrows(SQLException.class)
    private void createConnection() {
        connection = DriverManager.getConnection("jdbc:sqlite:data.db");
    }

    @SneakyThrows(SQLException.class)
    void createDB() {
        if (connection == null) {
            createConnection();
        }

        val statement = connection.createStatement();

        statement.executeUpdate(DROP_TABLE_QUERIES);
        statement.executeUpdate(DROP_TABLE_DOCUMENTS);
        statement.executeUpdate(DROP_TABLE_LINKS);

        statement.executeUpdate(CREATE_TABLE_QUERIES);
        log.info("Created table \'queries\'");
        statement.executeUpdate(CREATE_TABLE_DOCUMENTS);
        log.info("Created table \'documents\'");
        statement.executeUpdate(CREATE_TABLE_LINKS);
        log.info("Created table \'links\'");
    }

    @SneakyThrows(SQLException.class)
    void insert(String query, String document) {
        if (connection == null || connection.isClosed()) {
            createConnection();
        }
        val rs = prepareStatement(SELECT_COUNT_FROM_LINKS, query, document).executeQuery();
        if (rs.next()) {
            val count = rs.getInt("count");
            prepareStatement(UPDATE_LINKS, count + 1, query, document).execute();
        } else {
            prepareStatement(INSERT_INTO_QUERIES_IF_NOT_EXISTS, query, query).execute();
            prepareStatement(INSERT_INTO_DOCUMENTS_IF_NOT_EXISTS, document, document).execute();
            prepareStatement(INSERT_INTO_LINKS, query, document).execute();
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
        if (connection == null || connection.isClosed()) {
            createConnection();
        }
        val rs = prepareStatement(query, value).executeQuery();
        val map = new HashMap<String, Integer>();
        while (rs.next()) {
            String k = rs.getString(1);
            int v = rs.getInt(2);
            log.info(String.format("Select (%s, %d) from links table", k, v));
            map.put(k, v);
        }
        return map;
    }

    @SneakyThrows(SQLException.class)
    private PreparedStatement prepareStatement(String query, String value) {
        val statement = connection.prepareStatement(query);
        statement.setString(1, value);
        return statement;
    }

    @SneakyThrows(SQLException.class)
    private PreparedStatement prepareStatement(String query, String val1, String val2) {
        val statement = prepareStatement(query, val1);
        statement.setString(2, val2);
        return statement;
    }

    @SneakyThrows(SQLException.class)
    private PreparedStatement prepareStatement(String query, int val1, String val2, String val3) {
        val statement = connection.prepareStatement(query);
        statement.setInt(1, val1);
        statement.setString(2, val2);
        statement.setString(3, val3);
        return statement;
    }
}
