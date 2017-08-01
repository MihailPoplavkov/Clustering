package ru.poplavkov.cluster;

import java.io.PrintWriter;

import lombok.SneakyThrows;
import lombok.val;

public class Main {
    public static void main(String[] args) {
        work("src/main/resources/db",
                "data/user-ct-test-collection-01.txt",
                "output.txt");
    }

    @SneakyThrows
    private static void work(String dbConfig, String inputFile, String outputFile) {
        try (val store = new Store(dbConfig)) {
            store.createDB();
            store.createTables();

            val preprocessor = new Preprocessor(store, 100000);
            preprocessor.readAndStore(inputFile);

            val clusterizator = new Clusterizator(store, 0.001);
            clusterizator.cluster();
            val clusters = clusterizator.getClusters();

            try (val writer = new PrintWriter(outputFile)) {
                clusters.stream()
                        .map(cl -> cl.split(";"))
                        .filter(cl -> cl.length > 1)
                        .forEach(cl -> {
                            val sb = new StringBuilder();
                            sb.append("{");
                            for (val s : cl) {
                                sb.append(String.format("%s, ", s));
                            }
                            sb.delete(sb.length() - 2, sb.length());
                            sb.append("}\n\n");
                            writer.println(sb.toString());
                        });
            }
            store.dropTables();
            store.dropDB();
        }
    }
}
