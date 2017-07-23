package ru.poplavkov.cluster;

import lombok.Getter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

@SuppressWarnings("WeakerAccess")
public class Graph {
    @Getter
    private Set<String> queries = new HashSet<>();
    @Getter
    private Set<String> documents = new HashSet<>();
    @Getter
    private Map<Tuple, Integer> links = new HashMap<>();
    private String outputPath;
    private int counter;
    private int maxInternalSize;
    private int currentInternalSize;

    public Graph(String outputPath, int counter, int maxInternalSize) {
        this.outputPath = outputPath;
        this.counter = counter;
        this.maxInternalSize = maxInternalSize;
    }

    public Graph(String outputPath, int counter) {
        this(outputPath, counter, 10 * 1024 * 1024);
    }

    public Graph(String outputPath) {
        this(outputPath, 0);
    }

    public void addLink(String query, String document) {
        if (queries.add(query)) {
            currentInternalSize += query.getBytes().length;
        }
        if (documents.add(document)) {
            currentInternalSize += document.getBytes().length;
        }
        Tuple tuple = new Tuple(query, document);
        Integer count = links.get(tuple);
        if (count == null) {
            currentInternalSize += query.getBytes().length + document.getBytes().length;
            count = 1;
        } else {
            count++;
        }
        links.put(tuple, count);

        if (currentInternalSize >= maxInternalSize) {
            writeOnDisk();
        }
    }

    public boolean writeOnDisk() {
        if (outputPath.charAt(outputPath.length() - 1) == '/') {
            outputPath = outputPath.substring(0, outputPath.length() - 1);
        }
        counter++;
        File queriesFile = createFile(outputPath, ".queries", counter);
        File documentsFile = createFile(outputPath, ".documents", counter);
        File linksFile = createFile(outputPath, ".links", counter);
        boolean result = writeOnDisk(queriesFile, queries) &&
                writeOnDisk(documentsFile, documents) &&
                writeOnDisk(linksFile, links);
        if (result) clear();
        return result;
    }

    private boolean writeOnDisk(File file, Set<String> set) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            set.stream()
                    .sorted()
                    .forEach(s -> {
                        try {
                            fos.write(String.format("%s%n", s).getBytes());
                        } catch (IOException e) {
                            // TODO: написать handler
                            e.printStackTrace();
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean writeOnDisk(File file, Map<Tuple, Integer> map) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            map.entrySet()
                    .stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .forEach(es -> {
                        try {
                            Tuple tuple = es.getKey();
                            fos.write(
                                    String.format("%s:%s=%d%n",
                                            tuple.getQuery(), tuple.getDocument(), es.getValue())
                                            .getBytes());
                        } catch (IOException e) {
                            // TODO: написать handler
                            e.printStackTrace();
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private File createFile(String path, String name, int counter) {
        return new File(String.format("%s/%s%d", path, name, counter));
    }

    private void clear() {
        queries.clear();
        documents.clear();
        links.clear();
        currentInternalSize = 0;
    }
}
