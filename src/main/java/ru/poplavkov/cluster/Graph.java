package ru.poplavkov.cluster;

import lombok.Getter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

@SuppressWarnings("WeakerAccess")
@Getter
public class Graph {
    private int counter;
    private Set<String> queries = new HashSet<>();
    private Set<String> documents = new HashSet<>();
    private Map<Tuple, Integer> links = new HashMap<>();

    public Graph(int counter) {
        this.counter = counter;
    }

    public Graph() {
        this(0);
    }

    public void addLink(String query, String document) {
        queries.add(query);
        documents.add(document);
        Tuple tuple = new Tuple(query, document);
        Integer count = links.get(tuple);
        count = count == null ? 1 : count + 1;
        links.put(tuple, count);
    }

    public boolean writeOnDisk(String path) {
        if (path.charAt(path.length() - 1) == '/') {
            path = path.substring(0, path.length() - 1);
        }
        counter++;
        File queriesFile = createFile(path, ".queries", counter);
        File documentsFile = createFile(path, ".documents", counter);
        File linksFile = createFile(path, ".links", counter);
        return writeOnDisk(queriesFile, queries) &&
                writeOnDisk(documentsFile, documents) &&
                writeOnDisk(linksFile, links);
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

}
