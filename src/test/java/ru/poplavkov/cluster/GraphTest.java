package ru.poplavkov.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphTest {
    private Graph graph;
    private String path = "src/test/resources";
    private String q2 = "ab";
    private String q1 = "cd";
    private String d1 = "ef";
    private String d2 = "gh";

    @BeforeEach
    void setUp() {
        graph = new Graph(path);
        graph.addLink(q1, d1);
        graph.addLink(q1, d2);
        graph.addLink(q1, d2);
        graph.addLink(q1, d2);
        graph.addLink(q2, d2);
    }

    @Test
    void addLink() {
        Set<String> queries = graph.getQueries();
        Set<String> documents = graph.getDocuments();

        assertTrue(queries.containsAll(Arrays.asList(q1, q2)));
        assertTrue(documents.containsAll(Arrays.asList(d1, d2)));

        assertEquals(3, graph.getLinks().get(new Tuple(q1, d2)).intValue());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void writeOnDisk() {
        Set<String> queries = new HashSet<>(graph.getQueries());
        Set<String> documents = new HashSet<>(graph.getDocuments());
        Map<Tuple, Integer> links = new HashMap<>(graph.getLinks());

        assertTrue(graph.writeOnDisk());

        File queriesFile = new File(String.format("%s/%s", path, ".queries1"));
        File documentsFile = new File(String.format("%s/%s", path, ".documents1"));
        File linksFile = new File(String.format("%s/%s", path, ".links1"));

        assertTrue(assertExist(queriesFile, queries));
        assertTrue(assertExist(documentsFile, documents));
        assertTrue(assertExist(linksFile, links));

        assertTrue(graph.getQueries().isEmpty());
        assertTrue(graph.getDocuments().isEmpty());
        assertTrue(graph.getLinks().isEmpty());

        queriesFile.delete();
        documentsFile.delete();
        linksFile.delete();
    }

    private boolean assertExist(File file, Set<String> set) {
        try (FileInputStream fis = new FileInputStream(file)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                assertTrue(set.remove(line));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean assertExist(File file, Map<Tuple, Integer> map) {
        try (FileInputStream fis = new FileInputStream(file)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line = br.readLine()) != null) {
                String tokens[] = line.split(":", 2);
                String q = tokens[0];
                tokens = tokens[1].split("=", 2);
                String d = tokens[0];
                Integer i = Integer.valueOf(tokens[1]);
                Tuple tuple = new Tuple(q, d);
                assertEquals(map.remove(tuple), i);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}