package ru.poplavkov.cluster;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphTest {
    private static Graph graph;
    private static String q2 = "ab";
    private static String q1 = "cd";
    private static String d1 = "ef";
    private static String d2 = "gh";

    @BeforeAll
    static void init() {
        graph = new Graph();
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
        String path = "src/test/resources";
        assertTrue(graph.writeOnDisk(path));

        File queriesFile = new File(String.format("%s/%s", path, ".queries1"));
        File documentsFile = new File(String.format("%s/%s", path, ".documents1"));
        File linksFile = new File(String.format("%s/%s", path, ".links1"));

        assertTrue(assertExist(queriesFile, graph.getQueries()));
        assertTrue(assertExist(documentsFile, graph.getDocuments()));
        assertTrue(assertExist(linksFile, graph.getLinks()));

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