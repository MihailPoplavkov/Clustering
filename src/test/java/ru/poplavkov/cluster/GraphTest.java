package ru.poplavkov.cluster;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class GraphTest {
    @Test
    void addLink() {
        Graph graph = new Graph();
        String q1 = "ab";
        String q2 = "cd";
        String d1 = "ef";
        String d2 = "gh";

        graph.addLink(q1, d1);
        graph.addLink(q1, d2);
        graph.addLink(q1, d2);
        graph.addLink(q1, d2);
        graph.addLink(q2, d2);

        Set<String> queries = graph.getQueries();
        Set<String> documents = graph.getDocuments();

        assertTrue(queries.containsAll(Arrays.asList(q1, q2)));
        assertTrue(documents.containsAll(Arrays.asList(d1, d2)));

        assertEquals(3, graph.getLinks().get(new Tuple(q1, d2)).intValue());
    }
}