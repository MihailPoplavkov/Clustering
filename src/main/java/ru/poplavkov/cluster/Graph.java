package ru.poplavkov.cluster;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("WeakerAccess")
@Getter
public class Graph {
    private Set<String> queries = new HashSet<>();
    private Set<String> documents = new HashSet<>();
    private Map<Tuple, Integer> links = new HashMap<>();

    public void addLink(String query, String document) {
        queries.add(query);
        documents.add(document);
        Tuple tuple = new Tuple(query, document);
        Integer count = links.get(tuple);
        count = count == null ? 1 : count + 1;
        links.put(tuple, count);
    }
}
