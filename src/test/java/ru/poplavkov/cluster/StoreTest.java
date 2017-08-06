package ru.poplavkov.cluster;

import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoreTest {
    private static Store store;

    @BeforeAll
    static void init() {
        store = new Store("src/main/resources/db");
        store.createDB();
        store.createTables();
    }

    @SneakyThrows
    @AfterAll
    static void cancel() {
        store.dropTables();
        store.dropDB();
        store.close();
    }

    @Test
    void insertAndSelect() {
        val q1 = "ab";
        val q2 = "cd";
        val d1 = "ef";
        val d2 = "gh";

        store.insert(q1, d1, 1);
        store.insert(q1, d1, 1);
        store.insert(q1, d2, 10);
        store.insert(q2, d2, 1);
        store.insert(q2, d2, 1);
        store.insert(q2, d2, 1);
        store.compact();

        val map1 = store.selectSetOfDocuments(q1);
        assertEquals(2, map1.get("other").intValue());
        assertEquals(10, map1.get(d2).intValue());

        val map2 = store.selectSetOfQueries(d2);
        assertEquals(10, map2.get(q1).intValue());
        assertEquals(3, map2.get(q2).intValue());
    }

    @Test
    void insertAll() {
        val q1 = "query1";
        val q2 = "query2";
        val d1 = "doc1";
        val d2 = "doc2";

        Map<Tuple2<String, String>, Integer> map = new HashMap<>();
        map.put(new Tuple2<>(q1, d1), 10);
        map.put(new Tuple2<>(q1, d2), 20);
        map.put(new Tuple2<>(q2, d1), 30);
        map.put(new Tuple2<>(q2, d2), 40);
        store.insertAll(map);
        store.insert(q1, d2, 5);
        store.compact();

        val map1 = store.selectSetOfDocuments(q1);
        assertEquals(10, map1.get(d1).intValue());
        assertEquals(25, map1.get(d2).intValue());

        val map2 = store.selectSetOfQueries(d2);
        assertEquals(25, map2.get(q1).intValue());
        assertEquals(40, map2.get(q2).intValue());
    }
}