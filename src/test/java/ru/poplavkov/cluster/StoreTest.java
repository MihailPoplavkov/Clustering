package ru.poplavkov.cluster;

import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {
    private static Store store;

    @BeforeAll
    static void init() {
        store = new Store();
        store.createDB();
    }

    @Test
    void createTable() {
        val file = new File("data.db");
        assertTrue(file.exists());
    }

    @Test
    void insertAndSelect() {
        val q1 = "ab";
        val q2 = "cd";
        val d1 = "ef";
        val d2 = "gh";

        store.insert(q1, d1);
        store.insert(q1, d1);
        store.insert(q1, d2);
        store.insert(q2, d2);
        store.insert(q2, d2);
        store.insert(q2, d2);

        val map1 = store.selectSetOfDocuments(q1);
        assertEquals(2, map1.get(d1).intValue());
        assertEquals(1, map1.get(d2).intValue());

        val map2 = store.selectSetOfQueries(d2);
        assertEquals(1, map2.get(q1).intValue());
        assertEquals(3, map2.get(q2).intValue());
    }
}