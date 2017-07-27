package ru.poplavkov.cluster;

import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PreprocessorTest {
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

    @SneakyThrows
    @Test
    void readAndStore() {
        val preprocessor = new Preprocessor(store);
        preprocessor.readAndStore("src/test/resources/test.txt");
        val map = store.selectSetOfQueries("www.car.com");
        assertEquals(2, map.get("ferrari").intValue());
        assertEquals(1, map.get("car").intValue());
    }

}