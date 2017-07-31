package ru.poplavkov.cluster;

import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterizatorTest {
    private Store store;

    @SneakyThrows
    @BeforeEach
    void init() {
        store = new Store("src/main/resources/db");
        store.createDB();
        store.createTables();
        val preprocessor  = new Preprocessor(store);
        preprocessor.readAndStore("src/test/resources/test.txt");
    }

    @SneakyThrows
    @AfterEach
    void cancel() {
        store.dropTables();
        store.dropDB();
        store.close();
    }

    @Test
    void cluster() {
        val clusterizator = new Clusterizator(store);
        clusterizator.cluster();
        val set = clusterizator.getClusters();
        assertEquals(2, set.size());
        val iter = set.iterator();
        val set1 = iter.next().split(";");
        val set2 = iter.next().split(";");
        String[] cars;
        String[] pets;
        Arrays.sort(set1);
        Arrays.sort(set2);
        if (set1[0].equals("car")) {
            cars = set1;
            pets = set2;
        } else {
            cars = set2;
            pets = set1;
        }

        String[] expectedCars = {"car", "ferrari"};
        assertArrayEquals(expectedCars, cars);

        //kitti, but not kitty because of stemming
        String[] expectedPets = {"cat", "kitti"};
        assertArrayEquals(expectedPets, pets);
    }


}