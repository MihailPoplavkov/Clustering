package ru.poplavkov.cluster;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NormalizerTest {
    @SneakyThrows
    @Test
    void readAndStore() {
        Normalizer normalizer = new Normalizer();
        normalizer.readAndStore("data/test.txt");
        //show logs
    }

}