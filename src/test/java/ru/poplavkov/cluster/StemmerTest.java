package ru.poplavkov.cluster;

import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StemmerTest {
    private static Stemmer stemmer;

    @BeforeAll
    static void init() {
        stemmer = new Stemmer();
    }

    @Test
    void stem() {
        val s1 = "meetings";
        val s2 = "running";
        val s3 = "cats";

        assertEquals("meet", stemmer.stem(s1));
        assertEquals("run", stemmer.stem(s2));
        assertEquals("cat", stemmer.stem(s3));
    }
}