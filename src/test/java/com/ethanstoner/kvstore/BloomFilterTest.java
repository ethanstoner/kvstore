package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    @Test
    void addedKeyIsAlwaysFound() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        bf.add("hello");
        assertTrue(bf.mightContain("hello"));
    }

    @Test
    void missingKeyUsuallyNotFound() {
        BloomFilter bf = new BloomFilter(1000, 0.01);
        for (int i = 0; i < 1000; i++) {
            bf.add("key-" + i);
        }
        int falsePositives = 0;
        for (int i = 1000; i < 11000; i++) {
            if (bf.mightContain("key-" + i)) falsePositives++;
        }
        assertTrue(falsePositives < 300, "FP rate too high: " + falsePositives + "/10000");
    }

    @Test
    void noFalseNegatives() {
        BloomFilter bf = new BloomFilter(500, 0.01);
        for (int i = 0; i < 500; i++) {
            bf.add("item-" + i);
        }
        for (int i = 0; i < 500; i++) {
            assertTrue(bf.mightContain("item-" + i), "False negative for item-" + i);
        }
    }

    @Test
    void singleInsertionWorks() {
        BloomFilter bf = new BloomFilter(1, 0.01);
        bf.add("only");
        assertTrue(bf.mightContain("only"));
    }
}
