package ru.poplavkov.cluster;

import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("WeakerAccess")
@Log4j2
public class Preprocessor {
    private static final int POOL_SIZE = 5;
    private static final String stopWordsFileName = "src/main/resources/stopWordsSortedList.txt";
    private static String[] stopWordsSorted;

    static {
        try (val br = new BufferedReader(new InputStreamReader(new FileInputStream(stopWordsFileName)))) {
            stopWordsSorted = br.lines()
                    .toArray(String[]::new);
            log.info("Stop words file successfully loaded");
        } catch (Exception e) {
            log.error("Error with loading stop words file");
            stopWordsSorted = new String[0];
        }
    }

    private Store store;
    private Map<Tuple2<String, String>, Integer> map;
    private int countToFlush;
    private Lock lock;
    private ExecutorService threadPool;

    public Preprocessor(Store store, int countToFlush) {
        this.store = store;
        this.countToFlush = countToFlush;
        map = new HashMap<>(countToFlush);
        lock = new ReentrantLock();
        threadPool = Executors.newFixedThreadPool(POOL_SIZE);
        log.info(String.format("Created Preprocessor object with countToFlush=%d", countToFlush));
    }

    public Preprocessor(Store store) {
        this(store, 1000);
    }

    @SneakyThrows(IOException.class)
    public void readAndStore(String fileName) throws FileNotFoundException {
        try (val br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
            val line = br.readLine();
            if (line == null) return;
            val headers = line.split("\\t");
            int queryIndex = -1;
            int documentIndex = -1;
            int i = 0;
            for (val header : headers) {
                if ("query".equalsIgnoreCase(header)) {
                    queryIndex = i;
                } else if ("clickURL".equalsIgnoreCase(header)) {
                    documentIndex = i;
                }
                i++;
            }
            if (queryIndex == -1 || documentIndex == -1)
                throw new IllegalArgumentException("Input file have to contain correct headers");

            final int qIndex = queryIndex;
            final int docIndex = documentIndex;
            final int maxIndex = qIndex > docIndex ? qIndex : docIndex;
            br.lines()
                    .parallel()
                    .map(s -> s.split("\\t"))
                    .filter(s -> s.length > maxIndex)
                    .map(s ->
                            new Tuple2<>(stem(removeStopWords(normalize(s[qIndex]))),
                                    s[docIndex]))
                    .filter(tuple -> !tuple._1.isEmpty())
                    .forEach(tuple -> {
                        lock.lock();
                        try {
                            if (map.size() >= countToFlush) {
                                Map<Tuple2<String, String>, Integer> newMap = new HashMap<>(map);
                                map.clear();
                                threadPool.execute(() -> store.insertAll(newMap));
                            }
                            map.put(tuple, map.getOrDefault(tuple, 0) + 1);
                        } finally {
                            lock.unlock();
                        }
                    });
            //remaining
            threadPool.execute(() -> store.insertAll(map));

            //join
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
            store.compact();
        }

    }

    private String normalize(String str) {
        return str.toLowerCase()
                .replaceAll("\\p{Punct}", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String removeStopWords(String str) {
        val tokens = str.split("\\s");
        val stringBuilder = Arrays.stream(tokens)
                .filter(s -> Arrays.binarySearch(stopWordsSorted, s) < 0)
                .collect(StringBuilder::new,
                        (sb, s) -> sb.append(s).append(" "),
                        StringBuilder::append);
        if (stringBuilder.length() > 0) {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }

    private String stem(String str) {
        return str;
    }
}
