package ru.poplavkov.cluster;

import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("WeakerAccess")
@Log4j2
public class Preprocessor {
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
    private Condition mapIsEmpty;

    public Preprocessor(Store store, int countToFlush) {
        this.store = store;
        this.countToFlush = countToFlush;
        map = new HashMap<>(countToFlush);
        lock = new ReentrantLock();
        mapIsEmpty = lock.newCondition();
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

            val thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    if (!map.isEmpty()) {
                        Map<Tuple2<String, String>, Integer> newMap;
                        lock.lock();
                        try {
                            newMap = new HashMap<>(map);
                            map.clear();
                            mapIsEmpty.signalAll();
                        } finally {
                            lock.unlock();
                        }
                        store.insertAll(newMap);
                    }
                }
            });

            thread.start();

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
                            while (map.size() >= countToFlush) {
                                mapIsEmpty.awaitUninterruptibly();
                            }
                            map.put(tuple, map.getOrDefault(tuple, 0) + 1);
                        } finally {
                            lock.unlock();
                        }
                    });

            //TODO: redone below

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.debug("Reached the end of readAndStore");
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
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
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    private String stem(String str) {
        return str;
    }
}
