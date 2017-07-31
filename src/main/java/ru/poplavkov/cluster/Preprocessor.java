package ru.poplavkov.cluster;

import io.vavr.Tuple2;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

/**
 * Makes all preparatory work. It reads entries from specified file,
 * prepares them (normalize, delete stop words and stem) and stores
 * in database via specified {@code Store}.
 *
 * @see Store
 */
@SuppressWarnings("WeakerAccess")
@Log4j2
public class Preprocessor {
    /**
     * Initial thread pool capacity.
     */
    private static final int POOL_SIZE = 5;

    /**
     * Path to file, contains <b>sorted</b> stop words that will be
     * deleted from queries.
     */
    private static final String stopWordsFileName = "src/main/resources/stopWordsSortedList.txt";

    /**
     * Array of sorted stop words. It uses to delete unwanted words
     * from queries.
     */
    private static String[] stopWordsSorted;

    static {
        try (val br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(stopWordsFileName)))) {
            stopWordsSorted = br.lines()
                    .toArray(String[]::new);
            log.info("Stop words file successfully loaded");
        } catch (Exception e) {
            log.error("Error with loading stop words file");
            stopWordsSorted = new String[0];
        }
    }

    /**
     * Ready to write store.
     *
     * @see Store
     */
    private Store store;

    /**
     * Collection of read lines. Access to this map is critical,
     * so lock uses to restrain it.
     */
    private Map<Tuple2<String, String>, Integer> map;

    /**
     * Uses to restrain access to map.
     *
     * @see Lock
     */
    private Lock lock;

    /**
     * Count of read and prepared lines that will be send to
     * {@code Store} to store it to database.
     */
    private int countToFlush;

    /**
     * Thread pool to start many writer threads.
     */
    private ExecutorService threadPool;

    /**
     * Creates {@code Preprocessor} with specified {@code Store} and
     * {@code countToFlush}.
     *
     * @param store         {@code Store} that must be ready to write data.
     * @param countToFlush  count of read and prepared lines that will be
     *                      send to {@code store} to store it to database.
     */
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

    /**
     * Reads lines from specified by {@code fileName} file, prepares them and
     * stores to database. It uses {@code POOL_SIZE} threads to store data and
     * parallel stream to read data.
     *
     * @param fileName                  path to file with necessarily two headers:
     *                                  <ul>
     *                                      <li>query</li>
     *                                      <li>clickURL</li>
     *                                  </ul>
     *                                  The comparison is ignored case
     * @throws FileNotFoundException    if specified file not found
     */
    @SneakyThrows(IOException.class)
    public void readAndStore(String fileName) throws FileNotFoundException {
        try (val br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
            val line = br.readLine();
            if (line == null) {
                return;
            }
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
                            new Tuple2<>(stemAndRemoveStopWords(normalize(s[qIndex])),
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
            log.info("Store compacted");
        }
    }

    /**
     * Removes all punctuation marks and unnecessary spaces.
     *
     * @param str   {@code String} to normalize
     * @return      normalized {@code String}
     */
    private String normalize(String str) {
        return str.toLowerCase()
                .replaceAll("\\p{Punct}", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    /**
     * Stems and removes stop words from specified {@code String}
     * @param str   {@code String} to prepare
     * @return      prepared {@code String}
     */
    private String stemAndRemoveStopWords(String str) {
        val stemmer = new Stemmer();
        val stringBuilder = Arrays.stream(str.split("\\s"))
                .filter(s -> s.length() > 1)
                .map(stemmer::stem)
                .filter(s -> Arrays.binarySearch(stopWordsSorted, s) < 0)
                .collect(StringBuilder::new,
                        (sb, s) -> sb.append(s).append(" "),
                        StringBuilder::append);
        if (stringBuilder.length() > 0) {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }
}
