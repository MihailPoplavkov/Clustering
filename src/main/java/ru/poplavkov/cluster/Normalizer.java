package ru.poplavkov.cluster;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.val;

import java.io.*;
import java.util.Arrays;

@SuppressWarnings("WeakerAccess")
@Log4j2
public class Normalizer {
    private static final String stopWordsFileName = "src/main/resources/stopWordsSortedList.txt";
    private static String[] stopWordsSorted;
    static {
        try (val br = new BufferedReader(new InputStreamReader(new FileInputStream(stopWordsFileName)))) {
            stopWordsSorted = br.lines()
                    .toArray(String[]::new);
                    //.collect(ArrayList::new, List::add, List::addAll)
                    //.toArray(new String[0]);
            log.info("Stop words file successfully loaded");
        } catch (Exception e) {
            log.error("Error with loading stop words file");
            stopWordsSorted = new String[0];
        }
    }

    private Store store;

    public Normalizer() {
        store = new Store();
        store.createDB();
        log.info("Created Normalizer object");
    }

    @SneakyThrows(IOException.class)
    public void readAndStore(String fileName) throws FileNotFoundException {
        try (val br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
            val line = br.readLine();
            if (line == null) return;
            val headers = line.split("\\s");
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
                    .map(s -> s.split("\\s"))
                    .filter(s -> s.length > maxIndex)
                    .forEach(s -> {
                        val query = stem(removeStopWords(normalize(s[qIndex])));
                        //noinspection ConstantConditions
                        if (!query.isEmpty()) {
                            val document = s[docIndex];
                            store.insert(query.trim(), document);
                        }
                    });
        }

    }

    private String normalize(String str) {
        return str.toLowerCase().replaceAll("\\p{Punct}", "");
    }

    private String removeStopWords(String str) {
        val tokens = str.split("\\s");
        return Arrays.stream(tokens)
                .filter(s -> Arrays.binarySearch(stopWordsSorted, s) < 0)
                .collect(StringBuilder::new,
                        (sb, s) -> sb.append(s).append(" "),
                        StringBuilder::append)
                .toString();
    }

    private String stem(String str) {
        return str;
    }
}
