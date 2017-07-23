package ru.poplavkov.cluster;

import lombok.NonNull;
import lombok.Value;

@Value
class Tuple implements Comparable<Tuple> {
    @NonNull
    private final String query;
    @NonNull
    private final String document;

    @Override
    public int compareTo(Tuple o) {
        int i = query.compareTo(o.query);
        return i != 0 ? i :
                document.compareTo(o.document);
    }
}
