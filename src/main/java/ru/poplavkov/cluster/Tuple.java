package ru.poplavkov.cluster;

import lombok.Value;

@Value
class Tuple {
    private final String query;
    private final String document;
}
