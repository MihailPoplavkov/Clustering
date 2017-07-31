package ru.poplavkov.cluster;

import lombok.extern.log4j.Log4j2;

import java.util.List;

@SuppressWarnings("WeakerAccess")
@Log4j2
public class Clusterizator {
    private Store store;
    private double threshold;
    private int countBatches;

    public Clusterizator(Store store, double threshold, int countBatches) {
        this.store = store;
        this.threshold = threshold;
        this.countBatches = countBatches;
        log.info(String.format("Created Clusterizator object with threshold=%3.5f and countBatches=%d",
                threshold, countBatches));
    }

    @SuppressWarnings("unused")
    public Clusterizator(Store store) {
        this (store, 0.001, 100);
    }

    public void cluster() {
        store.createClusterTables();
        log.info("Cluster tables created");
        log.info("Start clustering");
        store.combineAll(threshold, countBatches);
        log.info("Clustering complete");
    }

    public List<String> getClusters() {
        return store.selectClusters();
    }
}
