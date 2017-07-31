package ru.poplavkov.cluster;

import java.util.Set;
import lombok.extern.log4j.Log4j2;

@SuppressWarnings("WeakerAccess")
@Log4j2
public class Clusterizator {
    /**
     * Ready to cluster store.
     *
     * @see Store
     */
    private Store store;

    /**
     * Threshold to similarity function, happened to differ similar and
     * dissimilar queries
     */
    private double threshold;

    /**
     * Count of queries sent to database per time
     */
    private int countBatches;

    public Clusterizator(Store store, double threshold, int countBatches) {
        this.store = store;
        this.threshold = threshold;
        this.countBatches = countBatches;
        log.info(String.format(
                "Created Clusterizator object with threshold=%3.5f and countBatches=%d",
                threshold, countBatches));
    }

    @SuppressWarnings("unused")
    public Clusterizator(Store store) {
        this (store, 0.001, 100);
    }

    /**
     * Creates all necessary tables and cluster.
     */
    public void cluster() {
        store.createClusterTables();
        log.info("Cluster tables created");
        log.info("Start clustering");
        store.combineAll(threshold, countBatches);
        log.info("Clustering complete");
    }

    public Set<String> getClusters() {
        return store.selectClusters();
    }
}
