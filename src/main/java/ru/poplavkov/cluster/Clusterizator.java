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

    public Clusterizator(Store store, double threshold) {
        this.store = store;
        this.threshold = threshold;
        log.info(String.format(
                "Created Clusterizator object with threshold=%3.5f", threshold));
    }

    @SuppressWarnings("unused")
    public Clusterizator(Store store) {
        this (store, 0.001);
    }

    /**
     * Creates all necessary tables and cluster.
     */
    public void cluster() {
        store.createClusterTables();
        log.info("Cluster tables created");
        log.info("Start clustering");
        store.combineAll(threshold);
        log.info("Clustering complete");
    }

    public Set<String> getClusters() {
        return store.selectClusters();
    }
}
