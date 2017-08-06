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
    private float threshold;

    public Clusterizator(Store store, float threshold) {
        this.store = store;
        this.threshold = threshold;
        log.info(String.format(
                "Created Clusterizator object with threshold=%3.5f", threshold));
    }

    @SuppressWarnings("unused")
    public Clusterizator(Store store) {
        this (store, 0.001f);
    }

    /**
     * Creates all necessary tables and cluster.
     */
    public void cluster() {
        while (store.createPreCluster() > 0) {
            store.createClusterTables();
            log.info("Cluster tables created");
            log.info("Start clustering");
            log.info(String.format("Clustering complete. Created %d clusters", store.combineAll(threshold)));
        }
        log.info("Clustering is over");
    }

    public Set<String> getClusters() {
        return store.selectClusters();
    }
}
