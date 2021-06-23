package org.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TX_CACHE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class FDBStorageSetup extends StorageSetup {
    static {

    }

    public static ModifiableConfiguration getFDBConfiguration(String dir) {
        return buildGraphConfiguration()
            .set(STORAGE_BACKEND,"fdb")
            .set(STORAGE_DIRECTORY, dir)
            .set(DROP_ON_CLEAR, false);
    }

    public static ModifiableConfiguration getFDBConfiguration() {
        return getFDBConfiguration(getHomeDir("fdb"));
    }

    public static WriteConfiguration getBerkeleyJEGraphConfiguration() {
        return getFDBConfiguration().getConfiguration();
    }

    public static ModifiableConfiguration getFDBPerformanceConfiguration() {
        return getFDBConfiguration()
            .set(STORAGE_TRANSACTIONAL,false)
            .set(TX_CACHE_SIZE,1000);
    }
}
