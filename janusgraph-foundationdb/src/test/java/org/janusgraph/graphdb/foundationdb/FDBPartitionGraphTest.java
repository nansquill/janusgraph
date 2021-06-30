package org.janusgraph.graphdb.foundationdb;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;

public class FDBPartitionGraphTest extends JanusGraphPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return FDBStorageSetup.getFDBGraphConfiguration();
    }

    /*
     * TODO: debug fdb dbs keyslice method
     */
}
