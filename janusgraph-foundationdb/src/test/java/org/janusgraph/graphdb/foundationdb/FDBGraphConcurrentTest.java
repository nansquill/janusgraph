package org.janusgraph.graphdb.foundationdb;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphConcurrentTest;

public class FDBGraphConcurrentTest extends JanusGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return FDBStorageSetup.getFDBGraphConfiguration();
    }
}
