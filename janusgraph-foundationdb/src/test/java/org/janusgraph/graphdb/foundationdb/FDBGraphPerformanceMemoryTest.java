package org.janusgraph.graphdb.foundationdb;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

public class FDBGraphPerformanceMemoryTest extends JanusGraphPerformanceMemoryTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return FDBStorageSetup.getFDBGraphConfiguration();
    }
}
