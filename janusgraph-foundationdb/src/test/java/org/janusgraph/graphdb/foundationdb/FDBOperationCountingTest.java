package org.janusgraph.graphdb.foundationdb;

import io.github.artsok.RepeatedIfExceptionsTest;
import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.junit.jupiter.api.AfterEach;

public class FDBOperationCountingTest extends JanusGraphOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return FDBStorageSetup.getFDBGraphConfiguration();
    }

    @Override
    @RepeatedIfExceptionsTest(repeats = 4, minSuccess = 2)
    public void testIdCounts() {
        super.testIdCounts();
    }

    @AfterEach
    public void resetCounts() {
        resetMetrics(); // Metrics is a singleton, so subsequents test runs have wrong counts if we don't clean up.
    }
}
