package org.janusgraph.graphdb.foundationdb;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.olap.OLAPTest;

public class FDBOLAPTest extends OLAPTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return FDBStorageSetup.getFDBGraphConfiguration();
    }
}
