package org.janusgraph.graphdb.database.management;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

public class FDBManagementTest extends ManagementTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return FDBStorageSetup.getBerkeleyJEGraphConfiguration();
    }
}
