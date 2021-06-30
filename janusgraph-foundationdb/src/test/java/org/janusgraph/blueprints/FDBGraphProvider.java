package org.janusgraph.blueprints;

import org.janusgraph.FDBStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.foundationdb.FDBTx;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.time.Duration;
import java.util.Set;

public class FDBGraphProvider extends AbstractJanusGraphProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return FDBStorageSetup.getFDBConfiguration(StorageSetup.getHomeDir(graphName)).set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(150L));
    }

    @Override
    public Set<Class> getImplementations() {
        final Set<Class> implementations = super.getImplementations();
        implementations.add(FDBTx.class);
        return implementations;
    }
}
