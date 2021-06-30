package org.janusgraph.blueprints;

import org.janusgraph.FDBContainer;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.foundationdb.FDBTx;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.util.Set;

public class FDBGraphProvider extends AbstractJanusGraphProvider {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        container.start();
        return container.getFDBConfiguration(StorageSetup.getHomeDir(graphName)).set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(150L));
        //return FDBStorageSetup.getFDBConfiguration(StorageSetup.getHomeDir(graphName)).set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(150L));
    }

    @Override
    public Set<Class> getImplementations() {
        final Set<Class> implementations = super.getImplementations();
        implementations.add(FDBTx.class);
        return implementations;
    }
}
