package org.janusgraph.blueprints;

import org.janusgraph.FDBContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.junit.jupiter.Container;

public class FDBMultiQueryGraphProvider extends FDBGraphProvider {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return super.getJanusGraphConfiguration(graphName, test, testMethodName).set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
    }
}
