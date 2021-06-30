package org.janusgraph.blueprints;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class FDBMultiQueryGraphProvider extends FDBGraphProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return super.getJanusGraphConfiguration(graphName, test, testMethodName)
            .set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
    }
}
