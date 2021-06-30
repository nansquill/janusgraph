package org.janusgraph.blueprints.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.janusgraph.blueprints.FDBGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(FDBProcessStandardSuite.class)
@GraphProviderClass(provider = FDBGraphProvider.class, graph = JanusGraph.class)
public class FDBJanusGraphProcessTest {
}
