package org.janusgraph.blueprints.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.janusgraph.blueprints.FDBGraphComputerProvider;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(FDBProcessComputerSuite.class)
@GraphProviderClass(provider = FDBGraphComputerProvider.class, graph = JanusGraph.class)
public class FDBJanusGraphComputerTest {
}
