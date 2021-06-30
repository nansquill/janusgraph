package org.janusgraph.blueprints.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.janusgraph.blueprints.FDBGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith((StructureStandardSuite.class))
@GraphProviderClass(provider = FDBGraphProvider.class, graph = JanusGraph.class)
public class FDBJanusGraphStructureTest {
}
