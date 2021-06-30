package org.janusgraph.graphdb;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class FDBTransactionTest {

    static {

    }

    @Test
    public void longRunningTxShouldBeRolledBack(@TempDir File dir) throws InterruptedException {
        JanusGraph graph = JanusGraphFactory.open("foundationdb:" + dir.getAbsolutePath());

        GraphTraversalSource traversal = graph.traversal();
        for (int i = 0; i < 10; i++) {
            traversal.addV().property("a", "2").next();
        }
        traversal.tx().commit();

        GraphTraversalSource g = graph.tx().createThreadedTx().traversal();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            g.V()
                .has("a", "2")
                .sideEffect(ignored -> {
                    try {
                        // artificially slow down the traversal execution so that
                        // this test has a chance to interrupt it before it finishes
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
                .toList();
        });
        Thread.sleep(100);
        g.tx().rollback();
        graph.close();
        assertThrows(ExecutionException.class, future::get);
    }
}
