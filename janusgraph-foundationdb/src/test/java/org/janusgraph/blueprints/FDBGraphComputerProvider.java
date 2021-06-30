// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.blueprints;

import org.janusgraph.FDBContainer;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.foundationdb.FDBTx;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.util.Set;

public class FDBGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        container.start();
        ModifiableConfiguration config = super.getJanusGraphConfiguration(graphName, test, testMethodName);
        config.setAll(container.getFDBConfiguration(StorageSetup.getHomeDir(graphName)).getAll());
        //config.setAll(FDBStorageSetup.getFDBConfiguration(StorageSetup.getHomeDir(graphName)).getAll());
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(20));
        return config;
    }

    @Override
    public Set<Class> getImplementations() {
        final Set<Class> implementations = super.getImplementations();
        implementations.add(FDBTx.class);
        return implementations;
    }
}
