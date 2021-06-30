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

package org.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TX_CACHE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class FDBStorageSetup extends StorageSetup {
    static {

    }

    public static ModifiableConfiguration getFDBConfiguration(String dir) {
        return buildGraphConfiguration()
            .set(STORAGE_BACKEND,"foundationdb")
            .set(STORAGE_DIRECTORY, dir)
            .set(DROP_ON_CLEAR, false);
    }

    public static ModifiableConfiguration getFDBConfiguration() {
        return getFDBConfiguration(getHomeDir("foundationdb"));
    }

    public static WriteConfiguration getFDBGraphConfiguration() {
        return getFDBConfiguration().getConfiguration();
    }

    public static ModifiableConfiguration getFDBPerformanceConfiguration() {
        return getFDBConfiguration()
            .set(STORAGE_TRANSACTIONAL,false)
            .set(TX_CACHE_SIZE,1000);
    }
}
