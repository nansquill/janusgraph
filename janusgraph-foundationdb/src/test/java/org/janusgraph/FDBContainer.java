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
import org.janusgraph.diskstorage.foundationdb.FDBStoreManager;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.net.ServerSocket;

import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.CLUSTER_FILE_PATH;
import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.GET_RANGE_MODE;
import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.ISOLATION_LEVEL;
import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class FDBContainer extends FixedHostPortGenericContainer<FDBContainer> {

    public static final String DEFAULT_IMAGE_AND_TAG = "foundationdb/foundationdb:6.3.13";
    private static final Integer DEFAULT_PORT = 4500;
    private static final String FDB_CLUSTER_FILE_ENV_KEY = "FDB_CLUSTER_FILE";
    private static final String FDB_NETWORKING_MODE_ENV_KEY = "FDB_NETWORKING_MODE";
    private static final String FDB_PORT_ENV_KEY = "FDB_PORT";
    private static final String DEFAULT_CLUSTER_FILE_PARENT_DIR = "/etc/foundationdb";
    private static final String DEFAULT_CLUSTER_FILE_PATH = DEFAULT_CLUSTER_FILE_PARENT_DIR + "/" + "fdb.cluster";
    private static final String DEFAULT_NETWORKING_MODE = "host";
    private static final String DEFAULT_VOLUME_SOURCE_PATH = "./fdb";

    public FDBContainer() {
        this(DEFAULT_IMAGE_AND_TAG);
    }

    public FDBContainer(String dockerImageName) {
        super(dockerImageName);
        Integer port = findRandomOpenPortOnAllLocalInterfaces();
        this.addFixedExposedPort(port, port);
        this.addExposedPort(port);
        this.addEnv(FDB_CLUSTER_FILE_ENV_KEY, DEFAULT_CLUSTER_FILE_PATH);
        this.addEnv(FDB_PORT_ENV_KEY, port.toString());
        this.addEnv(FDB_NETWORKING_MODE_ENV_KEY, DEFAULT_NETWORKING_MODE);
        this.withClasspathResourceMapping(DEFAULT_VOLUME_SOURCE_PATH, DEFAULT_CLUSTER_FILE_PARENT_DIR, BindMode.READ_WRITE);
    }

    public ModifiableConfiguration getFDBConfiguration() {
        return getFDBConfiguration("janusgraph-test-foundationdb");
    }

    public ModifiableConfiguration getFDBConfiguration(final String graphName) {
        ModifiableConfiguration config = buildGraphConfiguration()
            .set(STORAGE_BACKEND, FDBStoreManager.class.getName())
            .set(STORAGE_DIRECTORY, graphName)
            .set(DROP_ON_CLEAR, false)
            .set(CLUSTER_FILE_PATH, "target/test-classes/fdb/fdb.cluster")
            .set(ISOLATION_LEVEL, "read_committed_with_write")
            .set(GET_RANGE_MODE, getAndCheckRangeModeFromTestEnvironment())
            .set(VERSION, 620);

        return config;
    }

    private String getAndCheckRangeModeFromTestEnvironment() {
        String mode = System.getProperty("getrangemode");
        if (mode == null) {
            return "list";
        }
        else if (mode.equalsIgnoreCase("iterator")){
            return "iterator";
        }
        else if (mode.equalsIgnoreCase("list")){
            return "list";
        }
        else {
            return "list";
        }
    }

    private Integer findRandomOpenPortOnAllLocalInterfaces() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            return DEFAULT_PORT;
        }
    }
}
