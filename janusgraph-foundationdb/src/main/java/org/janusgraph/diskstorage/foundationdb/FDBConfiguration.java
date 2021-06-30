package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PreInitializeConfigOptions
public class FDBConfiguration {

    private static final Logger log = LoggerFactory.getLogger(FDBConfiguration.class);

    public static final ConfigNamespace FDB_NS = new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "fdb", "FoundationDB storage backend root configuration namespace");

    public static final ConfigOption<String> STORAGE_DIRECTORY = new ConfigOption<>(
        FDB_NS,
        "directory",
        "Storage directory for FoundationDB. It will be created if it does not exist.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    public static final ConfigOption<String> STORAGE_ROOT = new ConfigOption<>(
        FDB_NS,
        "root",
        "Storage root directory for FoundationDB",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    public static final ConfigOption<String> GRAPH_NAME = new ConfigOption<>(
        FDB_NS,
        "graphname",
        "Graph name as an optional configuration setting that you may supply when opening a graph.",
        ConfigOption.Type.LOCAL,
        "fdb_graph");

    public static final ConfigOption<Integer> VERSION = new ConfigOption<>(
        FDB_NS,
        "version",
        "The version of the FoundationDB cluster.",
        ConfigOption.Type.LOCAL,
        620);

    public static final ConfigOption<String> CLUSTER_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "cluster-file-path",
        "Path to the FoundationDB cluster file",
        ConfigOption.Type.LOCAL,
        "default");

    public static final ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
        FDB_NS,
        "isolation-level",
        "Options are serializable, read_committed_no_write, read_committed_with_write",
        ConfigOption.Type.LOCAL,
        "serializable");

    public static final ConfigOption<String> GET_RANGE_MODE = new ConfigOption<>(
        FDB_NS,
        "get-range-mode",
        "The mod of executing FDB getRange, either `iterator` or `list`",
        ConfigOption.Type.LOCAL,
        "list"
    );
}
