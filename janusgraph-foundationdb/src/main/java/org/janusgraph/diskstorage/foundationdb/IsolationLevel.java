package org.janusgraph.diskstorage.foundationdb;

public enum IsolationLevel {
    READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE;
}
