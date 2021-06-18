package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.diskstorage.log.KCVSLogTest;
import org.testcontainers.junit.jupiter.Container;

public class FoundationDBLogTest extends KCVSLogTest {

    @Container
    public static FoundationDBContainer container = new FoundationDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FoundationDBStoreManager foundationDBStoreManager = new FoundationDBStoreManager(container.getFoundationDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(foundationDBStoreManager);
    }
}
