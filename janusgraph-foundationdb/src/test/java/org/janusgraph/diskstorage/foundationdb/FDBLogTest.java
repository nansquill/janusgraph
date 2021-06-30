package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.FDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.diskstorage.log.KCVSLogTest;
import org.testcontainers.junit.jupiter.Container;

public class FDBLogTest extends KCVSLogTest {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FDBStoreManager fDBStoreManager = new FDBStoreManager(container.getFDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(fDBStoreManager);
    }
}
