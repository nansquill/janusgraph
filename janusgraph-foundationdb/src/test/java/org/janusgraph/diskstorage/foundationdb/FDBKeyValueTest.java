package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.FDBContainer;
import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.testcontainers.junit.jupiter.Container;

public class FDBKeyValueTest extends KeyValueStoreTest {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FDBStoreManager(FDBStorageSetup.getFDBConfiguration());
    }
}
