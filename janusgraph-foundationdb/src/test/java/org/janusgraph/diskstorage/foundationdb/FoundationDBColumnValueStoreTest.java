package org.janusgraph.diskstorage.foundationdb;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Container;

public class FoundationDBColumnValueStoreTest extends KeyColumnValueStoreTest {

    //TODO: differ fixed and variable length

    @Container
    public static FoundationDBContainer container = new FoundationDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FoundationDBStoreManager foundationDBStoreManager = new FoundationDBStoreManager(container.getFoundationDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(foundationDBStoreManager, ImmutableMap.of(storeName, 8));
    }

    @Test
    @Override
    public void testConcurrentGetSlice() {

    }

    @Test
    @Override
    public void testConcurrentGetSliceAndMutate() {

    }
}
