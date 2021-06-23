package org.janusgraph.diskstorage.foundationdb;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.FDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;
import org.testcontainers.junit.jupiter.Container;

public class FoundationDBColumnValueStoreTest extends KeyColumnValueStoreTest {

    //TODO: differ fixed and variable length

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FDBStoreManager fDBStoreManager = new FDBStoreManager(container.getFoundationDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(fDBStoreManager, ImmutableMap.of(storeName, 8));
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
