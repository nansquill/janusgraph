package org.janusgraph.diskstorage.foundationdb;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.FDBContainer;
import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.testcontainers.junit.jupiter.Container;

import java.util.concurrent.ExecutionException;

public class FDBVariableLengthKCVSTest extends KeyColumnValueStoreTest {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FDBStoreManager sm = new FDBStoreManager(FDBStorageSetup.getFDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Override
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        super.testConcurrentGetSlice();
    }

    @Override
    public void testConcurrentGetSliceAndMutate() throws BackendException, ExecutionException, InterruptedException {
        super.testConcurrentGetSliceAndMutate();
    }
}
