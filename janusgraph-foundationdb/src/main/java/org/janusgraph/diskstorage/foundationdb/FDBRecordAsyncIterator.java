package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;

import java.io.IOException;

public class FDBRecordAsyncIterator extends FDBRecordIterator {

    private final FDBTx tx;
    private final FDBRangeQuery rangeQuery;
    private final AsyncIterator<KeyValue> asyncIterator;

    public FDBRecordAsyncIterator(DirectorySubspace storeDatabase, FDBTx transaction, FDBRangeQuery rangeQuery, AsyncIterator<KeyValue> asyncIterator, KeySelector keySelector) {
        super(storeDatabase, asyncIterator, keySelector);

        this.tx = transaction;
        this.rangeQuery = rangeQuery;
        this.asyncIterator = asyncIterator;
    }

    @Override
    public void close() throws IOException {
        asyncIterator.cancel();
    }

    @Override
    public void remove() {
        asyncIterator.remove();
    }

    @Override
    protected void fetchNext() {
        while(true) {
            try {
                super.fetchNext();
                break;
            }
            catch (RuntimeException runtimeException) {
                asyncIterator.cancel();
                //TODO: restart
                throw runtimeException;
            }
            catch (Exception exception) {
                throw exception;
            }
        }
    }
}
