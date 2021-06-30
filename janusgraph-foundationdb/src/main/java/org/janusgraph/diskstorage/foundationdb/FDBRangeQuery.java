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

package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

import static org.janusgraph.diskstorage.StaticBuffer.ARRAY_FACTORY;

public class FDBRangeQuery {

    private final KVQuery originalQuery;
    private final KeySelector startKeySelector;
    private final KeySelector endKeySelector;
    private final int limit;

    final StaticBuffer.Factory<byte[]> ARRAY_ALT_FACTORY = (array, offset, limit) -> {
        final byte[] copyArray = new byte[limit - offset];
        System.arraycopy(array, offset, copyArray, 0, limit - offset);
        return copyArray;
    };

    public FDBRangeQuery(Subspace database, KVQuery kvQuery) {
        originalQuery = kvQuery;
        limit = kvQuery.getLimit();

        final StaticBuffer keyStart = kvQuery.getStart();
        final StaticBuffer keyEnd = kvQuery.getEnd();
        byte[] startKey = (keyStart == null) ? database.range().begin : database.pack(keyStart.as(ARRAY_FACTORY));
        byte[] endKey = (keyEnd == null) ? database.range().end : database.pack(keyEnd.as(ARRAY_FACTORY));
        startKeySelector = KeySelector.firstGreaterOrEqual(startKey);
        endKeySelector = KeySelector.firstGreaterOrEqual(endKey);
    }

    public KVQuery getOriginalQuery() {
        return originalQuery;
    }

    public KeySelector getStartKeySelector() {
        return startKeySelector;
    }

    public KeySelector getEndKeySelector() {
        return endKeySelector;
    }

    public int getLimit() {
        return limit;
    }
}
