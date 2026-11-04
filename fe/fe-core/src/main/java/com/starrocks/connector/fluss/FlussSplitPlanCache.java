// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.fluss;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Per-catalog cache of generated hybrid lake/log splits, so repeated lookups skip the lake snapshot,
 * Paimon plan and bucket-offset round trips. A cached plan pins the log stopping offsets and lake files it
 * was built with: within the TTL a query does not see newer rows, and the TTL must stay well below the
 * lake snapshot expiry or a hit can reference expired Paimon files. {@link #MAX_TTL_MS} caps that window
 * whatever the session asks for.
 */
final class FlussSplitPlanCache {
    static final long MAX_TTL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long MAX_ENTRIES = 1024;

    private final Ticker ticker;
    private final Cache<Key, Entry> cache;

    FlussSplitPlanCache() {
        this(Ticker.systemTicker());
    }

    FlussSplitPlanCache(Ticker ticker) {
        this.ticker = ticker;
        this.cache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .maximumSize(MAX_ENTRIES)
                .expireAfterWrite(MAX_TTL_MS, TimeUnit.MILLISECONDS)
                .build();
    }

    /** Returns the cached splits if they were generated within {@code ttlMs}, otherwise null. */
    List<SourceSplitBase> getIfFresh(Key key, long ttlMs) {
        Entry entry = cache.getIfPresent(key);
        if (entry == null) {
            return null;
        }
        long ageNanos = ticker.read() - entry.createdNanos;
        return ageNanos <= TimeUnit.MILLISECONDS.toNanos(Math.min(ttlMs, MAX_TTL_MS)) ? entry.splits : null;
    }

    void put(Key key, List<SourceSplitBase> splits) {
        cache.put(key, new Entry(ImmutableList.copyOf(splits), ticker.read()));
    }

    static Key key(long tableId, TablePath tablePath, Collection<String> partitionNames, List<Predicate> lakePredicates) {
        return new Key(tableId, tablePath, ImmutableList.copyOf(partitionNames), ImmutableList.copyOf(lakePredicates));
    }

    static final class Key {
        private final long tableId;
        private final TablePath tablePath;
        private final List<String> partitionNames;
        private final List<Predicate> lakePredicates;

        private Key(long tableId, TablePath tablePath, List<String> partitionNames, List<Predicate> lakePredicates) {
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.partitionNames = partitionNames;
            this.lakePredicates = lakePredicates;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key that = (Key) o;
            return tableId == that.tableId && tablePath.equals(that.tablePath)
                    && partitionNames.equals(that.partitionNames) && lakePredicates.equals(that.lakePredicates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, tablePath, partitionNames, lakePredicates);
        }
    }

    private static final class Entry {
        private final List<SourceSplitBase> splits;
        private final long createdNanos;

        private Entry(List<SourceSplitBase> splits, long createdNanos) {
            this.splits = splits;
            this.createdNanos = createdNanos;
        }
    }
}
