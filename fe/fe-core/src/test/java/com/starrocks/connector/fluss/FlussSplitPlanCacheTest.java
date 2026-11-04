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
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlussSplitPlanCacheTest {
    private static final TablePath PATH = TablePath.of("db", "events");
    private static final List<SourceSplitBase> SPLITS = List.of(new LogSplit(new TableBucket(1L, 0), null, 0L, 10L));

    private final AtomicLong nanos = new AtomicLong();
    private final FlussSplitPlanCache cache = new FlussSplitPlanCache(new Ticker() {
        @Override
        public long read() {
            return nanos.get();
        }
    });

    @Test
    public void testHitWithinTtlAndMissAfter() {
        FlussSplitPlanCache.Key key = FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("a"));
        Assertions.assertNull(cache.getIfFresh(key, 5000));

        cache.put(key, SPLITS);
        advanceMillis(5000);
        Assertions.assertEquals(SPLITS, cache.getIfFresh(key, 5000));
        advanceMillis(1);
        Assertions.assertNull(cache.getIfFresh(key, 5000));
        Assertions.assertEquals(SPLITS, cache.getIfFresh(key, 10_000));
    }

    @Test
    public void testTtlIsCappedAtMax() {
        FlussSplitPlanCache.Key key = FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("a"));
        cache.put(key, SPLITS);
        advanceMillis(FlussSplitPlanCache.MAX_TTL_MS + 1);
        Assertions.assertNull(cache.getIfFresh(key, Long.MAX_VALUE));
    }

    @Test
    public void testKeyDistinguishesTablePartitionsAndPredicates() {
        cache.put(FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("a")), SPLITS);

        Assertions.assertEquals(SPLITS, cache.getIfFresh(
                FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("a")), 5000));
        Assertions.assertNull(cache.getIfFresh(
                FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("b")), 5000));
        Assertions.assertNull(cache.getIfFresh(
                FlussSplitPlanCache.key(2L, PATH, Collections.emptyList(), predicates("a")), 5000));
        Assertions.assertNull(cache.getIfFresh(
                FlussSplitPlanCache.key(1L, TablePath.of("db", "other"), Collections.emptyList(), predicates("a")),
                5000));
        Assertions.assertNull(cache.getIfFresh(
                FlussSplitPlanCache.key(1L, PATH, List.of("dt=2026-11-04"), predicates("a")), 5000));
    }

    @Test
    public void testCachedSplitsAreImmutable() {
        FlussSplitPlanCache.Key key = FlussSplitPlanCache.key(1L, PATH, Collections.emptyList(), predicates("a"));
        cache.put(key, new ArrayList<>(SPLITS));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> cache.getIfFresh(key, 5000).clear());
    }

    private void advanceMillis(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private static List<Predicate> predicates(String tenant) {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(DataTypes.STRING()));
        return List.of(builder.equal(0, BinaryString.fromString(tenant)));
    }
}
