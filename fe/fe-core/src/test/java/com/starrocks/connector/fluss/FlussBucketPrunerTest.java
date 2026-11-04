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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class FlussBucketPrunerTest {
    private static final int NUM_BUCKETS = 4;

    @Test
    public void testStringBucketKeyMatchesWriterBucket() {
        TableInfo table = compositeKeyTable(false, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        int tenantIdx = table.getRowType().getFieldIndex("tenant");
        for (int i = 0; i < 500; i++) {
            BinaryString tenant = BinaryString.fromString(UUID.randomUUID().toString());
            Optional<Set<Integer>> buckets = FlussBucketPruner.candidateBuckets(table,
                    List.of(builder.equal(tenantIdx, tenant), builder.equal(
                            table.getRowType().getFieldIndex("category"), BinaryString.fromString("mobile"))));
            Assertions.assertEquals(Optional.of(Set.of(writerBucket(table, tenantRow(table, tenant)))), buckets);
        }
    }

    @Test
    public void testDefaultBucketKeyMatchesWriterBucket() {
        TableInfo table = intPrimaryKeyTable();
        Assertions.assertTrue(table.isDefaultBucketKey());
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        Random random = new Random(42);
        for (int i = 0; i < 500; i++) {
            int id = random.nextInt();
            GenericRow row = new GenericRow(table.getRowType().getFieldCount());
            row.setField(0, id);
            row.setField(1, BinaryString.fromString("n"));
            Assertions.assertEquals(Optional.of(Set.of(writerBucket(table, row))),
                    FlussBucketPruner.candidateBuckets(table, List.of(builder.equal(0, id))));
        }
    }

    @Test
    public void testInAndOrOfEqualsUnionBuckets() {
        TableInfo table = compositeKeyTable(false, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        int tenantIdx = table.getRowType().getFieldIndex("tenant");
        List<Object> tenants = new ArrayList<>();
        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 25; i++) {
            BinaryString tenant = BinaryString.fromString("tenant-" + i);
            tenants.add(tenant);
            expected.add(writerBucket(table, tenantRow(table, tenant)));
        }
        // More than 20 literals stays a single IN leaf; up to 20 becomes an OR of equals.
        Assertions.assertEquals(Optional.of(expected),
                FlussBucketPruner.candidateBuckets(table, List.of(builder.in(tenantIdx, tenants))));
        Set<Integer> expectedSmall = new TreeSet<>();
        for (Object tenant : tenants.subList(0, 3)) {
            expectedSmall.add(writerBucket(table, tenantRow(table, (BinaryString) tenant)));
        }
        Assertions.assertEquals(Optional.of(expectedSmall),
                FlussBucketPruner.candidateBuckets(table, List.of(builder.in(tenantIdx, tenants.subList(0, 3)))));
    }

    @Test
    public void testAndConjunctsAreFlattenedAndIntersected() {
        TableInfo table = compositeKeyTable(false, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        int tenantIdx = table.getRowType().getFieldIndex("tenant");
        BinaryString a = BinaryString.fromString("a");
        BinaryString b = BinaryString.fromString("b");
        Predicate and = PredicateBuilder.and(builder.in(tenantIdx, List.of(a, b)), builder.equal(tenantIdx, a));
        Assertions.assertEquals(Optional.of(Set.of(writerBucket(table, tenantRow(table, a)))),
                FlussBucketPruner.candidateBuckets(table, List.of(and)));
    }

    @Test
    public void testUnprovableShapesDoNotPrune() {
        TableInfo table = compositeKeyTable(false, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        int tenantIdx = table.getRowType().getFieldIndex("tenant");
        int categoryIdx = table.getRowType().getFieldIndex("category");
        BinaryString a = BinaryString.fromString("a");

        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table, Collections.emptyList()));
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.equal(categoryIdx, a))));
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.greaterThan(tenantIdx, a))));
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.notEqual(tenantIdx, a))));
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(PredicateBuilder.or(builder.equal(tenantIdx, a), builder.equal(categoryIdx, a)))));
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.equal(tenantIdx, a), builder.equal(tenantIdx, BinaryString.fromString("b")))));
    }

    @Test
    public void testPartitionedTableIsNotPruned() {
        TableInfo table = compositeKeyTable(true, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.equal(table.getRowType().getFieldIndex("tenant"), BinaryString.fromString("a")))));
    }

    @Test
    public void testTooManyKeyCombinationsIsNotPruned() {
        TableInfo table = compositeKeyTable(false, "paimon");
        PredicateBuilder builder = new PredicateBuilder(table.getRowType());
        List<Object> tenants = new ArrayList<>();
        for (int i = 0; i <= FlussBucketPruner.MAX_KEY_COMBINATIONS; i++) {
            tenants.add(BinaryString.fromString("t" + i));
        }
        Assertions.assertEquals(Optional.empty(), FlussBucketPruner.candidateBuckets(table,
                List.of(builder.in(table.getRowType().getFieldIndex("tenant"), tenants))));
    }

    /** The bucket the Fluss upsert writer assigns, using the same calls as {@code UpsertWriterImpl}. */
    private static int writerBucket(TableInfo table, GenericRow fullRow) {
        RowType rowType = table.getRowType();
        KeyEncoder primaryKeyEncoder = KeyEncoder.ofPrimaryKeyEncoder(
                rowType, table.getPhysicalPrimaryKeys(), table.getTableConfig(), table.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder = KeyEncoder.ofBucketKeyEncoder(
                rowType, table.getBucketKeys(), table.getTableConfig(), table.isDefaultBucketKey(),
                primaryKeyEncoder);
        DataLakeFormat lakeFormat = table.getTableConfig().getDataLakeFormat().orElse(null);
        return BucketingFunction.of(lakeFormat).bucketing(bucketKeyEncoder.encodeKey(fullRow), table.getNumBuckets());
    }

    private static GenericRow tenantRow(TableInfo table, BinaryString tenant) {
        RowType rowType = table.getRowType();
        GenericRow row = new GenericRow(rowType.getFieldCount());
        row.setField(rowType.getFieldIndex("tenant"), tenant);
        row.setField(rowType.getFieldIndex("connection"), BinaryString.fromString("c"));
        row.setField(rowType.getFieldIndex("category"), BinaryString.fromString("mobile"));
        row.setField(rowType.getFieldIndex("id"), BinaryString.fromString("item"));
        row.setField(rowType.getFieldIndex("dt"), BinaryString.fromString("2026-11-04"));
        return row;
    }

    /** Composite primary key bucketed by its leading column only, with a Paimon lake and kv format v2. */
    private static TableInfo compositeKeyTable(boolean partitioned, String lakeFormat) {
        Schema.Builder schema = Schema.newBuilder()
                .column("tenant", DataTypes.STRING())
                .column("connection", DataTypes.STRING())
                .column("category", DataTypes.STRING())
                .column("id", DataTypes.STRING())
                .column("dt", DataTypes.STRING());
        schema.primaryKey(partitioned
                ? List.of("tenant", "connection", "category", "id", "dt")
                : List.of("tenant", "connection", "category", "id"));
        TableDescriptor.Builder descriptor = TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(NUM_BUCKETS, "tenant")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", lakeFormat)
                .property("table.kv.format-version", "2");
        if (partitioned) {
            descriptor.partitionedBy("dt");
        }
        return TableInfo.of(TablePath.of("db", "events"), 1L, 1, descriptor.build(), null, 0L, 0L);
    }

    private static TableInfo intPrimaryKeyTable() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
        TableDescriptor descriptor = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(NUM_BUCKETS, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .build();
        return TableInfo.of(TablePath.of("db", "t"), 2L, 1, descriptor, null, 0L, 0L);
    }
}
