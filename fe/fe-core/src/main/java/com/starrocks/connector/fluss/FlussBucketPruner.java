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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Derives the Fluss buckets that can hold rows matching the lake predicates, from equality and IN constraints
 * on every bucket-key column. Any shape it cannot prove (a missing constraint, a type it does not encode,
 * a partitioned table) yields {@link Optional#empty()}, meaning "do not prune".
 *
 * <p>The bucket must be computed exactly as the Fluss writer does (see {@code UpsertWriterImpl} and
 * {@code BucketAssignerFactory}): a wrong encoder or bucketing function silently drops the bucket holding the
 * rows. For every lake format except Hudi, the writer's bucket-key encoder reduces to
 * {@code KeyEncoder.of(rowType, bucketKeys, lakeFormat)}, which is what is used here.
 */
final class FlussBucketPruner {
    static final int MAX_KEY_COMBINATIONS = 256;

    private FlussBucketPruner() {
    }

    static Optional<Set<Integer>> candidateBuckets(TableInfo tableInfo, List<Predicate> lakePredicates) {
        List<String> bucketKeys = tableInfo.getBucketKeys();
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        // Partitions may carry their own bucket count, which the split does not expose.
        if (tableInfo.isPartitioned() || bucketKeys.isEmpty() || lakeFormat == DataLakeFormat.HUDI
                || lakePredicates == null || lakePredicates.isEmpty()) {
            return Optional.empty();
        }

        RowType keyRowType = tableInfo.getRowType().project(bucketKeys);
        Map<String, Set<Object>> constraints = new HashMap<>();
        for (Predicate conjunct : flattenConjuncts(lakePredicates)) {
            Optional<FieldValues> fieldValues = valuesOf(conjunct);
            if (!fieldValues.isPresent() || !bucketKeys.contains(fieldValues.get().fieldName)) {
                continue;
            }
            FieldValues fv = fieldValues.get();
            DataType keyType = keyRowType.getTypeAt(keyRowType.getFieldIndex(fv.fieldName));
            if (fv.type.getTypeRoot() != keyType.getTypeRoot() || !allEncodable(fv.values, keyType)) {
                return Optional.empty();
            }
            constraints.merge(fv.fieldName, fv.values, (existing, added) -> {
                Set<Object> intersection = new LinkedHashSet<>(existing);
                intersection.retainAll(added);
                return intersection;
            });
        }

        long combinations = 1;
        for (String key : bucketKeys) {
            Set<Object> values = constraints.get(key);
            // An empty intersection means the filter is unsatisfiable; leave that to the scan rather than
            // trust it for pruning.
            if (values == null || values.isEmpty()) {
                return Optional.empty();
            }
            combinations *= values.size();
            if (combinations > MAX_KEY_COMBINATIONS) {
                return Optional.empty();
            }
        }

        KeyEncoder encoder = KeyEncoder.ofBucketKeyEncoder(keyRowType, bucketKeys, lakeFormat);
        BucketingFunction bucketing = BucketingFunction.of(lakeFormat);
        int numBuckets = tableInfo.getNumBuckets();
        Set<Integer> buckets = new TreeSet<>();
        for (List<Object> keyValues : cartesian(bucketKeys, constraints)) {
            GenericRow row = new GenericRow(keyValues.size());
            for (int i = 0; i < keyValues.size(); i++) {
                row.setField(i, keyValues.get(i));
            }
            buckets.add(bucketing.bucketing(encoder.encodeKey(row), numBuckets));
        }
        return Optional.of(buckets);
    }

    private static List<Predicate> flattenConjuncts(List<Predicate> predicates) {
        List<Predicate> conjuncts = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (predicate instanceof CompoundPredicate
                    && ((CompoundPredicate) predicate).function() instanceof And) {
                conjuncts.addAll(flattenConjuncts(((CompoundPredicate) predicate).children()));
            } else {
                conjuncts.add(predicate);
            }
        }
        return conjuncts;
    }

    /** Values a single field is restricted to by {@code f = v}, {@code f IN (...)} or an OR of those on {@code f}. */
    private static Optional<FieldValues> valuesOf(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            if (!(leaf.function() instanceof Equal) && !(leaf.function() instanceof In)) {
                return Optional.empty();
            }
            Set<Object> values = new LinkedHashSet<>();
            for (Object literal : leaf.literals()) {
                if (literal == null) {
                    return Optional.empty();
                }
                values.add(literal);
            }
            return Optional.of(new FieldValues(leaf.fieldName(), leaf.type(), values));
        }
        if (predicate instanceof CompoundPredicate && ((CompoundPredicate) predicate).function() instanceof Or) {
            FieldValues union = null;
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                Optional<FieldValues> childValues = valuesOf(child);
                if (!childValues.isPresent()
                        || (union != null && !union.fieldName.equals(childValues.get().fieldName))) {
                    return Optional.empty();
                }
                if (union == null) {
                    union = childValues.get();
                } else {
                    union.values.addAll(childValues.get().values);
                }
            }
            return Optional.ofNullable(union);
        }
        return Optional.empty();
    }

    private static boolean allEncodable(Set<Object> values, DataType type) {
        Class<?> expected = internalClass(type.getTypeRoot());
        if (expected == null) {
            return false;
        }
        for (Object value : values) {
            if (!expected.isInstance(value)) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> internalClass(DataTypeRoot root) {
        switch (root) {
            case CHAR:
            case STRING:
                return BinaryString.class;
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            default:
                return null;
        }
    }

    private static List<List<Object>> cartesian(List<String> keys, Map<String, Set<Object>> constraints) {
        List<List<Object>> result = new ArrayList<>();
        result.add(new ArrayList<>());
        for (String key : keys) {
            List<List<Object>> next = new ArrayList<>();
            for (List<Object> prefix : result) {
                for (Object value : constraints.get(key)) {
                    List<Object> combination = new ArrayList<>(prefix);
                    combination.add(value);
                    next.add(combination);
                }
            }
            result = next;
        }
        return result;
    }

    private static final class FieldValues {
        private final String fieldName;
        private final DataType type;
        private final Set<Object> values;

        private FieldValues(String fieldName, DataType type, Set<Object> values) {
            this.fieldName = fieldName;
            this.type = type;
            this.values = values;
        }
    }
}
