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

package com.starrocks.fluss.reader;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.LakeSnapshotAndLogSplitScanner;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import javax.annotation.Nullable;

public class FlussSnapshotAndLogScanner extends ConnectorScannerProxy {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final Table table;
    private final LakeSource<LakeSplit> lakeSource;
    private final LakeSnapshotAndFlussLogSplit split;
    @Nullable
    private final int[] projectedFields;
    private BatchScanner batchScanner;
    private CloseableIterator<InternalRow> currentBatch;
    private boolean finished = false;

    public FlussSnapshotAndLogScanner(Table table, LakeSource<LakeSplit> lakeSource,
                                      LakeSnapshotAndFlussLogSplit split,
                                      @Nullable int[] projectedFields) {
        this.table = table;
        this.lakeSource = lakeSource;
        this.split = split;
        this.projectedFields = projectedFields;
    }

    @Override
    void open() throws IOException {
        // Fluss 1.0.0 moved the bounded snapshot+log reader to
        // client.table.scanner.batch and dropped the single-split constructor;
        // feed it the split's lake splits plus the log offset range, mirroring
        // the bounded branch of Fluss's own LakeSplitReaderGenerator.
        if (split.isStreaming()) {
            throw new IOException("Streaming lake+log splits are not supported by the StarRocks Fluss reader: " + split);
        }
        long stoppingOffset = split.getStoppingOffset().orElseThrow(
                () -> new IOException("Bounded lake+log split is missing a stopping offset: " + split));
        batchScanner = new LakeSnapshotAndLogSplitScanner(
                table, lakeSource, split.getLakeSplits(), split.getTableBucket(),
                split.getStartingOffset(), stoppingOffset, projectedFields);
    }

    @Override
    boolean hasNext() throws IOException {
        while (true) {
            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }
            if (finished) {
                return false;
            }

            // An empty batch is transient; only null marks the end of input.
            currentBatch = batchScanner.pollBatch(POLL_TIMEOUT);
            if (currentBatch == null) {
                finished = true;
                return false;
            }
        }
    }

    @Override
    InternalRow getNext() throws IOException {
        return currentBatch.next();
    }

    @Override
    void close() throws IOException {
        if (batchScanner != null) {
            batchScanner.close();
        }
    }
}
