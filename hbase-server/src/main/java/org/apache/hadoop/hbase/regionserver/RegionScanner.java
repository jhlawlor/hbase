/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * RegionScanner describes iterators over rows in an HRegion.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Stable
public interface RegionScanner extends InternalScanner {
  /**
   * @return The RegionInfo for this scanner.
   */
  HRegionInfo getRegionInfo();

  /**
   * @return True if a filter indicates that this scanner will return no further rows.
   * @throws IOException in case of I/O failure on a filter.
   */
  boolean isFilterDone() throws IOException;

  /**
   * Do a reseek to the required row. Should not be used to seek to a key which
   * may come before the current position. Always seeks to the beginning of a
   * row boundary.
   *
   * @throws IOException
   * @throws IllegalArgumentException
   *           if row is null
   *
   */
  boolean reseek(byte[] row) throws IOException;

  /**
   * @return The preferred max buffersize. See 
   * {@link org.apache.hadoop.hbase.client.Scan#setMaxResultSize(long)}
   */
  long getMaxResultSize();

  /**
   * @return The Scanner's MVCC readPt see {@link MultiVersionConsistencyControl}
   */
  long getMvccReadPoint();
  
  /**
   * @return The limit on the number of cells to retrieve on each call to next(). See
   *         {@link org.apache.hadoop.hbase.client.Scan#setBatch(int)}
   */
  int getBatch();

  /**
   * Enumeration of possible states that scanner can be in after a call to
   * {@link RegionScanner#nextRaw(List)}. The enumeration is necessary so that we can communicate
   * the state of the scanner to whoever make calls to this method
   * <p>
   * {@link NextRawState#moreRows()} can be used to determine whether or not this state indicates
   * that the scanner has more rows to be scanned
   */
  public enum NextRawState {
    HasMoreRows(true), 
    NoMoreRows(false), 
    BatchLimitReached(true), 
    SizeLimitReached(true);
    
    private boolean moreRows;
    NextRawState(final boolean moreRows) {
      this.moreRows = moreRows;
    }

    /**
     * @return true when the state indicates that there are more rows left to be scanned
     */
    public boolean moreRows() {
      return this.moreRows;
    }
  }

  /**
   * Grab the next row's worth of values with the default limit on the number of values to return.
   * This is a special internal method to be called from coprocessor hooks to avoid expensive setup.
   * Caller must set the thread's readpoint, start and close a region operation, an synchronize on
   * the scanner object. Caller should maintain and update metrics. See
   * {@link #nextRaw(List, long, int)}
   * @param result return output array
   * @return a state where {@link NextRawState#moreRows()} is true when more rows exist, false when
   *         scanner is done.
   * @throws IOException e
   */
  NextRawState nextRaw(List<Cell> result) throws IOException;

  /**
   * Grab the next row's worth of values with the default limit on the number of values to return.
   * This is a special internal method to be called from coprocessor hooks to avoid expensive setup.
   * Caller must set the thread's readpoint, start and close a region operation, an synchronize on
   * the scanner object. Caller should maintain and update metrics. See
   * {@link #nextRaw(List, long, int)}
   * @param result return output array
   * @param limit limit on row count to get
   * @return a state where {@link NextRawState#moreRows()} is true when more rows exist, false when
   *         scanner is done.
   * @throws IOException e
   */
  NextRawState nextRaw(List<Cell> result, int limit) throws IOException;
  
  /**
   * Grab the next row's worth of values with a limit on the number of values to return. This is a
   * special internal method to be called from coprocessor hooks to avoid expensive setup. Caller
   * must set the thread's readpoint, start and close a region operation, an synchronize on the
   * scanner object. Example: <code><pre>
   * HRegion region = ...;
   * RegionScanner scanner = ...
   * MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
   * region.startRegionOperation();
   * try {
   *   synchronized(scanner) {
   *     ...
   *     boolean moreRows = scanner.nextRaw(values);
   *     ...
   *   }
   * } finally {
   *   region.closeRegionOperation();
   * }
   * </pre></code>
   * @param result return output array
   * @param limit limit on row count to get
   * @param remainingResultSize the space remaining within the restriction on the result size. Non
   *          positive values indicate no limit
   * @return a state where {@link NextRawState#moreRows()} is true when more rows exist, false when
   *         scanner is done.
   * @throws IOException e
   */
  NextRawState nextRaw(List<Cell> result, int limit, final long remainingResultSize)
      throws IOException;
}
