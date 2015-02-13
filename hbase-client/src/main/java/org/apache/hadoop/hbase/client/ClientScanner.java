/**
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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ReversedClientScanner.createClosestRowBefore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all.
 */
@InterfaceAudience.Private
public class ClientScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());
    protected Scan scan;
    protected boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    protected HRegionInfo currentRegion = null;
    protected ScannerCallableWithReplicas callable = null;
    protected final LinkedList<Result> cache = new LinkedList<Result>();
    protected final LinkedList<Result> partialResults = new LinkedList<Result>();
    protected int numberOfCellsInPartialResults = 0;
    protected final int caching;
    protected long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    protected Result lastResult = null;
    // Keep a flag to tell us whether or not a partial result was returned from a call to next
    protected boolean partialResultReturned = false;
    protected final long maxScannerResultSize;
    private final ClusterConnection connection;
    private final TableName tableName;
    protected final int scannerTimeout;
    protected boolean scanMetricsPublished = false;
    protected RpcRetryingCaller<Result []> caller;
    protected RpcControllerFactory rpcControllerFactory;
    protected Configuration conf;
    //The timeout on the primary. Applicable if there are multiple replicas for a region
    //In that case, we will only wait for this much timeout on the primary before going
    //to the replicas and trying the same scan. Note that the retries will still happen
    //on each replica and the first successful results will be taken. A timeout of 0 is
    //disallowed.
    protected final int primaryOperationTimeout;
    private int retries;
    protected final ExecutorService pool;

  /**
   * Create a new ClientScanner for the specified table Note that the passed {@link Scan}'s start
   * row maybe changed changed.
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   * @param connection Connection identifying the cluster
   * @throws IOException
   */
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      ClusterConnection connection, RpcRetryingCallerFactory rpcFactory,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout)
      throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Scan table=" + tableName
            + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
      }
      this.scan = scan;
      this.tableName = tableName;
      this.lastNext = System.currentTimeMillis();
      this.connection = connection;
      this.pool = pool;
      this.primaryOperationTimeout = primaryOperationTimeout;
      this.retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      if (scan.getMaxResultSize() > 0) {
        this.maxScannerResultSize = scan.getMaxResultSize();
      } else {
        this.maxScannerResultSize = conf.getLong(
          HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
      }
      this.scannerTimeout = HBaseConfiguration.getInt(conf,
        HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

      // check if application wants to collect scan metrics
      initScanMetrics(scan);

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = conf.getInt(
            HConstants.HBASE_CLIENT_SCANNER_CACHING,
            HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
      }

      this.caller = rpcFactory.<Result[]> newCaller();
      this.rpcControllerFactory = controllerFactory;

      this.conf = conf;
      initializeScannerInConstruction();
    }

    protected void initializeScannerInConstruction() throws IOException{
      // initialize the scanner
      nextScanner(this.caching, false);
    }

    protected ClusterConnection getConnection() {
      return this.connection;
    }

    /**
     * @return Table name
     * @deprecated Since 0.96.0; use {@link #getTable()}
     */
    @Deprecated
    protected byte [] getTableName() {
      return this.tableName.getName();
    }

    protected TableName getTable() {
      return this.tableName;
    }

    protected int getRetries() {
      return this.retries;
    }

    protected int getScannerTimeout() {
      return this.scannerTimeout;
    }

    protected Configuration getConf() {
      return this.conf;
    }

    protected Scan getScan() {
      return scan;
    }

    protected ExecutorService getPool() {
      return pool;
    }

    protected int getPrimaryOperationTimeout() {
      return primaryOperationTimeout;
    }

    protected int getCaching() {
      return caching;
    }

    protected long getTimestamp() {
      return lastNext;
    }

    // returns true if the passed region endKey
    protected boolean checkScanStopRow(final byte [] endKey) {
      if (this.scan.getStopRow().length > 0) {
        // there is a stop row, check to see if we are past it.
        byte [] stopRow = scan.getStopRow();
        int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
        if (cmp <= 0) {
          // stopRow <= endKey (endKey is equals to or larger than stopRow)
          // This is a stop.
          return true;
        }
      }
      return false; //unlikely.
    }

    private boolean possiblyNextScanner(int nbRows, final boolean done) throws IOException {
      // If we have just switched replica, don't go to the next scanner yet. Rather, try
      // the scanner operations on the new replica, from the right point in the scan
      // Note that when we switched to a different replica we left it at a point
      // where we just did the "openScanner" with the appropriate startrow
      if (callable != null && callable.switchedToADifferentReplica()) return true;
      return nextScanner(nbRows, done);
    }

    /*
     * Gets a scanner for the next region.  If this.currentRegion != null, then
     * we will move to the endrow of this.currentRegion.  Else we will get
     * scanner at the scan.getStartRow().  We will go no further, just tidy
     * up outstanding scanners, if <code>currentRegion != null</code> and
     * <code>done</code> is true.
     * @param nbRows
     * @param done Server-side says we're done scanning.
     */
  protected boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        call(callable, caller, scannerTimeout);
        this.callable = null;
      }

      // Where to start the next scanner
      byte [] localStartKey;

      // if we're at end of table, close and return false to stop iterating
      if (this.currentRegion != null) {
        byte [] endKey = this.currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            checkScanStopRow(endKey) ||
            done) {
          close();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Finished " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Finished " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (LOG.isDebugEnabled() && this.currentRegion != null) {
        // Only worth logging if NOT first region in scan.
        LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        call(callable, caller, scannerTimeout);
        this.currentRegion = callable.getHRegionInfo();
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.incrementAndGet();
        }
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

  @VisibleForTesting
  boolean isAnyRPCcancelled() {
    return callable.isAnyRPCcancelled();
  }

  static Result[] call(ScannerCallableWithReplicas callable,
      RpcRetryingCaller<Result[]> caller, int scannerTimeout)
      throws IOException, RuntimeException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
    // we do a callWithRetries
    return caller.callWithoutRetries(callable, scannerTimeout);
  }

    @InterfaceAudience.Private
    protected ScannerCallableWithReplicas getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s =
          new ScannerCallable(getConnection(), getTable(), scan, this.scanMetrics,
              this.rpcControllerFactory);
      s.setCaching(nbRows);
      ScannerCallableWithReplicas sr = new ScannerCallableWithReplicas(tableName, getConnection(),
       s, pool, primaryOperationTimeout, scan,
       retries, scannerTimeout, caching, conf, caller);
      return sr;
    }

    /**
     * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
     * application or TableInputFormat.Later, we could push it to other systems. We don't use
     * metrics framework because it doesn't support multi-instances of the same metrics on the same
     * machine; for scan/map reduce scenarios, we will have multiple scans running at the same time.
     *
     * By default, scan metrics are disabled; if the application wants to collect them, this
     * behavior can be turned on by calling calling:
     *
     * scan.setAttribute(SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE))
     */
    protected void writeScanMetrics() {
      if (this.scanMetrics == null || scanMetricsPublished) {
        return;
      }
      MapReduceProtos.ScanMetrics pScanMetrics = ProtobufUtil.toScanMetrics(scanMetrics);
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, pScanMetrics.toByteArray());
      scanMetricsPublished = true;
    }

  @Override
  public Result next() throws IOException {
    // If the scanner is closed and there's nothing left in the cache, next is a no-op.
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
      Result[] values = null;
      long remainingResultSize = maxScannerResultSize;
      int countdown = this.caching;

      // We need to reset it if it's a new callable that was created
      // with a countdown in nextScanner
      callable.setCaching(this.caching);
      // This flag is set when we want to skip the result returned. We do
      // this when we reset scanner because it split under us.
      boolean retryAfterOutOfOrderException = true;
      do {
        try {
          // Reset flag at beginning of each loop
          partialResultReturned = false;

          // Server returns a null values if scanning is to stop. Else,
          // returns an empty array if scanning is to go on and we've just
          // exhausted current region.
          values = call(callable, caller, scannerTimeout);

          // When the replica switch happens, we need to do certain operations
          // again. The callable will openScanner with the right startkey
          // but we need to pick up from there. Bypass the rest of the loop
          // and let the catch-up happen in the beginning of the loop as it
          // happens for the cases where we see exceptions. Since only openScanner
          // would have happened, values would be null
          if (values == null && callable.switchedToADifferentReplica()) {
            this.currentRegion = callable.getHRegionInfo();
            continue;
          }
          retryAfterOutOfOrderException = true;
        } catch (DoNotRetryIOException e) {
          // DNRIOEs are thrown to make us break out of retries. Some types of DNRIOEs want us
          // to reset the scanner and come back in again.
          if (e instanceof UnknownScannerException) {
            long timeout = lastNext + scannerTimeout;
            // If we are over the timeout, throw this exception to the client wrapped in
            // a ScannerTimeoutException. Else, it's because the region moved and we used the old
            // id against the new region server; reset the scanner.
            if (timeout < System.currentTimeMillis()) {
              long elapsed = System.currentTimeMillis() - lastNext;
              ScannerTimeoutException ex =
                  new ScannerTimeoutException(elapsed + "ms passed since the last invocation, "
                      + "timeout is currently set to " + scannerTimeout);
              ex.initCause(e);
              throw ex;
            }
          } else {
            // If exception is any but the list below throw it back to the client; else setup
            // the scanner and retry.
            Throwable cause = e.getCause();
            if ((cause != null && cause instanceof NotServingRegionException)
                || (cause != null && cause instanceof RegionServerStoppedException)
                || e instanceof OutOfOrderScannerNextException) {
              // Pass
              // It is easier writing the if loop test as list of what is allowed rather than
              // as a list of what is not allowed... so if in here, it means we do not throw.
            } else {
              throw e;
            }
          }
          // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
          if (this.lastResult != null) {
            // The region has moved. We need to open a brand new scanner at
            // the new location.
            // Reset the startRow to the row we've seen last so that the new
            // scanner starts at the correct row. Otherwise we may see previously
            // returned rows again.
            // (ScannerCallable by now has "relocated" the correct region)
            if (scan.isReversed()) {
              scan.setStartRow(createClosestRowBefore(lastResult.getRow()));
            } else {
              scan.setStartRow(Bytes.add(lastResult.getRow(), new byte[1]));
            }
          }
          if (e instanceof OutOfOrderScannerNextException) {
            if (retryAfterOutOfOrderException) {
              retryAfterOutOfOrderException = false;
            } else {
              // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
              throw new DoNotRetryIOException("Failed after retry of "
                  + "OutOfOrderScannerNextException: was there a rpc timeout?", e);
            }
          }
          // Clear region.
          this.currentRegion = null;
          // Set this to zero so we don't try and do an rpc and close on remote server when
          // the exception we got was UnknownScanner or the Server is going down.
          callable = null;

          // Need to reset the partialResults so that old partials do not stick around
          partialResults.clear();

          // This continue will take us to while at end of loop where we will set up new scanner.
          continue;
        }
        long currentTime = System.currentTimeMillis();
        if (this.scanMetrics != null) {
          this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
        }
        lastNext = currentTime;
        List<Result> resultsToAddToCache = handlePartialResults(values);
        if (!resultsToAddToCache.isEmpty()) {
          for (Result rs : resultsToAddToCache) {
            cache.add(rs);
            // We don't make Iterator here
            for (Cell cell : rs.rawCells()) {
              remainingResultSize -= CellUtil.estimatedHeapSizeOf(cell);
            }
            countdown--;
            this.lastResult = rs;
          }
        }
        // Values == null means server-side filter has determined we must STOP
      } while (remainingResultSize > 0 && countdown > 0
          && (partialResultReturned || possiblyNextScanner(countdown, values == null)));
    }

    if (cache.size() > 0) {
      return cache.poll();
    }

    // if we exhausted this scanner before calling close, write out the scan metrics
    writeScanMetrics();
    return null;
  }

  /**
   * This method ensures all of our book keeping regarding partial results is kept up to date. This
   * method should be called once we know that the results we received back from the RPC request do
   * not contain errors. We return a list of results that should be added to the cache. In general,
   * this list will contain all NON-partial results from the input array (unless the client has
   * specified that they are okay with receiving partial results)
   * @return the list of results that should be added to the cache.
   */
  protected List<Result> handlePartialResults(Result[] results) {
    List<Result> resultsToAddToCache = new ArrayList<Result>();

    // If the caller has indicated in their scan that they are okay with seeing partial results,
    // then simply add all results to the list.
    if (scan != null && scan.getAllowPartialResults()) {
      addResultsFromArray(resultsToAddToCache, results, 0, results.length);
      return resultsToAddToCache;
    }

    // TODO: are we sure this shouldn't be included in the next if statement as an OR
    if (results == null) return resultsToAddToCache;

    if (results.length == 0) {
      if (!partialResults.isEmpty()) {
        resultsToAddToCache.add(Result.createCompleteResult(partialResults, null));
        partialResults.clear();
      }

      return resultsToAddToCache;
    }

    // At any given time there should only ever be a single partial result returned. Furthermore, if
    // there is a partial result, it is guaranteed to be in the last spot. This should be true
    // because a partial result is formed when the server reaches its result size limit. When this
    // limit is reached, the row being traversed will be returned in fragments (partials) that must
    // be reconstructed on the client side.
    Result last = results[results.length - 1];
    Result partial = last.isPartial() ? last : null;
    partialResultReturned = partial != null;

    // TODO: remove; logging stuff -- replace with actual logs
    System.out.println("\n===START:handlePartialResultsLogs===");
    System.out.println("partialResultReturned: " + partialResultReturned);
    System.out.println("contents of results returned: ");
    long sizeEst = 0;
    for (int i = 0; i < results.length; i++) {
      System.out.println("\tresult.toString(): " + results[i].toString());
      System.out.println("\tcell count: " + results[i].rawCells().length);
      for (Cell c : results[i].rawCells()) {
        System.out.println("\t\tCell: " + c.toString() + " size: "
            + CellUtil.estimatedHeapSizeOf(c));

        sizeEst += CellUtil.estimatedHeapSizeOf(c);
      }
    }
    System.out.println("Size estimate: " + sizeEst);

    // There are four possibilities when a partial result is returned:
    //
    // 1. (partial != null && partialResults.isEmpty())
    //
    // This is the first partial result that we have received. It should be added to
    // the list of partialResults and await the next RPC request at which point another
    // portion of the complete result will be received
    //
    // 2. (partial != null && !partialResults.isEmpty())
    //
    // a. values.length == 1
    // Since partialResults contains some elements, it means that we are expecting to receive
    // the remainder of the complete result within this RPC request. The fact that a partial result
    // was returned and it's the ONLY result returned indicates that we are still receiving
    // fragments of the complete result. The Result can be completely formed only when we have
    // received all of the fragments and thus in this case we simply add the partial result to
    // our list.
    //
    // b. values.length > 1
    // More than one result has been returned from the server. The fact that we are accumulating
    // partials in partialList and we just received more than one result back from the server
    // indicates that the FIRST result we received from the server must be the final fragment that
    // can be used to complete our result. What this means is that the partial that we received is
    // a partial result for a different row, and at this point we should combine the existing
    // partials into a complete result, clear the partialList, and add the new partial to the list
    //
    // 3. (partial == null && !partialResults.isEmpty())
    //
    // No partial was received but we are accumulating partials in our list. That means the final
    // fragment of the complete result will be the first Result in values[]. Should take it,
    // create the complete Result, clear the list, and add it to the list of Results that must be
    // added to the cache. All other Results in values[] are added after the complete result
    //
    // 4. (partial == null && partialResults.isEmpty())
    //
    // Business as usual. We are not accumulating partial results and there wasn't a partial result
    // in the RPC response. This means that all of the results we received from the server are
    // complete and can be added directly to the cache

    if (partial != null && partialResults.isEmpty()) {
      System.out.println("ClientScanner: case 1");
      partialResults.add(partial);

      // Exclude the last result, it's a partial
      addResultsFromArray(resultsToAddToCache, results, 0, results.length - 1);
    } else if (partial != null && !partialResults.isEmpty()) {
      System.out.println("ClientScanner: case 2");
      if (results.length > 1) {
        Result finalResult = results[0];
        resultsToAddToCache.add(Result.createCompleteResult(partialResults, finalResult));
        partialResults.clear();

        // Exclude first result, it was used to form our complete result
        // Exclude last result, it's a partial result
        addResultsFromArray(resultsToAddToCache, results, 1, results.length - 1);
      }
      partialResults.add(partial);
    } else if (partial == null && !partialResults.isEmpty()) {
      System.out.println("ClientScanner: case 3");
      Result finalResult = results[0];
      resultsToAddToCache.add(Result.createCompleteResult(partialResults, finalResult));
      partialResults.clear();

      // Exclude the first result, it was used to form our complete result
      addResultsFromArray(resultsToAddToCache, results, 1, results.length);
    } else { // partial == null && partialResults.isEmpty() -- business as usual
      System.out.println("ClientScanner: case 4");
      addResultsFromArray(resultsToAddToCache, results, 0, results.length);
    }

    // TODO: In the case that Scan#setBatch has been set and a single batch does not fit into the
    // defined maxResultSize, the batches will be returned as partials of varying sizes. It
    // is our responsibility to reconstruct the partial Results into results containing the
    // appropriate number of cells (i.e. each result should contain Scan#batch number of cells
    // except if it is the final set of cells for the row). This is where we ensure that
    System.out.println("===END:handlePartialResultsLogs===");
    System.out.println();
    return resultsToAddToCache;
  }

  /**
   * Helper method for adding results between the indices [start, end] to the outputList
   * @param outputList the list that results will be added to
   * @param inputArray the array that results are taken from
   * @param start beginning index (inclusive)
   * @param end ending index (exclusive)
   */
  private void addResultsFromArray(List<Result> outputList, Result[] inputArray, int start, int end) {
    if (start < 0 || end > inputArray.length) return;

    for (int i = start; i < end; i++) {
      outputList.add(inputArray[i]);
    }
  }

    @Override
    public void close() {
      if (!scanMetricsPublished) writeScanMetrics();
      if (callable != null) {
        callable.setClose();
        try {
          call(callable, caller, scannerTimeout);
        } catch (UnknownScannerException e) {
           // We used to catch this error, interpret, and rethrow. However, we
           // have since decided that it's not nice for a scanner's close to
           // throw exceptions. Chances are it was just due to lease time out.
          if (LOG.isDebugEnabled()) {
            LOG.debug("scanner failed to close", e);
          }
        } catch (IOException e) {
          /* An exception other than UnknownScanner is unexpected. */
          LOG.warn("scanner failed to close.", e);
        }
        callable = null;
      }
      closed = true;
    }
}
