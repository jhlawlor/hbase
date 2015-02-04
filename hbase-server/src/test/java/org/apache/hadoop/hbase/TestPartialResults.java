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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * These tests are focused on testing how partial results appear to a client. Partial results are
 * {@link Result}s that contain only a portion of a row's complete list of cells. Partial results
 * are formed when the server reaches its maximum chunk size when trying to service a client's RPC
 * request. It is the responsibility of the scanner on the client side to recognize when partial
 * results have been returned and to take action to form the complete results. Unless the flag
 * {@link Scan#setAllowPartialResults(boolean)} has been set to true, the caller of
 * {@link ResultScanner#next()} should never see partial results.
 */
@Category(MediumTests.class)
public class TestPartialResults {
  private static final Log LOG = LogFactory.getLog(TestPartialResults.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("testTable");
  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  // Should keep this value below 10 to keep generation of expected kv's simple. If above 10 then
  // table/row/cf1/... will be followed by table/row/cf10/... instead of table/row/cf2/...
  private static int NUM_FAMILIES = 10;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 10;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 1024;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static int APPROX_TABLE_SIZE = NUM_ROWS * NUM_FAMILIES * NUM_QUALIFIERS * VALUE_SIZE;
  private static int NUM_COLS = NUM_FAMILIES * NUM_QUALIFIERS;

  // Approximation of how large the heap size of cells in our table. Should be accessed through
  // getCellHeapSize(). Value will be cached here after it is calculated for the first time
  private static long CELL_HEAP_SIZE = -1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);

    return ht;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testExpectedValuesOfPartialResults() throws Exception {
    testExpectedValuesOfPartialResults(false);
    testExpectedValuesOfPartialResults(true);
  }

  public void testExpectedValuesOfPartialResults(boolean reversed) throws Exception {
    // Scan that reconstructs partial results before sending result up to caller
    Scan partialScan = new Scan();
    partialScan.setMaxVersions();
    partialScan.setMaxResultSize(1);
    partialScan.setReversed(reversed);
    ResultScanner partialScanner = TABLE.getScanner(partialScan);

    // Loop parameters based on reversed flag
    final int startRow = reversed ? ROWS.length - 1 : 0;
    final int endRow = reversed ? -1 : ROWS.length;
    final int loopDelta = reversed ? -1 : 1;
    String message;

    for (int row = startRow; row != endRow; row = row + loopDelta) {
      message = "Ensuring the expected keyValues are present";
      List<Cell> expectedKeyValues = createRowKeyValues(ROWS[row], FAMILIES, QUALIFIERS, VALUE);
      Result partialResult = partialScanner.next();
      verifyResult(partialResult, expectedKeyValues, true, message);
    }

    partialScanner.close();
  }

  @Test
  public void testAllowPartialResults() throws Exception {
    Scan scan = new Scan();
    scan.setAllowPartialResults(true);
    scan.setMaxResultSize(1);
    ResultScanner scanner = TABLE.getScanner(scan);
    Result result = scanner.next();

    assertTrue(result != null);
    assertTrue(result.isPartial());
    assertTrue(result.rawCells() != null);
    assertTrue(result.rawCells().length == 1);

    scanner.close();
    scan.setAllowPartialResults(false);
    scanner = TABLE.getScanner(scan);
    result = scanner.next();

    assertTrue(result != null);
    assertTrue(!result.isPartial());
    assertTrue(result.rawCells() != null);
    assertTrue(result.rawCells().length == NUM_COLS);

    scanner.close();
  }

  @Test
  public void testEquivalenceOfScanResults() throws Exception {
    Scan oneShotScan = new Scan();
    oneShotScan.setMaxResultSize(Long.MAX_VALUE);
    Scan partialScan = new Scan(oneShotScan);
    partialScan.setMaxResultSize(1);

    testEquivalenceOfScanResults(TABLE, oneShotScan, partialScan);
  }

  public void testEquivalenceOfScanResults(Table table, Scan scan1, Scan scan2) throws Exception {
    ResultScanner scanner1 = table.getScanner(scan1);
    ResultScanner scanner2 = table.getScanner(scan2);

    Result r1 = null;
    Result r2 = null;
    int count = 0;

    while ((r1 = scanner1.next()) != null) {
      r2 = scanner2.next();

      assertTrue(r2 != null);
      assertTrue(compareResults(r1, r2, "Comparing result #" + count));
      count++;
    }

    assertTrue(scanner2.next() == null);

    scanner1.close();
    scanner2.close();
  }

  @Test
  public void testOrderingOfCellsInPartialResults() throws Exception {
    Scan scan = new Scan();

    for (int col = 1; col <= NUM_COLS; col++) {
      scan.setMaxResultSize(col * getCellHeapSize());
      testOrderingOfCellsInPartialResults(scan);

      // Test again with a reversed scanner
      scan.setReversed(true);
      testOrderingOfCellsInPartialResults(scan);
    }
  }

  public void testOrderingOfCellsInPartialResults(final Scan basePartialScan) throws Exception {
    // Scan that retrieves all table results in single RPC request
    Scan oneShotScan = new Scan(basePartialScan);
    oneShotScan.setMaxResultSize(Long.MAX_VALUE);
    oneShotScan.setCaching(ROWS.length);
    ResultScanner oneShotScanner = TABLE.getScanner(oneShotScan);

    // Scan that retrieves results in pieces (partials). By setting allowPartialResults to be true
    // the results will NOT be reconstructed and instead the caller will see the partial results
    // returned by the server
    Scan partialScan = new Scan(basePartialScan);
    partialScan.setAllowPartialResults(true);
    ResultScanner partialScanner = TABLE.getScanner(partialScan);

    Result oneShotResult = oneShotScanner.next();
    Result partialResult = null;
    int iterationCount = 0;

    while (oneShotResult != null && oneShotResult.rawCells() != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("iteration:" + iterationCount);
      }

      // Call partialScanner.next() one time per cell returned in the oneShotResult since each
      // Result returned from partialScanner.next() will contain a single cell
      List<Cell> aggregatePartialCells = new ArrayList<Cell>();
      for (int i = 0; i < oneShotResult.rawCells().length; i++) {  
        partialResult = partialScanner.next();
        assertTrue("Partial Result is null", partialResult != null);
        assertTrue("Partial cells are null", partialResult.rawCells() != null);
        assertTrue("Partial cell count should be 1", partialResult.rawCells().length == 1);
        assertTrue("Partial cell is null", partialResult.rawCells()[0] != null);
        aggregatePartialCells.add(partialResult.rawCells()[0]);
      }

      assertTrue("Number of cells differs",
        oneShotResult.rawCells().length == aggregatePartialCells.size());
      final Cell[] oneShotCells = oneShotResult.rawCells();
      for (int cell = 0; cell < oneShotCells.length; cell++) {
        Cell oneShotCell = oneShotCells[cell];
        Cell partialCell = aggregatePartialCells.get(cell);

        if (LOG.isInfoEnabled()) {
          LOG.info("oneShotCell is: " + oneShotCell);
          LOG.info("partialCell is: " + partialCell);
        }

        assertTrue("One shot cell was null", oneShotCell != null);
        assertTrue("Partial cell was null", partialCell != null);
        assertTrue("Cell differs", oneShotCell.equals(partialCell));
      }

      oneShotResult = oneShotScanner.next();
      iterationCount++;
    }
    
    assertTrue(partialScanner.next() == null);

    partialScanner.close();
    oneShotScanner.close();
  }

  /**
   * @throws Exception
   */
  @Test
  public void testExpectedNumberOfCellsPerPartialResult() throws Exception {
    Scan scan = new Scan();
    testExpectedNumberOfCellsPerPartialResult(scan);

    scan.setReversed(true);
    testExpectedNumberOfCellsPerPartialResult(scan);
  }

  public void testExpectedNumberOfCellsPerPartialResult(Scan baseScan) throws Exception {
    // Increase the number of expected number of cells up to the maximum of NUM_COLS
    for (int expectedCells = 1; expectedCells <= NUM_COLS; expectedCells++) {
      testExpectedNumberOfCellsPerPartialResult(baseScan, expectedCells);
    }
  }

  /**
   * @param baseScan
   * @param expectedNumberOfCells
   * @param cellHeapSize
   * @throws Exception
   */
  public void testExpectedNumberOfCellsPerPartialResult(Scan baseScan, int expectedNumberOfCells)
      throws Exception {
    // Use the cellHeapSize to set maxResultSize such that we know how many cells to expect back
    // from the call. The returned results should NOT exceed expectedNumberOfCells but may be less
    // than it in cases where expectedNumberOfCells is not an exact multiple of the number of
    // columns in the table.
    Scan scan = new Scan(baseScan);
    scan.setAllowPartialResults(true);
    scan.setMaxResultSize(expectedNumberOfCells * getCellHeapSize());

    ResultScanner scanner = TABLE.getScanner(scan);
    Result result = null;
    byte[] prevRow = null;

    while ((result = scanner.next()) != null) {
      assertTrue(result.rawCells() != null);
      if (LOG.isInfoEnabled()) {
        LOG.info("result.rawCells().length:" + result.rawCells().length);
        LOG.info("result.isPartial():" + result.isPartial());
        LOG.info("result row: " + Bytes.toString(result.getRow()));
        LOG.info("groupSize:" + expectedNumberOfCells);
      }

      // Cases when cell count won't equal groupSize:
      // 1. Returned result is the final result needed to form the complete result for that row
      // 2. It is the first result we have seen for that row
      assertTrue(result.rawCells().length == expectedNumberOfCells || !result.isPartial()
          || !Bytes.equals(prevRow, result.getRow()));
      prevRow = result.getRow();
    }

    scanner.close();
  }

  private long getCellHeapSize() throws Exception {
    if (CELL_HEAP_SIZE == -1) {
      // Do a partial scan that will return a single result with a single cell
      Scan scan = new Scan();
      scan.setMaxResultSize(1);
      scan.setAllowPartialResults(true);
      ResultScanner scanner = TABLE.getScanner(scan);

      Result result = scanner.next();

      assertTrue(result != null);
      assertTrue(result.rawCells() != null);
      assertTrue(result.rawCells().length == 1);

      CELL_HEAP_SIZE = CellUtil.estimatedHeapSizeOf(result.rawCells()[0]);
      if (LOG.isInfoEnabled()) LOG.info("Cell heap size: " + CELL_HEAP_SIZE);
      scanner.close();
    }

    return CELL_HEAP_SIZE;
  }

  @Test
  public void testPartialResultsAndBatch() throws Exception {

  }

  @Test
  public void testPartialResultsAndFilters() throws Exception {

  }

  @Test
  public void testPartialResultsAndCaching() throws Exception {

  }

  @Test
  public void testPartialResultsAndOneHugeRow() throws Exception {

  }

  @Test
  public void testPartialResultsReassembly() throws Exception {

  }

  @Test
  public void testScannerResetOnPartialResultError() throws Exception {

  }

  @Test
  public void testPartialResultScannerCase1() throws Exception {

  }

  @Test
  public void testPartialResultScannerCase2() throws Exception {

  }

  @Test
  public void testPartialResultScannerCase3() throws Exception {

  }

  @Test
  public void testPartialResultScannerCase4() throws Exception {

  }

  @Test
  public void testCachingSetToLongMax() throws Exception {

  }

  @Test
  public void testCoprocessorsAndPartials() throws Exception {

  }

  @Test
  public void testSmallScannersDoNotAllowPartials() throws Exception {

  }

  @Test
  public void testPartialResultsDoNotAllowFilters() throws Exception {

  }

  @Test
  public void testPartialResultRegionSplit() throws Exception {

  }

  /**
   * Make puts to put the input value into each combination of row, family, and qualifier
   * @param rows
   * @param families
   * @param qualifiers
   * @param value
   * @return
   * @throws IOException
   */
  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  /**
   * Make key values to represent each possible combination of family and qualifier in the specified
   * row.
   * @param row
   * @param families
   * @param qualifiers
   * @param value
   * @return
   */
  static ArrayList<Cell> createRowKeyValues(byte[] row, byte[][] families, byte[][] qualifiers,
      byte[] value) {
    ArrayList<Cell> outList = new ArrayList<>();
    for (int fam = 0; fam < families.length; fam++) {
      for (int qual = 0; qual < qualifiers.length; qual++) {
        outList.add(new KeyValue(row, families[fam], qualifiers[qual], qual, value));
      }
    }
    return outList;
  }

  static void verifyResult(Result result, List<Cell> expKvList, boolean toLog, String msg) {
    if (LOG.isInfoEnabled()) {
      LOG.info(msg);
      LOG.info("Expected count: " + expKvList.size());
      LOG.info("Actual count: " + result.size());
    }

    if (expKvList.size() == 0) return;

    int i = 0;
    for (Cell kv : result.rawCells()) {
      if (i >= expKvList.size()) {
        break; // we will check the size later
      }

      Cell kvExp = expKvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue("Not equal", kvExp.equals(kv));
    }

    assertEquals(expKvList.size(), result.size());
  }

  static boolean compareResults(Result r1, Result r2, final String message) {
    if (LOG.isInfoEnabled()) {
      LOG.info(message);
      LOG.info("r1: " + r1);
      LOG.info("r2: " + r2);
    }

    if (r1 == null && r2 == null) return true;
    else if (r1 == null || r2 == null) return false;

    try {
      Result.compareResults(r1, r2);
    } catch (Exception e) {
      fail("Results r1:" + r1 + " r2:" + r2 + " are not equivalent");
      return false;
    }

    return true;
  }

  public void testReadPoint() throws IOException {
    // TODO: should create a test that uses batching/rpcChunkSize to confirm read point is
    // maintained.
    // The test would open a scanner and fetch a batch. This would be followed by a delete of the
    // table or row being read. And finally followed by another next() call to confirm we can see
    // the old read point's values (prior to delete).
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("t"));

    Scan scan = new Scan();

    ResultScanner scanner = table.getScanner(scan);
    System.out.println("opened scanner for table t");

    Delete delete1 = new Delete(Bytes.toBytes("r1"));
    delete1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
    table.delete(delete1);
    System.out.println("deleted t:r1:cf:b");

    Delete delete2 = new Delete(Bytes.toBytes("r2"));
    delete2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
    table.delete(delete2);
    System.out.println("deleted t:r2:cf:b");

    Result result;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration");
      for (Cell cell : result.rawCells()) {
        // printCellSummary(cell);
      }
    }
    System.out.println("---1---");
    scanner.close();
    System.out.println("closed scanner for table t");

    System.out.println("opened scanner for table t");
    scanner = table.getScanner(scan);
    while ((result = scanner.next()) != null) {
      System.out.println("iteration");
      for (Cell cell : result.rawCells()) {
        // printCellSummary(cell);
      }
    }
    System.out.println("---2---");
    scanner.close();
    System.out.println("closed scanner for table t");

    System.out.println("opened scanner for table t");
    scanner = table.getScanner(scan);

    Put put1 = new Put(Bytes.toBytes("r1"));
    put1.add(Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes("v2"));
    table.put(put1);
    System.out.println("put t:r1:cf:b");

    Put put2 = new Put(Bytes.toBytes("r2"));
    put2.add(Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes("v2"));
    table.put(put2);
    System.out.println("put t:r2:cf:b");

    while ((result = scanner.next()) != null) {
      System.out.println("iteration");
      for (Cell cell : result.rawCells()) {
        // printCellSummary(cell);
      }
    }
    System.out.println("---3---");
    scanner.close();
    System.out.println("closed scanner for table t");

    System.out.println("opened scanner for table t");
    scanner = table.getScanner(scan);
    while ((result = scanner.next()) != null) {
      System.out.println("iteration");
      for (Cell cell : result.rawCells()) {
        // printCellSummary(cell);
      }
    }
    System.out.println("---4---");

  }
}
