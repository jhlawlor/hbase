package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPartialResult {

  @Test
  public void testPullFatRow() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("t"));
    ResultScanner scanner = table.getScanner(new Scan());
    scanner.next();
  }

  @Test
  public void testDefaults() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testScanCaching() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final int caching = 1;
    scan.setCaching(caching);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
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
        printCellSummary(cell);
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
        printCellSummary(cell);
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
        printCellSummary(cell);
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
        printCellSummary(cell);
      }
    }
    System.out.println("---4---");

  }

  @Test
  public void testScanBatching() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final int batch = 1;
    scan.setBatch(batch);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testBatchingAndCachingTogether() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final int batch = 3;
    final int caching = 3;
    scan.setBatch(batch);
    scan.setCaching(caching);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testScanMaxResultSize() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    scan.setMaxResultSize(1);

    ResultScanner scanner = table.getScanner(scan);
    List<Result> results = new ArrayList<>();
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      results.add(result);
      System.out.println("iteration: " + iteration);
      System.out.println("isPartial?: " + result.isPartial());
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testMaxResultSizeAndCaching() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final long maxResultSize = 1;
    final int caching = 3;
    scan.setMaxResultSize(maxResultSize);
    scan.setCaching(caching);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testMiniCluster() {

  }

  @Test
  public void testAllConfigurationsCombined() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final long maxResultSize = 64000;
    final int caching = 2;
    final int batch = 3;
    scan.setMaxResultSize(maxResultSize);
    scan.setCaching(caching);
    scan.setBatch(batch);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  @Test
  public void testFilter() throws IOException {
    // TODO: since filters are a server side construct, there may be a problem with applying them
    // when partials are to be returned
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final int batch = 1;
    scan.setBatch(batch);

  }

  @Test
  public void testScratch() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("table_r10_c10"));

    Scan scan = new Scan();
    final int caching = 1;
    scan.setCaching(caching);

    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int iteration = 0;
    while ((result = scanner.next()) != null) {
      System.out.println("iteration: " + iteration);
      for (Cell cell : result.rawCells()) {
        printCellSummary(cell);
      }
      System.out.println();
      iteration++;
    }
  }

  private void printCellSummary(Cell cell) {
    final boolean verbose = false;
    HashMap<String, String> map = new LinkedHashMap<>();
    map.put("toString:", CellUtil.toString(cell, verbose));
    map.put("size:", "" + CellUtil.estimatedHeapSizeOf(cell));

    for (Entry<String, String> e : map.entrySet()) {
      System.out.println(e.getKey() + e.getValue());
    }
  }
}
