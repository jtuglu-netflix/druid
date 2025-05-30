/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(Parameterized.class)
public class CompressedVSizeColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  private final CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public CompressedVSizeColumnarIntsSerializerTest(
      CompressionStrategy compressionStrategy,
      ByteOrder byteOrder
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.byteOrder = byteOrder;
  }

  @Parameterized.Parameters(name = "{index}: compression={0}, byteOrder={1}")
  public static Iterable<Object[]> compressionStrategiesAndByteOrders()
  {
    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(CompressionStrategy.noNoneValues()),
        Sets.newHashSet(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    );

    return Iterables.transform(
        combinations,
        (Function<List, Object[]>) input -> new Object[]{input.get(0), input.get(1)}
    );
  }

  @Before
  public void setUp()
  {
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    segmentWriteOutMedium.close();
  }

  private void generateVals(final int totalSize, final int maxValue)
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkSize) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(temporaryFolder.newFolder());
    final String columnName = "test";
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        segmentWriteOutMedium,
        "test",
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        GenericIndexedWriter.MAX_FILE_SIZE,
        segmentWriteOutMedium.getCloser()
    );
    CompressedVSizeColumnarIntsSupplier supplierFromList = CompressedVSizeColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals),
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }
    long writtenLength = writer.getSerializedSize();
    final WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    writer.writeTo(writeOutBytes, smoosher);
    smoosher.close();

    Assert.assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder,
        null
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assert.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAndWrapExceptions(columnarInts);
  }

  @Test
  public void testSmallData() throws Exception
  {
    // less than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals(rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }

  @Test
  public void testLargeData() throws Exception
  {
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }


  // this test takes ~18 minutes to run
  @Ignore
  @Test
  public void testTooManyValues() throws IOException
  {
    final int maxValue = 0x0FFFFFFF;
    final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
    expectedException.expect(ColumnCapacityExceededException.class);
    expectedException.expectMessage(ColumnCapacityExceededException.formatMessage("test"));
    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(temporaryFolder.newFolder())
    ) {
      GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
          segmentWriteOutMedium,
          "test",
          compressionStrategy,
          Long.BYTES * 10000,
          GenericIndexedWriter.MAX_FILE_SIZE,
          segmentWriteOutMedium.getCloser()
      );
      CompressedVSizeColumnarIntsSerializer serializer = new CompressedVSizeColumnarIntsSerializer(
          "test",
          maxValue,
          maxChunkSize,
          byteOrder,
          compressionStrategy,
          genericIndexed,
          segmentWriteOutMedium.getCloser()
      );
      serializer.open();

      final long numRows = Integer.MAX_VALUE + 100L;
      for (long i = 0L; i < numRows; i++) {
        serializer.addValue(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
      }
    }
  }

  @Test
  public void testEmpty() throws Exception
  {
    vals = new int[0];
    checkSerializedSizeAndData(2);
  }

  private void checkV2SerializedSizeAndData(int chunkSize) throws Exception
  {
    File tmpDirectory = temporaryFolder.newFolder();
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    final String columnName = "test";
    GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        "test",
        compressionStrategy,
        Long.BYTES * 10000,
        GenericIndexedWriter.MAX_FILE_SIZE,
        segmentWriteOutMedium.getCloser()
    );
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        genericIndexed,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }

    final SmooshedWriter channel = smoosher.addWithSmooshedWriter(
        "test",
        writer.getSerializedSize()
    );
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder,
        null
    );

    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assert.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAll(columnarInts, mapper);
  }

  @Test
  public void testMultiValueFileLargeData() throws Exception
  {
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkV2SerializedSizeAndData(maxChunkSize);
    }
  }

  @Test
  public void testLargeColumn() throws IOException
  {
    final File columnDir = temporaryFolder.newFolder();
    final String columnName = "column";
    final int maxValue = Integer.MAX_VALUE;
    final long numRows = 500_000; // enough values that we expect to switch into large-column mode

    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(temporaryFolder.newFolder());
        FileSmoosher smoosher = new FileSmoosher(columnDir)
    ) {
      final Random random = new Random(0);
      final int fileSizeLimit = 128_000; // limit to 128KB so we switch to large-column mode sooner
      final CompressedVSizeColumnarIntsSerializer serializer = new CompressedVSizeColumnarIntsSerializer(
          columnName,
          segmentWriteOutMedium,
          columnName,
          maxValue,
          CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue),
          byteOrder,
          compressionStrategy,
          fileSizeLimit,
          segmentWriteOutMedium.getCloser()
      );
      serializer.open();

      for (int i = 0; i < numRows; i++) {
        serializer.addValue(random.nextInt() ^ Integer.MIN_VALUE);
      }

      try (SmooshedWriter primaryWriter = smoosher.addWithSmooshedWriter(columnName, serializer.getSerializedSize())) {
        serializer.writeTo(primaryWriter, smoosher);
      }
    }

    try (SmooshedFileMapper smooshMapper = SmooshedFileMapper.load(columnDir)) {
      MatcherAssert.assertThat(
          "Number of value parts written", // ensure the column actually ended up multi-part
          smooshMapper.getInternalFilenames().stream().filter(s -> s.startsWith("column_value_")).count(),
          Matchers.greaterThan(1L)
      );

      final Supplier<ColumnarInts> columnSupplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          smooshMapper.mapFile(columnName),
          byteOrder,
          smooshMapper
      );

      try (final ColumnarInts column = columnSupplier.get()) {
        Assert.assertEquals(numRows, column.size());
      }
    }
  }
}
