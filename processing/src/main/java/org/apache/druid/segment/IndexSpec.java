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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.loading.SegmentizerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 * <p>
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  public static final IndexSpec DEFAULT = IndexSpec.builder().build();

  public static Builder builder()
  {
    return new Builder();
  }

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final CompressionStrategy dimensionCompression;
  private final StringEncodingStrategy stringDictionaryEncoding;
  private final CompressionStrategy metricCompression;
  private final CompressionFactory.LongEncodingStrategy longEncoding;
  @Nullable
  private final CompressionStrategy complexMetricCompression;
  @Nullable
  private final CompressionStrategy jsonCompression;
  @Nullable
  private final SegmentizerFactory segmentLoader;

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   * @param bitmapSerdeFactory       type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                                 Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                                 setting, or, if none was set, uses the default defined in {@link BitmapSerde}
   * @param dimensionCompression     compression format for dimension columns, null to use the default.
   *                                 Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY}
   * @param stringDictionaryEncoding encoding strategy for string dictionaries of dictionary encoded string columns
   * @param metricCompression        compression format for primitive type metric columns, null to use the default.
   *                                 Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY}
   * @param longEncoding             encoding strategy for metric and dimension columns with type long, null to use the default.
   *                                 Defaults to {@link CompressionFactory#DEFAULT_LONG_ENCODING_STRATEGY}
   * @param segmentLoader            specify a {@link SegmentizerFactory} which will be written to 'factory.json' and used to load
   *                                 the written segment
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") @Nullable CompressionStrategy dimensionCompression,
      @JsonProperty("stringDictionaryEncoding") @Nullable StringEncodingStrategy stringDictionaryEncoding,
      @JsonProperty("metricCompression") @Nullable CompressionStrategy metricCompression,
      @JsonProperty("longEncoding") @Nullable CompressionFactory.LongEncodingStrategy longEncoding,
      @JsonProperty("complexMetricCompression") @Nullable CompressionStrategy complexMetricCompression,
      @JsonProperty("jsonCompression") @Nullable CompressionStrategy jsonCompression,
      @JsonProperty("segmentLoader") @Nullable SegmentizerFactory segmentLoader
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory != null
                              ? bitmapSerdeFactory
                              : new BitmapSerde.DefaultBitmapSerdeFactory();
    this.dimensionCompression = dimensionCompression == null
                                ? CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY
                                : dimensionCompression;
    this.stringDictionaryEncoding = stringDictionaryEncoding == null
                                    ? StringEncodingStrategy.DEFAULT
                                    : stringDictionaryEncoding;

    this.metricCompression = metricCompression == null
                             ? CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY
                             : metricCompression;
    this.complexMetricCompression = complexMetricCompression;
    this.longEncoding = longEncoding == null
                        ? CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY
                        : longEncoding;
    this.jsonCompression = jsonCompression;
    this.segmentLoader = segmentLoader;
  }

  @JsonProperty("bitmap")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public CompressionStrategy getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty
  public StringEncodingStrategy getStringDictionaryEncoding()
  {
    return stringDictionaryEncoding;
  }

  @JsonProperty
  public CompressionStrategy getMetricCompression()
  {
    return metricCompression;
  }

  @JsonProperty
  public CompressionFactory.LongEncodingStrategy getLongEncoding()
  {
    return longEncoding;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public CompressionStrategy getComplexMetricCompression()
  {
    return complexMetricCompression;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public SegmentizerFactory getSegmentLoader()
  {
    return segmentLoader;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public CompressionStrategy getJsonCompression()
  {
    return jsonCompression;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexSpec indexSpec = (IndexSpec) o;
    return Objects.equals(bitmapSerdeFactory, indexSpec.bitmapSerdeFactory) &&
           dimensionCompression == indexSpec.dimensionCompression &&
           Objects.equals(stringDictionaryEncoding, indexSpec.stringDictionaryEncoding) &&
           metricCompression == indexSpec.metricCompression &&
           longEncoding == indexSpec.longEncoding &&
           Objects.equals(complexMetricCompression, indexSpec.complexMetricCompression) &&
           Objects.equals(jsonCompression, indexSpec.jsonCompression) &&
           Objects.equals(segmentLoader, indexSpec.segmentLoader);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        bitmapSerdeFactory,
        dimensionCompression,
        stringDictionaryEncoding,
        metricCompression,
        longEncoding,
        complexMetricCompression,
        jsonCompression,
        segmentLoader
    );
  }

  @Override
  public String toString()
  {
    return "IndexSpec{" +
           "bitmapSerdeFactory=" + bitmapSerdeFactory +
           ", dimensionCompression=" + dimensionCompression +
           ", stringDictionaryEncoding=" + stringDictionaryEncoding +
           ", metricCompression=" + metricCompression +
           ", longEncoding=" + longEncoding +
           ", complexMetricCompression=" + complexMetricCompression +
           ", jsonCompression=" + jsonCompression +
           ", segmentLoader=" + segmentLoader +
           '}';
  }

  public static class Builder
  {
    @Nullable
    private BitmapSerdeFactory bitmapSerdeFactory;
    @Nullable
    private CompressionStrategy dimensionCompression;
    @Nullable
    private StringEncodingStrategy stringDictionaryEncoding;
    @Nullable
    private CompressionStrategy metricCompression;
    @Nullable
    private CompressionFactory.LongEncodingStrategy longEncoding;
    @Nullable
    private CompressionStrategy complexMetricCompression;
    @Nullable
    private CompressionStrategy jsonCompression;
    @Nullable
    private SegmentizerFactory segmentLoader;

    public Builder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public Builder withDimensionCompression(CompressionStrategy dimensionCompression)
    {
      this.dimensionCompression = dimensionCompression;
      return this;
    }

    public Builder withStringDictionaryEncoding(StringEncodingStrategy stringDictionaryEncoding)
    {
      this.stringDictionaryEncoding = stringDictionaryEncoding;
      return this;
    }

    public Builder withMetricCompression(CompressionStrategy metricCompression)
    {
      this.metricCompression = metricCompression;
      return this;
    }

    public Builder withComplexMetricCompression(CompressionStrategy complexMetricCompression)
    {
      this.complexMetricCompression = complexMetricCompression;
      return this;
    }

    public Builder withLongEncoding(CompressionFactory.LongEncodingStrategy longEncoding)
    {
      this.longEncoding = longEncoding;
      return this;
    }

    public Builder withJsonCompression(CompressionStrategy jsonCompression)
    {
      this.jsonCompression = jsonCompression;
      return this;
    }

    public Builder withSegmentLoader(SegmentizerFactory segmentLoader)
    {
      this.segmentLoader = segmentLoader;
      return this;
    }

    public IndexSpec build()
    {
      return new IndexSpec(
          bitmapSerdeFactory,
          dimensionCompression,
          stringDictionaryEncoding,
          metricCompression,
          longEncoding,
          complexMetricCompression,
          jsonCompression,
          segmentLoader
      );
    }
  }
}
