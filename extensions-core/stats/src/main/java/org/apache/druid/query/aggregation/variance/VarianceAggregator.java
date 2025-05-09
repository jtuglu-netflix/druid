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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

/**
 */
public abstract class VarianceAggregator implements Aggregator
{
  protected final VarianceAggregatorCollector holder = new VarianceAggregatorCollector();

  @Override
  public Object get()
  {
    return holder;
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("VarianceAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("VarianceAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("VarianceAggregator does not support getDouble()");
  }

  public static final class FloatVarianceAggregator extends VarianceAggregator
  {
    private final BaseFloatColumnValueSelector selector;

    public FloatVarianceAggregator(BaseFloatColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      if (!selector.isNull()) {
        holder.add(selector.getFloat());
      }
    }
  }

  public static final class DoubleVarianceAggregator extends VarianceAggregator
  {
    private final BaseDoubleColumnValueSelector selector;

    public DoubleVarianceAggregator(BaseDoubleColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      if (!selector.isNull()) {
        holder.add(selector.getDouble());
      }
    }
  }

  public static final class LongVarianceAggregator extends VarianceAggregator
  {
    private final BaseLongColumnValueSelector selector;

    public LongVarianceAggregator(BaseLongColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      if (!selector.isNull()) {
        holder.add(selector.getLong());
      }
    }
  }

  public static final class ObjectVarianceAggregator extends VarianceAggregator
  {
    private final BaseObjectColumnValueSelector<?> selector;

    public ObjectVarianceAggregator(BaseObjectColumnValueSelector<?> selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      VarianceAggregatorCollector.combineValues(holder, selector.getObject());
    }
  }
}
