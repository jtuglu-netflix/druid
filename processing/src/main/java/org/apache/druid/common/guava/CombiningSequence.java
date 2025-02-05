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

package org.apache.druid.common.guava;

import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.BinaryOperator;

public class CombiningSequence<T> implements Sequence<T>
{
  public static <T> CombiningSequence<T> create(
      Sequence<T> baseSequence,
      Comparator<T> ordering,
      BinaryOperator<T> mergeFn
  )
  {
    return new CombiningSequence<>(baseSequence, ordering, mergeFn);
  }

  private final Sequence<T> baseSequence;
  private final Comparator<T> ordering;
  private final BinaryOperator<T> mergeFn;

  private CombiningSequence(
      Sequence<T> baseSequence,
      Comparator<T> ordering,
      BinaryOperator<T> mergeFn
  )
  {
    this.baseSequence = baseSequence;
    this.ordering = ordering;
    this.mergeFn = mergeFn;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> accumulator)
  {
    final CombiningAccumulator<OutType> combiningAccumulator = new CombiningAccumulator<>(initValue, accumulator);
    T lastValue = baseSequence.accumulate(null, combiningAccumulator);
    if (combiningAccumulator.accumulatedSomething()) {
      return accumulator.accumulate(combiningAccumulator.retVal, lastValue);
    } else {
      return initValue;
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, final YieldingAccumulator<OutType, T> accumulator)
  {
    final CombiningYieldingAccumulator<OutType, T> combiningAccumulator =
        new CombiningYieldingAccumulator<>(ordering, mergeFn, accumulator);

    combiningAccumulator.setRetVal(initValue);

    final Yielder<T> baseYielder = baseSequence.toYielder(null, combiningAccumulator);

    try {
      // If the yielder is already done at this point, that means that it ran through all of the inputs
      // without hitting a yield(), i.e. it's effectively just a single accumulate() call.  As such we just
      // return a done yielder with the correct accumulated value.
      if (baseYielder.isDone()) {
        if (combiningAccumulator.accumulatedSomething()) {
          combiningAccumulator.accumulateLastValue();
        }
        // If we yielded, then the expectation is that we get a Yielder with the yielded value, followed by a done
        // yielder.  This will happen if we fall through to the normal makeYielder.  If the accumulator did not yield
        // then the code expects a single Yielder that returns whatever was left over from the accumulation on the
        // get() call.
        if (!combiningAccumulator.yielded()) {
          return Yielders.done(combiningAccumulator.getRetVal(), baseYielder);
        }
      }

      return makeYielder(baseYielder, combiningAccumulator);
    }
    catch (Throwable t1) {
      try {
        baseYielder.close();
      }
      catch (Throwable t2) {
        t1.addSuppressed(t2);
      }

      throw t1;
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      final Yielder<T> yielder,
      final CombiningYieldingAccumulator<OutType, T> combiningAccumulator
  )
  {
    return new Yielder<>()
    {
      private Yielder<T> myYielder = yielder;
      private CombiningYieldingAccumulator<OutType, T> accum = combiningAccumulator;

      @Override
      public OutType get()
      {
        return accum.getRetVal();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accum.reset();
        if (myYielder.isDone()) {
          return Yielders.done(null, myYielder);
        }

        myYielder = myYielder.next(myYielder.get());
        if (myYielder.isDone() && accum.accumulatedSomething()) {
          accum.accumulateLastValue();
          if (!accum.yielded()) {
            return Yielders.done(accum.getRetVal(), myYielder);
          }
        }

        return this;
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        myYielder.close();
      }
    };
  }

  private static class CombiningYieldingAccumulator<OutType, T> extends YieldingAccumulator<T, T>
  {
    private final Comparator<T> ordering;
    private final BinaryOperator<T> mergeFn;
    private final YieldingAccumulator<OutType, T> accumulator;

    private OutType retVal;
    private T lastMergedVal;
    private boolean accumulatedSomething = false;

    CombiningYieldingAccumulator(
        Comparator<T> ordering,
        BinaryOperator<T> mergeFn,
        YieldingAccumulator<OutType, T> accumulator
    )
    {
      this.ordering = ordering;
      this.mergeFn = mergeFn;
      this.accumulator = accumulator;
    }

    public OutType getRetVal()
    {
      return retVal;
    }

    public void setRetVal(OutType retVal)
    {
      this.retVal = retVal;
    }

    @Override
    public void reset()
    {
      accumulator.reset();
    }

    @Override
    public boolean yielded()
    {
      return accumulator.yielded();
    }

    @Override
    public void yield()
    {
      accumulator.yield();
    }

    @Override
    public T accumulate(T prevValue, T t)
    {
      if (!accumulatedSomething) {
        accumulatedSomething = true;
      }

      if (prevValue == null) {
        lastMergedVal = mergeFn.apply(t, null);
        return lastMergedVal;
      }

      if (ordering.compare(prevValue, t) == 0) {
        lastMergedVal = mergeFn.apply(prevValue, t);
        return lastMergedVal;
      }

      lastMergedVal = t;
      retVal = accumulator.accumulate(retVal, prevValue);
      return t;
    }

    void accumulateLastValue()
    {
      retVal = accumulator.accumulate(retVal, lastMergedVal);
    }

    boolean accumulatedSomething()
    {
      return accumulatedSomething;
    }
  }

  private class CombiningAccumulator<OutType> implements Accumulator<T, T>
  {
    private OutType retVal;
    private final Accumulator<OutType, T> accumulator;

    private volatile boolean accumulatedSomething = false;

    CombiningAccumulator(OutType retVal, Accumulator<OutType, T> accumulator)
    {
      this.retVal = retVal;
      this.accumulator = accumulator;
    }

    boolean accumulatedSomething()
    {
      return accumulatedSomething;
    }

    @Override
    public T accumulate(T prevValue, T t)
    {
      if (!accumulatedSomething) {
        accumulatedSomething = true;
      }

      if (prevValue == null) {
        return mergeFn.apply(t, null);
      }

      if (ordering.compare(prevValue, t) == 0) {
        return mergeFn.apply(prevValue, t);
      }

      retVal = accumulator.accumulate(retVal, prevValue);
      return t;
    }
  }
}
