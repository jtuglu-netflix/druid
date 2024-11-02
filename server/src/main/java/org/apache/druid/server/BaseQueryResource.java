package org.apache.druid.server;

import org.apache.druid.server.metrics.QueryCountStatsProvider;

import java.util.concurrent.atomic.AtomicLong;

public class BaseQueryResource implements QueryCountStatsProvider
{
  protected final AtomicLong successfulQueryCount = new AtomicLong();
  protected final AtomicLong failedQueryCount = new AtomicLong();
  protected final AtomicLong interruptedQueryCount = new AtomicLong();
  protected final AtomicLong timedOutQueryCount = new AtomicLong();
  protected final ResourceQueryMetricCounter counter = new ResourceQueryMetricCounter();

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  @Override
  public long getTimedOutQueryCount()
  {
    return timedOutQueryCount.get();
  }

  protected class ResourceQueryMetricCounter implements QueryMetricCounter {
    @Override
    public void incrementSuccess()
    {
      successfulQueryCount.incrementAndGet();
    }

    @Override
    public void incrementFailed()
    {
      failedQueryCount.incrementAndGet();
    }

    @Override
    public void incrementInterrupted()
    {
      interruptedQueryCount.incrementAndGet();
    }

    @Override
    public void incrementTimedOut()
    {
      timedOutQueryCount.incrementAndGet();
    }
  }
}
