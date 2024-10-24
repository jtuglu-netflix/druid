package org.apache.druid.query.topn;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.utils.JvmUtils;

public class TopNAggregatorResourceHelper
{
  private static final Logger log = new Logger(TopNAggregatorResourceHelper.class);
  public static class TopNResourceConfig {
    public final float aggregatorMapHeapPct;

    public TopNResourceConfig(){
      this.aggregatorMapHeapPct = 0.8f;
    }

    public TopNResourceConfig(float aggregatorMapHeapPct) {
      this.aggregatorMapHeapPct = aggregatorMapHeapPct;
    }
  }

  private final TopNResourceConfig config;
  private long used = 0;

  TopNAggregatorResourceHelper() {
    this.config = new TopNResourceConfig();
  }

  TopNAggregatorResourceHelper(TopNResourceConfig config) {
    this.config = config;
  }

  public synchronized void verifyHeapLimits(final long expectedAllocBytes) {
    log.info("alloc=%d, used=%d", expectedAllocBytes, used);
    used += expectedAllocBytes;
    final double maxHeapSize = JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes();
    log.info("maxHeapSize=%f", maxHeapSize);
    final double usagePct = used / maxHeapSize;

    log.info("usagePct=%f", usagePct);
    if (usagePct > config.aggregatorMapHeapPct) {
      throw new ResourceLimitExceededException(StringUtils.format("Query ran out of memory. Maximum allowed pct=%f, Hit pct=%f", config.aggregatorMapHeapPct, usagePct));
    }
  }
}
