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

package org.apache.druid.indexing.overlord.setup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class FillCapacityWithAffinityWorkerSelectStrategyTest
{
  @Test
  public void testFindWorkerForTask()
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false),
        null
    );

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "lhost", "lhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        NoopTask.forDatasource("foo")
    );
    Assert.assertEquals("localhost", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNulls()
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false),
        null
    );

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "lhost", "lhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        NoopTask.create()
    );
    Assert.assertEquals("lhost", worker.getWorker().getHost());
  }

  @Test
  public void testIsolation()
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false),
        null
    );

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        NoopTask.create()
    );
    Assert.assertNull(worker);
  }

  @Test
  public void testFindWorkerForTaskWithGlobalLimits()
  {
    Map<String, Integer> taskLimits = new HashMap<>();
    taskLimits.put("noop", 2);

    Map<String, Integer> capacityUsed = new HashMap<>();
    capacityUsed.put("noop", 1);
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWorkerSelectStrategy(
        null,
        new TaskLimits(taskLimits, null)
    );

    NoopTask noopTask = NoopTask.forDatasource("foo");
    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost0",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost1",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        noopTask
    );
    Assert.assertNotNull(worker);

    ImmutableWorkerInfo worker1 = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost0",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost1",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        noopTask
    );

    Assert.assertNull(worker1);

  }

  @Test
  public void testFindWorkerForTaskWithGlobalRatios()
  {
    Map<String, Double> taskRatios = new HashMap<>();
    taskRatios.put("noop", 0.5);

    Map<String, Integer> capacityUsed = new HashMap<>();
    capacityUsed.put("noop", 1);
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWorkerSelectStrategy(
        null,
        new TaskLimits(null, taskRatios)
    );

    NoopTask noopTask = NoopTask.forDatasource("foo");
    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost0",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost1",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        noopTask
    );
    Assert.assertNotNull(worker);

    ImmutableWorkerInfo worker1 = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost0",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost1",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                0,
                capacityUsed,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        noopTask
    );

    Assert.assertNull(worker1);

  }
}
