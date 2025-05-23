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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.ServerViewModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
public abstract class ServerRunnable extends GuiceRunnable
{
  private static final EmittingLogger log = new EmittingLogger(ServerRunnable.class);

  public ServerRunnable(Logger log)
  {
    super(log);
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector(getNodeRoles(getProperties()));
    final Lifecycle lifecycle = initLifecycle(injector);

    try {
      lifecycle.join();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract Set<NodeRole> getNodeRoles(Properties properties);

  public static void bindAnnouncer(
      final Binder binder,
      final DiscoverySideEffectsProvider provider
  )
  {
    binder.bind(DiscoverySideEffectsProvider.Child.class)
          .toProvider(provider)
          .in(LazySingleton.class);

    LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class));
  }

  public static void bindAnnouncer(
      final Binder binder,
      final Class<? extends Annotation> annotation,
      final DiscoverySideEffectsProvider provider
  )
  {
    binder.bind(DiscoverySideEffectsProvider.Child.class)
          .annotatedWith(annotation)
          .toProvider(provider)
          .in(LazySingleton.class);

    LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class, annotation));
  }

  /**
   * This is a helper class used by CliXXX classes to announce {@link DiscoveryDruidNode}
   * as part of {@link Lifecycle.Stage#ANNOUNCEMENTS}.
   */
  public static class DiscoverySideEffectsProvider implements Provider<DiscoverySideEffectsProvider.Child>
  {
    public static class Child
    {
    }

    @Inject
    @Self
    private DruidNode druidNode;

    @Inject
    private DruidNodeAnnouncer announcer;

    @Inject
    private ServiceAnnouncer legacyAnnouncer;

    @Inject
    private Lifecycle lifecycle;

    @Inject
    private Injector injector;

    @Inject
    @Self
    private Set<NodeRole> nodeRoles; // this set can be different from the keySet of serviceClasses

    @Inject
    private Map<NodeRole, Set<Class<? extends DruidService>>> serviceClasses;

    private final boolean useLegacyAnnouncer;

    public static DiscoverySideEffectsProvider create()
    {
      return new DiscoverySideEffectsProvider(false);
    }

    public static DiscoverySideEffectsProvider withLegacyAnnouncer()
    {
      return new DiscoverySideEffectsProvider(true);
    }

    private DiscoverySideEffectsProvider(final boolean useLegacyAnnouncer)
    {
      this.useLegacyAnnouncer = useLegacyAnnouncer;
    }

    @Override
    public Child get()
    {
      for (NodeRole nodeRole : nodeRoles) {
        ImmutableMap.Builder<String, DruidService> builder = new ImmutableMap.Builder<>();
        for (Class<? extends DruidService> clazz : serviceClasses.getOrDefault(nodeRole, Collections.emptySet())) {
          DruidService service = injector.getInstance(clazz);
          if (service.isDiscoverable()) {
            builder.put(service.getName(), service);
          } else {
            log.info(
                "Service[%s] is not discoverable. This will not be listed as a service provided by this node.",
                service.getName()
            );
          }
        }
        DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(druidNode, nodeRole, builder.build());

        lifecycle.addHandler(
            new Lifecycle.Handler()
            {
              @Override
              public void start()
              {
                announcer.announce(discoveryDruidNode);

                if (useLegacyAnnouncer) {
                  legacyAnnouncer.announce(discoveryDruidNode.getDruidNode());
                }
              }

              @Override
              public void stop()
              {
                // Reverse order vs. start().

                if (useLegacyAnnouncer) {
                  legacyAnnouncer.unannounce(discoveryDruidNode.getDruidNode());
                }

                announcer.unannounce(discoveryDruidNode);
              }
            },
            Lifecycle.Stage.ANNOUNCEMENTS
        );
      }
      return new Child();
    }
  }

  protected static void validateCentralizedDatasourceSchemaConfig(Properties properties)
  {
    if (MetadataConfigModule.isSegmentSchemaCacheEnabled(properties)) {
      String serverViewType = properties.getProperty(ServerViewModule.SERVERVIEW_TYPE_PROPERTY);
      if (serverViewType != null && !serverViewType.equals(ServerViewModule.SERVERVIEW_TYPE_HTTP)) {
        throw DruidException
            .forPersona(DruidException.Persona.ADMIN)
            .ofCategory(DruidException.Category.UNSUPPORTED)
            .build(
                StringUtils.format(
                    "CentralizedDatasourceSchema feature is incompatible with config %1$s=%2$s. "
                    + "Please consider switching to HTTP-based segment discovery (set %1$s=%3$s) "
                    + "or disable the feature (set %4$s=false).",
                    ServerViewModule.SERVERVIEW_TYPE_PROPERTY,
                    serverViewType,
                    ServerViewModule.SERVERVIEW_TYPE_HTTP,
                    MetadataConfigModule.CENTRALIZED_DATASOURCE_SCHEMA_ENABLED
                )
            );
      }
    }
  }
}
