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

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public abstract class SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    implements MetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataStorageActionHandler.class);
  private static final String CONTEXT_KEY_IS_TRANSIENT = "isTransient";

  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final TypeReference<EntryType> entryType;
  private final TypeReference<StatusType> statusType;
  private final TypeReference<LockType> lockType;

  private final String entryTypeName;
  private final String entryTable;
  private final String lockTable;

  private final TaskInfoMapper<EntryType, StatusType> taskInfoMapper;
  private final TaskStatusMapper taskStatusMapper;
  private final TaskStatusMapperFromPayload taskStatusMapperFromPayload;
  private final TaskIdentifierMapper taskIdentifierMapper;

  private Future<Boolean> taskMigrationCompleteFuture;

  /**
   * @deprecated Use the other constructor without {@code logTable} argument
   * since this argument is now unused.
   */
  @Deprecated
  public SQLMetadataStorageActionHandler(
      final SQLMetadataConnector connector,
      final ObjectMapper jsonMapper,
      final MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      final String entryTypeName,
      final String entryTable,
      final String logTable,
      final String lockTable
  )
  {
    this(connector, jsonMapper, types, entryTypeName, entryTable, lockTable);
  }

  @SuppressWarnings("PMD.UnnecessaryFullyQualifiedName")
  public SQLMetadataStorageActionHandler(
      final SQLMetadataConnector connector,
      final ObjectMapper jsonMapper,
      final MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      final String entryTypeName,
      final String entryTable,
      final String lockTable
  )
  {
    this.connector = connector;
    //fully qualified references required below due to identical package names across project modules.
    //noinspection UnnecessaryFullyQualifiedName
    this.jsonMapper = jsonMapper.copy().addMixIn(PasswordProvider.class, PasswordProviderRedactionMixIn.class);
    this.entryType = types.getEntryType();
    this.statusType = types.getStatusType();
    this.lockType = types.getLockType();
    this.entryTypeName = entryTypeName;
    this.entryTable = entryTable;
    this.lockTable = lockTable;
    this.taskInfoMapper = new TaskInfoMapper<>(jsonMapper, entryType, statusType);
    this.taskStatusMapper = new TaskStatusMapper(jsonMapper);
    this.taskStatusMapperFromPayload = new TaskStatusMapperFromPayload(jsonMapper);
    this.taskIdentifierMapper = new TaskIdentifierMapper(jsonMapper);
  }

  protected SQLMetadataConnector getConnector()
  {
    return connector;
  }

  protected ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  protected TypeReference<StatusType> getStatusType()
  {
    return statusType;
  }

  protected String getEntryTable()
  {
    return entryTable;
  }

  protected String getLogTable()
  {
    throw new UnsupportedOperationException("'tasklogs' table is not used anymore");
  }

  protected String getEntryTypeName()
  {
    return entryTypeName;
  }

  public TypeReference<EntryType> getEntryType()
  {
    return entryType;
  }

  @Override
  public void insert(
      final String id,
      final DateTime timestamp,
      final String dataSource,
      final EntryType entry,
      final boolean active,
      final StatusType status,
      final String type,
      final String groupId
  )
  {
    try {
      getConnector().retryWithHandle(
          handle -> insertEntryWithHandle(handle, id, timestamp, dataSource, entry, active, status, type, groupId),
          this::isTransientDruidException
      );
    }
    catch (CallbackFailedException e) {
      propagateAsRuntimeException(e.getCause());
    }
    catch (Exception e) {
      propagateAsRuntimeException(e);
    }
  }

  private void propagateAsRuntimeException(Throwable t)
  {
    Throwables.propagateIfPossible(t);
    throw new RuntimeException(t);
  }

  /**
   * Inserts the given entry into the metadata store. This method wraps any
   * exception thrown in a {@link DruidException}. When used in a HandleCallback,
   * that exception is further wrapped in a {@link CallbackFailedException}.
   */
  private Void insertEntryWithHandle(
      Handle handle,
      String entryId,
      DateTime timestamp,
      String dataSource,
      EntryType entry,
      boolean active,
      StatusType status,
      String type,
      String groupId
  )
  {
    try {
      final String sql = StringUtils.format(
          "INSERT INTO %s (id, created_date, datasource, payload, type, group_id, active, status_payload) "
          + "VALUES (:id, :created_date, :datasource, :payload, :type, :group_id, :active, :status_payload)",
          getEntryTable()
      );
      handle.createStatement(sql)
            .bind("id", entryId)
            .bind("created_date", timestamp.toString())
            .bind("datasource", dataSource)
            .bind("payload", jsonMapper.writeValueAsBytes(entry))
            .bind("type", type)
            .bind("group_id", groupId)
            .bind("active", active)
            .bind("status_payload", jsonMapper.writeValueAsBytes(status))
            .execute();
      return null;
    }
    catch (Throwable t) {
      throw wrapInDruidException(entryId, t);
    }
  }

  public static boolean isStatementException(Throwable e)
  {
    return e instanceof StatementException ||
           (e instanceof CallbackFailedException && e.getCause() instanceof StatementException);
  }

  private boolean isTransientDruidException(Throwable t)
  {
    if (t instanceof CallbackFailedException) {
      return isTransientDruidException(t.getCause());
    } else if (t instanceof DruidException) {
      return Boolean.parseBoolean(((DruidException) t).getContextValue(CONTEXT_KEY_IS_TRANSIENT));
    } else {
      return getConnector().isTransientException(t);
    }
  }

  @Override
  public boolean setStatus(final String entryId, final boolean active, final StatusType status)
  {
    return connector.retryWithHandle(
        new HandleCallback<>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                StringUtils.format(
                    "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = TRUE",
                    entryTable
                )
            )
                         .bind("id", entryId)
                         .bind("active", active)
                         .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                         .execute() == 1;
          }
        }
    );
  }

  @Override
  public Optional<EntryType> getEntry(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<>()
        {
          @Override
          public Optional<EntryType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                StringUtils.format("SELECT payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.readValue(res, entryType)
            );
          }
        }
    );

  }

  @Override
  public Optional<StatusType> getStatus(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<>()
        {
          @Override
          public Optional<StatusType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                StringUtils.format("SELECT status_payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.readValue(res, statusType)
            );
          }
        }
    );
  }

  @Override
  @Nullable
  public TaskInfo<EntryType, StatusType> getTaskInfo(String entryId)
  {
    return connector.retryWithHandle(handle -> {
      final String query = StringUtils.format(
          "SELECT id, status_payload, payload, datasource, created_date FROM %s WHERE id = :id",
          entryTable
      );
      return handle.createQuery(query)
                   .bind("id", entryId)
                   .map(taskInfoMapper)
                   .first();
    });
  }

  @Override
  public List<TaskInfo<EntryType, StatusType>> getTaskInfos(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource
  )
  {
    return getConnector().retryTransaction(
        (handle, status) -> {
          final List<TaskInfo<EntryType, StatusType>> tasks = new ArrayList<>();
          for (Entry<TaskLookupType, TaskLookup> entry : taskLookups.entrySet()) {
            final Query<Map<String, Object>> query;
            switch (entry.getKey()) {
              case ACTIVE:
                query = createActiveTaskStreamingQuery(handle, dataSource);
                tasks.addAll(query.map(taskInfoMapper).list());
                break;
              case COMPLETE:
                CompleteTaskLookup completeTaskLookup = (CompleteTaskLookup) entry.getValue();
                query = createCompletedTaskStreamingQuery(
                    handle,
                    completeTaskLookup.getTasksCreatedPriorTo(),
                    completeTaskLookup.getMaxTaskStatuses(),
                    dataSource
                );
                tasks.addAll(query.map(taskInfoMapper).list());
                break;
              default:
                throw new IAE("Unknown TaskLookupType: [%s]", entry.getKey());
            }
          }
          return tasks;
        },
        SQLMetadataConnector.QUIET_RETRIES,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  @Override
  public List<TaskInfo<TaskIdentifier, StatusType>> getTaskStatusList(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource
  )
  {
    boolean fetchPayload = true;
    if (taskMigrationCompleteFuture != null && taskMigrationCompleteFuture.isDone()) {
      try {
        fetchPayload = !taskMigrationCompleteFuture.get();
      }
      catch (Exception e) {
        log.info(e, "Exception getting task migration future");
      }
    }
    return getTaskStatusList(taskLookups, dataSource, fetchPayload);
  }

  @VisibleForTesting
  List<TaskInfo<TaskIdentifier, StatusType>> getTaskStatusList(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource,
      boolean fetchPayload
  )
  {
    ResultSetMapper<TaskInfo<TaskIdentifier, StatusType>> resultSetMapper =
        fetchPayload ? taskStatusMapperFromPayload : taskStatusMapper;
    return getConnector().retryTransaction(
        (handle, status) -> {
          final List<TaskInfo<TaskIdentifier, StatusType>> taskMetadataInfos = new ArrayList<>();
          for (Entry<TaskLookupType, TaskLookup> entry : taskLookups.entrySet()) {
            final Query<Map<String, Object>> query;
            switch (entry.getKey()) {
              case ACTIVE:
                query = fetchPayload
                        ? createActiveTaskStreamingQuery(handle, dataSource)
                        : createActiveTaskSummaryStreamingQuery(handle, dataSource);
                taskMetadataInfos.addAll(query.map(resultSetMapper).list());
                break;
              case COMPLETE:
                CompleteTaskLookup completeTaskLookup = (CompleteTaskLookup) entry.getValue();
                DateTime priorTo = completeTaskLookup.getTasksCreatedPriorTo();
                Integer limit = completeTaskLookup.getMaxTaskStatuses();
                query = fetchPayload
                        ? createCompletedTaskStreamingQuery(handle, priorTo, limit, dataSource)
                        : createCompletedTaskSummaryStreamingQuery(handle, priorTo, limit, dataSource);
                taskMetadataInfos.addAll(query.map(resultSetMapper).list());
                break;
              default:
                throw new IAE("Unknown TaskLookupType: [%s]", entry.getKey());
            }
          }
          return taskMetadataInfos;
        },
        SQLMetadataConnector.QUIET_RETRIES,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  /**
   * Wraps the given error in a user-friendly DruidException.
   */
  private DruidException wrapInDruidException(String taskId, Throwable t)
  {
    if (isStatementException(t) && getEntry(taskId).isPresent()) {
      return InvalidInput.exception("Task [%s] already exists", taskId);
    } else if (connector.isRootCausePacketTooBigException(t)) {
      return InvalidInput.exception(
          "Payload for task [%s] exceeds the max allowed packet limit."
          + " If you encountered this error while running native batch ingestion,"
          + " set a 'splitHintSpec' to reduce the payload of each task."
          + " If not running native batch ingestion, report this error to your operator.",
          taskId
      );
    } else {
      return DruidException.forPersona(DruidException.Persona.OPERATOR)
                           .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                           .build(t, "Encountered metadata exception for task [%s]", taskId)
                           .withContext(CONTEXT_KEY_IS_TRANSIENT, connector.isTransientException(t));
    }
  }

  /**
   * Fetches the columns needed to build TaskStatusPlus for completed tasks
   * Please note that this requires completion of data migration to avoid empty values for task type and groupId
   * Recommended for GET /tasks API
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for TaskStatusPlus for completed tasks of interest
   */
  private Query<Map<String, Object>> createCompletedTaskSummaryStreamingQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses,
      @Nullable String dataSource
  )
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  created_date, "
        + "  datasource, "
        + "  group_id, "
        + "  type, "
        + "  status_payload "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForInactiveStatusesSinceQuery(dataSource)
        + "ORDER BY created_date DESC",
        getEntryTable()
    );

    if (maxNumStatuses != null) {
      sql = decorateSqlWithLimit(sql);
    }
    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("start", timestamp.toString())
                                             .setFetchSize(connector.getStreamingFetchSize());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  /**
   * Fetches the columns needed to build a Task object with payload for completed tasks
   * This requires the task payload which can be large. Please use only when necessary.
   * For example for ingestion tasks view before migration of the new columns
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for completed TaskInfos of interest
   */
  private Query<Map<String, Object>> createCompletedTaskStreamingQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses,
      @Nullable String dataSource
  )
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  created_date, "
        + "  datasource, "
        + "  payload "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForInactiveStatusesSinceQuery(dataSource)
        + "ORDER BY created_date DESC",
        getEntryTable()
    );

    if (maxNumStatuses != null) {
      sql = decorateSqlWithLimit(sql);
    }
    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("start", timestamp.toString())
                                             .setFetchSize(connector.getStreamingFetchSize());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  protected abstract String decorateSqlWithLimit(String sql);

  private String getWhereClauseForInactiveStatusesSinceQuery(@Nullable String datasource)
  {
    String sql = StringUtils.format("active = FALSE AND created_date >= :start ");
    if (datasource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  /**
   * Fetches the columns needed to build TaskStatusPlus for active tasks
   * Please note that this requires completion of data migration to avoid empty values for task type and groupId
   * Recommended for GET /tasks API
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for TaskStatusPlus for active tasks of interest
   */
  private Query<Map<String, Object>> createActiveTaskSummaryStreamingQuery(Handle handle, @Nullable String dataSource)
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  group_id, "
        + "  type, "
        + "  datasource, "
        + "  created_date "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForActiveStatusesQuery(dataSource)
        + "ORDER BY created_date",
        entryTable
    );

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .setFetchSize(connector.getStreamingFetchSize());
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  /**
   * Fetches the columns needed to build Task objects with payload for active tasks
   * This requires the task payload which can be large. Please use only when necessary.
   * For example for ingestion tasks view before migration of the new columns
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for active TaskInfos of interest
   */
  private Query<Map<String, Object>> createActiveTaskStreamingQuery(Handle handle, @Nullable String dataSource)
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  payload, "
        + "  datasource, "
        + "  created_date "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForActiveStatusesQuery(dataSource)
        + "ORDER BY created_date",
        entryTable
    );

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .setFetchSize(connector.getStreamingFetchSize());
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  private String getWhereClauseForActiveStatusesQuery(String dataSource)
  {
    String sql = StringUtils.format("active = TRUE ");
    if (dataSource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  private class TaskStatusMapperFromPayload implements ResultSetMapper<TaskInfo<TaskIdentifier, StatusType>>
  {
    private final ObjectMapper objectMapper;

    TaskStatusMapperFromPayload(ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
    }

    @Override
    public TaskInfo<TaskIdentifier, StatusType> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      return toTaskIdentifierInfo(objectMapper, resultSet, true);
    }
  }

  private class TaskStatusMapper implements ResultSetMapper<TaskInfo<TaskIdentifier, StatusType>>
  {
    private final ObjectMapper objectMapper;

    TaskStatusMapper(ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
    }

    @Override
    public TaskInfo<TaskIdentifier, StatusType> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      return toTaskIdentifierInfo(objectMapper, resultSet, false);
    }
  }

  private TaskInfo<TaskIdentifier, StatusType> toTaskIdentifierInfo(ObjectMapper objectMapper,
                                                                    ResultSet resultSet,
                                                                    boolean usePayload
  ) throws SQLException
  {
    String type;
    String groupId;
    if (usePayload) {
      try {
        ObjectNode payload = objectMapper.readValue(resultSet.getBytes("payload"), ObjectNode.class);
        type = payload.get("type").asText();
        groupId = payload.get("groupId").asText();
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task payload");
        throw new SQLException(e);
      }
    } else {
      type = resultSet.getString("type");
      groupId = resultSet.getString("group_id");
    }

    String id = resultSet.getString("id");
    DateTime createdTime = DateTimes.of(resultSet.getString("created_date"));
    StatusType status;
    try {
      status = objectMapper.readValue(resultSet.getBytes("status_payload"), statusType);
    }
    catch (IOException e) {
      log.error(e, "Encountered exception while deserializing task status_payload");
      throw new SQLException(e);
    }
    String datasource = resultSet.getString("datasource");
    TaskIdentifier taskIdentifier = new TaskIdentifier(id, groupId, type);

    return new TaskInfo<>(id, createdTime, status, datasource, taskIdentifier);
  }

  static class TaskIdentifierMapper implements ResultSetMapper<TaskIdentifier>
  {
    private final ObjectMapper objectMapper;

    TaskIdentifierMapper(ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
    }

    @Override
    public TaskIdentifier map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      try {
        ObjectNode payload = objectMapper.readValue(resultSet.getBytes("payload"), ObjectNode.class);
        // If field is absent (older task version), use blank string to avoid a loop of migration of such tasks.
        JsonNode type = payload.get("type");
        JsonNode groupId = payload.get("groupId");
        return new TaskIdentifier(
            resultSet.getString("id"),
            groupId == null ? "" : groupId.asText(),
            type == null ? "" : type.asText()
        );
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task payload");
        throw new SQLException(e);
      }
    }
  }

  static class TaskInfoMapper<EntryType, StatusType> implements ResultSetMapper<TaskInfo<EntryType, StatusType>>
  {
    private final ObjectMapper objectMapper;
    private final TypeReference<EntryType> entryType;
    private final TypeReference<StatusType> statusType;

    TaskInfoMapper(ObjectMapper objectMapper, TypeReference<EntryType> entryType, TypeReference<StatusType> statusType)
    {
      this.objectMapper = objectMapper;
      this.entryType = entryType;
      this.statusType = statusType;
    }

    @Override
    public TaskInfo<EntryType, StatusType> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      final TaskInfo<EntryType, StatusType> taskInfo;
      EntryType task;
      StatusType status;
      try {
        task = objectMapper.readValue(resultSet.getBytes("payload"), entryType);
      }
      catch (IOException e) {
        log.warn("Encountered exception[%s] while deserializing task payload, setting payload to null", e.getMessage());
        task = null;
      }
      try {
        status = objectMapper.readValue(resultSet.getBytes("status_payload"), statusType);
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task status_payload");
        throw new SQLException(e);
      }
      taskInfo = new TaskInfo<>(
          resultSet.getString("id"),
          DateTimes.of(resultSet.getString("created_date")),
          status,
          resultSet.getString("datasource"),
          task
      );
      return taskInfo;
    }
  }

  @Override
  public boolean addLock(final String entryId, final LockType lock)
  {
    return connector.retryWithHandle(
        new HandleCallback<>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return addLock(handle, entryId, lock);
          }
        }
    );
  }

  private boolean addLock(Handle handle, String entryId, LockType lock) throws JsonProcessingException
  {
    final String statement = StringUtils.format(
        "INSERT INTO %1$s (%2$s_id, lock_payload) VALUES (:entryId, :payload)",
        lockTable, entryTypeName
    );
    return handle.createStatement(statement)
                 .bind("entryId", entryId)
                 .bind("payload", jsonMapper.writeValueAsBytes(lock))
                 .execute() == 1;
  }

  @Override
  public boolean replaceLock(final String entryId, final long oldLockId, final LockType newLock)
  {
    return connector.retryTransaction(
        (handle, transactionStatus) -> {
          int numDeletedRows = removeLock(handle, oldLockId);

          if (numDeletedRows != 1) {
            transactionStatus.setRollbackOnly();
            final String message = numDeletedRows == 0 ?
                                   StringUtils.format("Cannot find lock[%d]", oldLockId) :
                                   StringUtils.format("Found multiple locks for lockId[%d]", oldLockId);
            throw new RuntimeException(message);
          }

          return addLock(handle, entryId, newLock);
        },
        SQLMetadataConnector.QUIET_RETRIES,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  @Override
  public void removeLock(final long lockId)
  {
    connector.retryWithHandle(
        handle -> removeLock(handle, lockId)
    );
  }

  @Override
  public void removeTasksOlderThan(final long timestamp)
  {
    DateTime dateTime = DateTimes.utc(timestamp);
    connector.retryWithHandle(
        handle ->
          handle.createStatement(
              StringUtils.format(
                  "DELETE FROM %s WHERE created_date < :date_time AND active = false",
                  entryTable
              )
          ).bind("date_time", dateTime.toString()).execute()
    );
  }

  private int removeLock(Handle handle, long lockId)
  {
    return handle.createStatement(StringUtils.format("DELETE FROM %s WHERE id = :id", lockTable))
                 .bind("id", lockId)
                 .execute();
  }

  @Override
  public Map<Long, LockType> getLocks(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<>()
        {
          @Override
          public Map<Long, LockType> withHandle(Handle handle)
          {
            return handle.createQuery(
                StringUtils.format(
                    "SELECT id, lock_payload FROM %1$s WHERE %2$s_id = :entryId",
                    lockTable, entryTypeName
                )
            )
                         .bind("entryId", entryId)
                         .map(
                             new ResultSetMapper<Pair<Long, LockType>>()
                             {
                               @Override
                               public Pair<Long, LockType> map(int index, ResultSet r, StatementContext ctx)
                                   throws SQLException
                               {
                                 try {
                                   return Pair.of(
                                       r.getLong("id"),
                                       jsonMapper.readValue(
                                           r.getBytes("lock_payload"),
                                           lockType
                                       )
                                   );
                                 }
                                 catch (IOException e) {
                                   log.makeAlert(e, "Failed to deserialize " + lockType.getType())
                                      .addData("id", r.getLong("id"))
                                      .addData(
                                          "lockPayload", StringUtils.fromUtf8(r.getBytes("lock_payload"))
                                      )
                                      .emit();
                                   throw new SQLException(e);
                                 }
                               }
                             }
                         )
                         .fold(
                             Maps.newLinkedHashMap(),
                             new Folder3<Map<Long, LockType>, Pair<Long, LockType>>()
                             {
                               @Override
                               public Map<Long, LockType> fold(
                                   Map<Long, LockType> accumulator,
                                   Pair<Long, LockType> lock,
                                   FoldController control,
                                   StatementContext ctx
                               )
                               {
                                 accumulator.put(lock.lhs, lock.rhs);
                                 return accumulator;
                               }
                             }
                         );
          }
        }
    );
  }

  @Override
  @Nullable
  public Long getLockId(String entryId, LockType lock)
  {
    return getLocks(entryId).entrySet().stream()
                            .filter(entry -> entry.getValue().equals(lock))
                            .map(Entry::getKey)
                            .findAny()
                            .orElse(null);
  }

  private List<TaskIdentifier> fetchTasksWithTypeColumnNullAndIdGreaterThan(String id, int limit)
  {
    List<TaskIdentifier> taskIdentifiers = new ArrayList<>();
    connector.retryWithHandle(
        handle -> {
          String sql = StringUtils.format(
              "SELECT * FROM %1$s WHERE id > '%2$s' AND type IS null ORDER BY id %3$s",
              entryTable, id, connector.limitClause(limit)
          );
          Query<Map<String, Object>> query = handle.createQuery(sql);
          taskIdentifiers.addAll(query.map(taskIdentifierMapper).list());
          return null;
        }
    );
    return taskIdentifiers;
  }

  private int updateColumnsTypeAndGroupIdForTasks(List<TaskIdentifier> taskIdentifiers)
  {
    return connector.retryWithHandle(
        handle -> {
          final Batch batch = handle.createBatch();
          for (TaskIdentifier task : taskIdentifiers) {
            batch.add(
                StringUtils.format(
                    "UPDATE %1$s SET type = '%2$s', group_id = '%3$s' WHERE id = '%4$s'",
                    entryTable, task.getType(), task.getGroupId(), task.getId()
                )
            );
          }
          int[] result = batch.execute();
          return IntStream.of(result).sum();
        }
    );
  }

  @Override
  public void populateTaskTypeAndGroupIdAsync()
  {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    taskMigrationCompleteFuture = executorService.submit(this::populateTaskTypeAndGroupId);
  }

  /**
   * Utility to migrate existing tasks to the new schema by populating type and groupId synchronously
   *
   * @return true if successful
   */
  @VisibleForTesting
  boolean populateTaskTypeAndGroupId()
  {
    log.debug("Populating columns [task] and [group_id] in task table[%s] from payload.", entryTable);
    String lastUpdatedTaskId = "";
    final int limit = 100;
    int numUpdatedTasks = 0;
    while (true) {
      List<TaskIdentifier> taskIdentifiers;
      try {
        taskIdentifiers = fetchTasksWithTypeColumnNullAndIdGreaterThan(lastUpdatedTaskId, limit);
      }
      catch (Exception e) {
        log.warn(e, "Task migration failed while reading entries from task table");
        return false;
      }
      if (taskIdentifiers.isEmpty()) {
        break;
      }
      try {
        final int updatedCount = updateColumnsTypeAndGroupIdForTasks(taskIdentifiers);
        if (updatedCount > 0) {
          numUpdatedTasks += updatedCount;
          log.info("Successfully updated columns [type] and [group_id] for [%d] tasks.", numUpdatedTasks);
        }
      }
      catch (Exception e) {
        log.warn(e, "Task migration failed while updating entries in task table");
        return false;
      }
      lastUpdatedTaskId = taskIdentifiers.get(taskIdentifiers.size() - 1).getId();

      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        log.info("Interrupted, exiting!");
        Thread.currentThread().interrupt();
      }
    }
    if (numUpdatedTasks > 0) {
      log.info("Task migration for table[%s] successful.", entryTable);
    }
    return true;
  }
}
