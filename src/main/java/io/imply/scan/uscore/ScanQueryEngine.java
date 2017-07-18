/*
 * Portions of this file are copyright 2017 Imply Data, Inc and
 * are licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Portions of this file are copied from Druid. The following
 * header applies to these portions:
 *
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.imply.scan.uscore;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ScanQueryEngine
{
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final Map<String, Object> responseContext
  )
  {
    if (responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) != null) {
      long count = (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT);
      if (count >= query.getLimit()) {
        return Sequences.empty();
      }
    }
    final boolean hasTimeout = hasTimeout(query);
    final long timeoutAt = (long) responseContext.get(ScanQueryRunnerFactory.CTX_TIMEOUT_AT);
    final long start = System.currentTimeMillis();
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<String> allColumns = new ArrayList<>();

    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      allColumns.addAll(query.getColumns());
    } else {
      allColumns.add(Column.TIME_COLUMN_NAME);
      Iterables.addAll(allColumns, adapter.getAvailableDimensions());
      Iterables.addAll(allColumns, adapter.getAvailableMetrics());
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final String segmentId = segment.getIdentifier();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    if (responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) == null) {
      responseContext.put(ScanQueryRunnerFactory.CTX_COUNT, 0L);
    }
    final long limit = query.getLimit() - (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT);
    return Sequences.concat(
        Sequences.map(
            adapter.makeCursors(
                filter,
                intervals.get(0),
                query.getVirtualColumns(),
                Granularities.ALL,
                query.isDescending()
            ),
            new Function<Cursor, Sequence<ScanResultValue>>()
            {
              @Override
              public Sequence<ScanResultValue> apply(final Cursor cursor)
              {
                return new BaseSequence<>(
                    new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                    {
                      @Override
                      public Iterator<ScanResultValue> make()
                      {
                        final List<ObjectColumnSelector> columnSelectors = new ArrayList<>(allColumns.size());

                        for (String column : allColumns) {
                          columnSelectors.add(cursor.makeObjectColumnSelector(column));
                        }

                        final int batchSize = query.getBatchSize();
                        return new Iterator<ScanResultValue>()
                        {
                          private long offset = 0;

                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && offset < limit;
                          }

                          @Override
                          public ScanResultValue next()
                          {
                            if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                              throw new QueryInterruptedException(new TimeoutException());
                            }
                            final long lastOffset = offset;
                            final Object events;
                            final String resultFormat = query.getResultFormat();
                            if (ScanQuery.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
                              throw new UnsupportedOperationException("valueVector is not supported now");
                            } else if (ScanQuery.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                              events = rowsToCompactedList();
                            } else {
                              events = rowsToList();
                            }
                            responseContext.put(
                                ScanQueryRunnerFactory.CTX_COUNT,
                                (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) + (offset - lastOffset)
                            );
                            if (hasTimeout) {
                              responseContext.put(
                                  ScanQueryRunnerFactory.CTX_TIMEOUT_AT,
                                  timeoutAt - (System.currentTimeMillis() - start)
                              );
                            }
                            return new ScanResultValue(segmentId, allColumns, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private Object rowsToCompactedList()
                          {
                            final List<Object> events = new ArrayList<>(batchSize);
                            for (int i = 0; !cursor.isDone()
                                            && i < batchSize
                                            && offset < limit; cursor.advance(), i++, offset++) {
                              final List<Object> theEvent = new ArrayList<>(allColumns.size());
                              for (int j = 0; j < allColumns.size(); j++) {
                                final ObjectColumnSelector selector = columnSelectors.get(j);
                                theEvent.add(selector == null ? null : selector.get());
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private Object rowsToList()
                          {
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            for (int i = 0; !cursor.isDone()
                                            && i < batchSize
                                            && offset < limit; cursor.advance(), i++, offset++) {
                              final Map<String, Object> theEvent = new LinkedHashMap<>();
                              for (int j = 0; j < allColumns.size(); j++) {
                                final ObjectColumnSelector selector = columnSelectors.get(j);
                                theEvent.put(allColumns.get(j), selector == null ? null : selector.get());
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<ScanResultValue> iterFromMake)
                      {
                      }
                    }
                );
              }
            }
        )
    );
  }

  // From QueryContexts, not in 0.10.0
  static <T> boolean hasTimeout(Query<T> query)
  {
    return getTimeout(query) != 0;
  }

  // From QueryContexts, not in 0.10.0
  static <T> long getTimeout(Query<T> query)
  {
    return getTimeout(query, getDefaultTimeout(query));
  }

  // From QueryContexts, not in 0.10.0
  static <T> long getTimeout(Query<T> query, long defaultTimeout)
  {
    final long timeout = parseLong(query, "timeout", defaultTimeout);
    Preconditions.checkState(timeout >= 0, "Timeout must be a non negative value, but was [%s]", timeout);
    return timeout;
  }

  // From QueryContexts, not in 0.10.0
  static <T> long getDefaultTimeout(Query<T> query)
  {
    final long defaultTimeout = parseLong(query, "defaultTimeout", 300_000);
    Preconditions.checkState(defaultTimeout >= 0, "Timeout must be a non negative value, but was [%s]", defaultTimeout);
    return defaultTimeout;
  }

  // From QueryContexts, not in 0.10.0
  static <T> long parseLong(Query<T> query, String key, long defaultValue)
  {
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Long.parseLong((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).longValue();
    } else {
      throw new ISE("Unknown type [%s]", val.getClass());
    }
  }
}
