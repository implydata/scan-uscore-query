/*
 * Copyright 2017 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.imply.scan.uscore;

import com.google.common.collect.ImmutableList;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;

public class ScanUscoreQueryTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ScanQuery scanQuery = ScanQuery
        .newScanQueryBuilder()
        .dataSource("wikiticker")
        .intervals(ImmutableList.of(new Interval("2016-06-27/2017-06-28", ISOChronology.getInstanceUTC())))
        .virtualColumns(new ExpressionVirtualColumn("v", "x + y"))
        .filters(new SelectorDimFilter("user", "JasonAQuest", null))
        .columns("countryName", "page")
        .limit(100)
        .build();

    final DefaultObjectMapper jsonMapper = new DefaultObjectMapper();
    new ScanQueryDruidModule().getJacksonModules().forEach(jsonMapper::registerModule);
    final String serialized = jsonMapper.writeValueAsString(scanQuery);
    final Query scanQuery2 = jsonMapper.readValue(serialized, Query.class);

    Assert.assertEquals(scanQuery2, scanQuery);
    Assert.assertEquals(
        "{\"queryType\":\"scan_\",\"dataSource\":{\"type\":\"table\",\"name\":\"wikiticker\"},\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2016-06-27T00:00:00.000Z/2017-06-28T00:00:00.000Z\"]},\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v\",\"expression\":\"x + y\"}],\"resultFormat\":\"list\",\"batchSize\":20480,\"limit\":100,\"filter\":{\"type\":\"selector\",\"dimension\":\"user\",\"value\":\"JasonAQuest\",\"extractionFn\":null},\"columns\":[\"countryName\",\"page\"],\"context\":null,\"descending\":false}",
        serialized
    );
  }
}
