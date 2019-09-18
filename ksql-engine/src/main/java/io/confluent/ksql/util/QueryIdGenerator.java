/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.concurrent.atomic.AtomicLong;

public class QueryIdGenerator {

  private final AtomicLong queryIdCounter;
  private boolean useOffsetAsQueryId;
  private long offset;

  public QueryIdGenerator() {
    this(0L);
  }

  private QueryIdGenerator(final long initialValue) {
    this.queryIdCounter = new AtomicLong(initialValue);
    this.useOffsetAsQueryId = false;
    this.offset = 0L;
  }

  public void updateOffset(final long offset, final boolean useOffsetAsQueryId) {
    this.offset = offset;
    this.useOffsetAsQueryId = useOffsetAsQueryId;
  }

  public String getNextId() {
    return String.valueOf((useOffsetAsQueryId ? offset : queryIdCounter.getAndIncrement()));
  }

  public QueryIdGenerator copy() {
    return new QueryIdGenerator(queryIdCounter.get());
  }
}
