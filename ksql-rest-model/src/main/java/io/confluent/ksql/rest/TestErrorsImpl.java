/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import javax.ws.rs.core.Response;

public class TestErrorsImpl implements Errors {
  private static String ACLS_DOCS = 
      "https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html#create-acls-for-ksql-to-access-a-specific-topic-in-ccloud";
  
  @Override
  public Response accessDeniedFromKafkaResponse(final Throwable t) {
    return Response
        .status(FORBIDDEN)
        .entity(new KsqlErrorMessage(
            ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, 
            String.format("You have not configured ACLs properly, please check: %s", ACLS_DOCS)))
        .build();
  }

  @Override
  public String webSocketAuthorizationErrorMessage(final Throwable t) {
    return String.format("You have not configured ACLs properly (%s), please check: %s",
        t.getMessage(), ACLS_DOCS);
  }
}
