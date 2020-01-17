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

package io.confluent.ksql.schema.registry;

import static org.junit.Assert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslEngineBuilder;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSchemaRegistryClientFactoryTest {

  private static final SSLContext SSL_CONTEXT = getTestSslContext();

  @Mock
  private Supplier<RestService> restServiceSupplier;

  @Mock
  private RestService restService;

  @Mock
  private SslFactory sslFactory;
  @Mock
  private SslEngineBuilder sslEngineBuilder;

  @Mock
  private KsqlSchemaRegistryClientFactory.SchemaRegistryClientFactory srClientFactory;

  @Before
  public void setUp() {
    when(srClientFactory.create(any(), anyInt(), any(), any()))
        .thenReturn(mock(CachedSchemaRegistryClient.class));

    when(restServiceSupplier.get()).thenReturn(restService);

    when(sslFactory.sslEngineBuilder()).thenReturn(sslEngineBuilder);
    when(sslEngineBuilder.sslContext()).thenReturn(SSL_CONTEXT);
  }

  @Test
  public void shouldSetSocketFactoryWhenNoSpecificSslConfig() {
    // Given:
    final KsqlConfig config = config();

    final Map<String, Object> expectedConfigs = defaultConfigs();

    // When:
    KsqlSchemaRegistryClientFactory.configureSslFactory(config, sslFactory);

    // Then:
    verify(sslFactory).configure(expectedConfigs);
  }

  @Test
  public void shouldPickUpNonPrefixedSslConfig() {
    // Given:
    final KsqlConfig config = config(
        SslConfigs.SSL_PROTOCOL_CONFIG, "SSLv3"
    );

    final Map<String, Object> expectedConfigs = defaultConfigs();
    expectedConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "SSLv3");

    // When:
    KsqlSchemaRegistryClientFactory.configureSslFactory(config, sslFactory);

    // Then:
    verify(sslFactory).configure(expectedConfigs);
  }

  @Test
  public void shouldPickUpPrefixedSslConfig() {
    // Given:
    final KsqlConfig config = config(
        "ksql.schema.registry." + SslConfigs.SSL_PROTOCOL_CONFIG, "SSLv3"
    );

    final Map<String, Object> expectedConfigs = defaultConfigs();
    expectedConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "SSLv3");

    // When:
    KsqlSchemaRegistryClientFactory.configureSslFactory(config, sslFactory);


    // Then:
    verify(sslFactory).configure(expectedConfigs);
  }

  @Test
  public void shouldUseDefaultSchemaRegistryClientWhenUrlNotSpecified() {
    // Given
    final KsqlConfig config1 = config();

    final Map<String, Object> schemaRegistryClientConfigs = ImmutableMap.of(
        "ksql.schema.registry.url", "     "
    );
    final KsqlConfig config2 = new KsqlConfig(schemaRegistryClientConfigs);

    // When:
    SchemaRegistryClient client1 = new KsqlSchemaRegistryClientFactory(
        config1, restServiceSupplier, sslFactory, srClientFactory, Collections.emptyMap()).get();
    SchemaRegistryClient client2 = new KsqlSchemaRegistryClientFactory(
        config2, restServiceSupplier, sslFactory, srClientFactory, Collections.emptyMap()).get();

    // Then:
    assertThat(client1, instanceOf(DefaultSchemaRegistryClient.class));
    assertThat(client2, instanceOf(DefaultSchemaRegistryClient.class));
  }

  @Test
  public void shouldPassBasicAuthCredentialsToSchemaRegistryClient() {
    // Given
    final Map<String, Object> schemaRegistryClientConfigs = ImmutableMap.of(
        "ksql.schema.registry.basic.auth.credentials.source", "USER_INFO",
        "ksql.schema.registry.basic.auth.user.info", "username:password",
        "ksql.schema.registry.url", "some url"
    );

    final KsqlConfig config = new KsqlConfig(schemaRegistryClientConfigs);

    final Map<String, Object> expectedConfigs = defaultConfigs();
    expectedConfigs.put("basic.auth.credentials.source", "USER_INFO");
    expectedConfigs.put("basic.auth.user.info", "username:password");

    // When:
    new KsqlSchemaRegistryClientFactory(
        config, restServiceSupplier, sslFactory, srClientFactory, Collections.emptyMap()).get();

    // Then:
    verify(restService).setSslSocketFactory(isA(SSL_CONTEXT.getSocketFactory().getClass()));
    srClientFactory.create(same(restService), anyInt(), eq(expectedConfigs), any());
  }

  private static Map<String, Object> defaultConfigs() {
    return config().valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);
  }

  private static KsqlConfig config() {
    return new KsqlConfig(ImmutableMap.of());
  }

  private static KsqlConfig config(final String k1, final Object v1) {
    return new KsqlConfig(ImmutableMap.of(k1, v1));
  }

  // Can't mock SSLContext.
  private static SSLContext getTestSslContext() {
    final SslFactory sslFactory = new SslFactory(Mode.CLIENT);

    final Map<String, Object> configs = new KsqlConfig(Collections.emptyMap())
        .valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    sslFactory.configure(configs);
    return sslFactory.sslEngineBuilder().sslContext();
  }
}