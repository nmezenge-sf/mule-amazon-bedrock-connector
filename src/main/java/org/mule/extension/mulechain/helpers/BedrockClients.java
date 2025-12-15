package org.mule.extension.mulechain.helpers;

import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.net.ssl.TrustManagerFactory;
import org.mule.extension.mulechain.internal.*;
import org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsParameters;
import org.mule.extension.mulechain.internal.embeddings.AwsbedrockParametersEmbedding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.TlsTrustManagersProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrock.BedrockClient;
import software.amazon.awssdk.services.bedrock.BedrockClientBuilder;
import software.amazon.awssdk.services.bedrockagent.BedrockAgentClient;
import software.amazon.awssdk.services.bedrockagent.BedrockAgentClientBuilder;
import software.amazon.awssdk.services.bedrockagentruntime.BedrockAgentRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockagentruntime.BedrockAgentRuntimeAsyncClientBuilder;
import software.amazon.awssdk.services.bedrockagentruntime.BedrockAgentRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.iam.IamClient;

/**
 * Promotes reuse of client instances as per AWS SDK best practices:
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/best-practices.html
 *
 * while providing centralized client management configuration (eg. proxy, trust store, timeouts)
 */
public class BedrockClients {

  private static final Logger logger = LoggerFactory.getLogger(BedrockClients.class);
  private static final ConcurrentHashMap<String, Object> clients = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  private static <T> T getOrCreateClient(String clientType, Supplier<T> clientSupplier) {
    return (T) clients.computeIfAbsent(clientType, k -> clientSupplier.get());
  }

  public static BedrockAgentClient getAgentClient(AwsbedrockConfiguration configuration,
                                                  AwsbedrockAgentsParameters awsbedrockAgentsParameters) {
    return getOrCreateClient("agent", () -> {
      BedrockAgentClientBuilder bedrockAgentClientBuilder = BedrockAgentClient.builder()
          .region(Region.of(awsbedrockAgentsParameters.getRegion()))
          .fipsEnabled(configuration.getFipsModeEnabled())
          .credentialsProvider(StaticCredentialsProvider.create(createAwsBasicCredentials(configuration)));

      if (configuration.getEndpointOverride() != null && !configuration.getEndpointOverride().isBlank()) {
        bedrockAgentClientBuilder.endpointOverride(URI.create(configuration.getEndpointOverride()));
      }
      bedrockAgentClientBuilder.httpClient(getConfiguredHttpClient(configuration));
      return bedrockAgentClientBuilder.build();
    });
  }

  public static BedrockAgentRuntimeAsyncClient getAgentRuntimeAsyncClient(AwsbedrockConfiguration configuration,
                                                                          AwsbedrockAgentsParameters awsbedrockAgentsParameters) {
    return getOrCreateClient("agentRuntimeAsync", () -> {
      BedrockAgentRuntimeAsyncClientBuilder bedrockAgentRuntimeAsyncClientBuilder = BedrockAgentRuntimeAsyncClient.builder()
          .region(Region.of(awsbedrockAgentsParameters.getRegion()))
          .fipsEnabled(configuration.getFipsModeEnabled())
          .credentialsProvider(StaticCredentialsProvider.create(createAwsBasicCredentials(configuration)));

      if (configuration.getEndpointOverride() != null && !configuration.getEndpointOverride().isBlank()) {
        bedrockAgentRuntimeAsyncClientBuilder.endpointOverride(URI.create(configuration.getEndpointOverride()));
      }

      // Configure API-level timeouts for streaming operations
      Integer timeout = configuration.getTimeout();
      TimeUnitEnum timeoutUnit = configuration.getTimeoutUnit();
      if (timeout != null && timeoutUnit != null) {
        Duration apiTimeoutDuration = CommonUtils.toDuration(timeout, timeoutUnit);
        logger.debug("Configuring API-level timeouts: {} {} (apiCallTimeout, apiCallAttemptTimeout)", timeout, timeoutUnit);
        bedrockAgentRuntimeAsyncClientBuilder.overrideConfiguration(
                                                                    ClientOverrideConfiguration.builder()
                                                                        .apiCallTimeout(apiTimeoutDuration)
                                                                        .apiCallAttemptTimeout(apiTimeoutDuration)
                                                                        .build());
      } else {
        logger.warn("Timeout configuration is null - using SDK defaults. This may cause premature timeouts!");
      }

      bedrockAgentRuntimeAsyncClientBuilder.httpClient(getConfiguredAsyncHttpClient(configuration));
      return bedrockAgentRuntimeAsyncClientBuilder.build();
    });
  }

  public static BedrockRuntimeClient getRuntimeClient(AwsbedrockConfiguration configuration,
                                                      String region) {
    return getOrCreateClient("runtime", () -> {

      AwsCredentials awsCredentials = createAwsBasicCredentials(configuration);

      BedrockRuntimeClientBuilder bedrockRuntimeClientBuilder = BedrockRuntimeClient.builder()
          .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
          .fipsEnabled(configuration.getFipsModeEnabled())
          .region(Region.of(region));

      if (configuration.getEndpointOverride() != null && !configuration.getEndpointOverride().isBlank()) {
        bedrockRuntimeClientBuilder.endpointOverride(URI.create(configuration.getEndpointOverride()));
      }

      bedrockRuntimeClientBuilder.httpClient(getConfiguredHttpClient(configuration));

      return bedrockRuntimeClientBuilder.build();
    });
  }

  public static BedrockRuntimeClient getRuntimeClient(AwsbedrockConfiguration configuration,
                                                      AwsbedrockParametersEmbedding awsbedrockParametersEmbedding) {
    return getRuntimeClient(configuration, awsbedrockParametersEmbedding.getRegion());
  }

  public static BedrockRuntimeClient getRuntimeClient(AwsbedrockConfiguration configuration,
                                                      AwsbedrockParameters awsBedrockParameters) {
    return getRuntimeClient(configuration, awsBedrockParameters.getRegion());
  }

  public static BedrockClient getBedrockClient(AwsbedrockConfiguration configuration,
                                               String region) {
    return getOrCreateClient("bedrock", () -> {
      AwsCredentials awsCredentials = createAwsBasicCredentials(configuration);
      BedrockClientBuilder bedrockClient = BedrockClient.builder()
          .region(Region.of(region))
          .fipsEnabled(configuration.getFipsModeEnabled())
          .credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
      if (configuration.getEndpointOverride() != null && !configuration.getEndpointOverride().isBlank()) {
        bedrockClient.endpointOverride(URI.create(configuration.getEndpointOverride()));
      }

      bedrockClient.httpClient(getConfiguredHttpClient(configuration));
      return bedrockClient.build();
    });
  }

  public static BedrockClient getBedrockClient(AwsbedrockConfiguration configuration,
                                               AwsbedrockParams awsBedrockParameters) {
    return getBedrockClient(configuration, awsBedrockParameters.getRegion());
  }

  public static BedrockClient getBedrockClient(AwsbedrockConfiguration configuration,
                                               AwsbedrockParamsModelDetails awsbedrockParamsModelDetails) {
    return getBedrockClient(configuration, awsbedrockParamsModelDetails.getRegion());
  }

  public static IamClient getIamClient(AwsbedrockConfiguration configuration,
                                       AwsbedrockAgentsParameters awsbedrockAgentsParameters) {
    return getOrCreateClient("iam", () -> {
      return IamClient.builder()
          .credentialsProvider(StaticCredentialsProvider.create(createAwsBasicCredentials(configuration)))
          .region(Region.of(awsbedrockAgentsParameters.getRegion()))
          .build();
    });
  }

  public static BedrockAgentRuntimeClient getAgentRuntimeClient(AwsbedrockConfiguration configuration,
                                                                AwsbedrockParameters awsBedrockParameters) {
    return getOrCreateClient("agentRuntime", () -> BedrockAgentRuntimeClient.builder()
        .region(Region.of(awsBedrockParameters.getRegion())).build());
  }


  private static SdkHttpClient getConfiguredHttpClient(AwsbedrockConfiguration configuration) {
    ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

    if (configuration.getProxyConfig() != null) {
      software.amazon.awssdk.http.apache.ProxyConfiguration proxyConfig = software.amazon.awssdk.http.apache.ProxyConfiguration
          .builder()
          .endpoint(URI.create(String.format("%s://%s:%d",
                                             configuration.getProxyConfig().getScheme(),
                                             configuration.getProxyConfig().getHost(),
                                             configuration.getProxyConfig().getPort())))
          .username(configuration.getProxyConfig().getUsername())
          .password(configuration.getProxyConfig().getPassword())
          .nonProxyHosts(configuration.getProxyConfig().getNonProxyHosts())
          .build();

      httpClientBuilder.proxyConfiguration(proxyConfig);

      // Configure truststore if available
      if (configuration.getProxyConfig().getTrustStorePath() != null) {
        TlsTrustManagersProvider tlsTrustManagersProvider = createTlsKeyManagersProvider(
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStorePath(),
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStorePassword(),
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStoreType().name());

        httpClientBuilder.tlsTrustManagersProvider(tlsTrustManagersProvider);
      }
    }

    httpClientBuilder.socketTimeout(CommonUtils.toDuration(configuration.getTimeout(), configuration.getTimeoutUnit()));
    return httpClientBuilder.build();
  }

  private static SdkAsyncHttpClient getConfiguredAsyncHttpClient(AwsbedrockConfiguration configuration) {
    NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();

    // Configure HTTP client if proxy or truststore is needed
    if (configuration.getProxyConfig() != null) {

      software.amazon.awssdk.http.nio.netty.ProxyConfiguration proxyConfig =
          software.amazon.awssdk.http.nio.netty.ProxyConfiguration
              .builder()
              .host(configuration.getProxyConfig().getHost())
              .port(configuration.getProxyConfig().getPort())
              .username(configuration.getProxyConfig().getUsername())
              .password(configuration.getProxyConfig().getPassword())
              .nonProxyHosts(configuration.getProxyConfig().getNonProxyHosts())
              .build();

      httpClientBuilder.proxyConfiguration(proxyConfig);

      // Configure truststore if available
      if (configuration.getProxyConfig().getTrustStorePath() != null) {
        TlsTrustManagersProvider tlsTrustManagersProvider = createTlsKeyManagersProvider(
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStorePath(),
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStorePassword(),
                                                                                         configuration.getProxyConfig()
                                                                                             .getTrustStoreType().name());

        httpClientBuilder.tlsTrustManagersProvider(tlsTrustManagersProvider);
      }
    }

    // Configure ALL timeouts for async client - critical for streaming operations
    Integer timeout = configuration.getTimeout();
    TimeUnitEnum timeoutUnit = configuration.getTimeoutUnit();

    // Use configured timeout or default to 300 seconds if not set
    int effectiveTimeout = (timeout != null) ? timeout : 300;
    TimeUnitEnum effectiveUnit = (timeoutUnit != null) ? timeoutUnit : TimeUnitEnum.SECONDS;
    Duration timeoutDuration = CommonUtils.toDuration(effectiveTimeout, effectiveUnit);

    logger
        .debug("Configuring async HTTP client timeouts: {} {} (readTimeout, writeTimeout, connectionTimeout, connectionAcquisitionTimeout, tlsNegotiationTimeout)",
               effectiveTimeout, effectiveUnit);

    httpClientBuilder.readTimeout(timeoutDuration)
        .writeTimeout(timeoutDuration) // Critical for streaming write operations
        .connectionTimeout(timeoutDuration) // Connection establishment - respects configured timeout
        .connectionAcquisitionTimeout(timeoutDuration) // Getting connection from pool
        .tlsNegotiationTimeout(timeoutDuration); // TLS handshake - was using default ~5 seconds!

    return httpClientBuilder.build();
  }

  private static TlsTrustManagersProvider createTlsKeyManagersProvider(String trustStorePath,
                                                                       String trustStorePassword, String trustStoreType) {
    try {
      // Load the truststore (supports JKS, PKCS12, etc.)
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);
      try (FileInputStream fis = new FileInputStream(trustStorePath)) {
        trustStore.load(fis, trustStorePassword != null ? trustStorePassword.toCharArray() : null);
      }

      TrustManagerFactory trustManagerFactory = TrustManagerFactory
          .getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);

      return () -> trustManagerFactory.getTrustManagers();
    } catch (Exception e) {
      throw new RuntimeException("Failed to configure JKS truststore", e);
    }
  }

  private static AwsCredentials createAwsBasicCredentials(AwsbedrockConfiguration configuration) {

    if (configuration.getAwsSessionToken() == null || configuration.getAwsSessionToken().isEmpty()) {
      return AwsBasicCredentials.create(
                                        configuration.getAwsAccessKeyId(),
                                        configuration.getAwsSecretAccessKey());
    } else {

      return AwsSessionCredentials.create(
                                          configuration.getAwsAccessKeyId(),
                                          configuration.getAwsSecretAccessKey(),
                                          configuration.getAwsSessionToken());
    }
  }

  public static void closeAll() {
    clients.values().forEach(client -> {
      if (client instanceof AutoCloseable) {
        try {
          ((AutoCloseable) client).close();
        } catch (Exception e) {
          System.err.println("Error closing client: " + e.getMessage());
        }
      }
    });
    clients.clear();
  }

}
