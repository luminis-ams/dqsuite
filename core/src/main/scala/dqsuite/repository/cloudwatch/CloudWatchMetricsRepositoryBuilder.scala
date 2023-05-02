package dqsuite.repository.cloudwatch

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient

import java.time.Duration

case class CloudWatchMetricsRepositoryBuilder(
  namespace: Option[String] = None,
) {
  def useNamespace(namespace: String): CloudWatchMetricsRepositoryBuilder =
    copy(namespace = Some(namespace))

  def build: CloudWatchMetricsRepository = {
    val httpClientBuilder = ApacheHttpClient.builder
      .maxConnections(5000)

    val retryPolicy = RetryPolicy.builder
      .numRetries(10)
      .build

    val overrideConfig = ClientOverrideConfiguration.builder
      .apiCallAttemptTimeout(Duration.ofSeconds(20))
      .retryPolicy(retryPolicy)
      .build

    val cloudwatchClient = CloudWatchClient.builder
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfig)
      .build

    new CloudWatchMetricsRepository(
      cloudwatchClient,
      namespace.get
    )
  }
}
