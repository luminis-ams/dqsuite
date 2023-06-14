package dqsuite.repository.cloudwatch

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient

import java.time.Duration

/** Builds a [[CloudWatchMetricsRepository]] with the given configuration.
  * @param namespace
  *   The CloudWatch namespace to save metrics to
  */
case class CloudWatchMetricsRepositoryBuilder(
  namespace: Option[String] = None
) {
  def useNamespace(namespace: String): CloudWatchMetricsRepositoryBuilder =
    copy(namespace = Some(namespace))

  def build: CloudWatchMetricsRepository = {
    val httpClientBuilder = ApacheHttpClient.builder
      .maxConnections(CloudWatchMetricsRepositoryBuilder.CLIENT_MAX_CONNECTIONS)

    val retryPolicy = RetryPolicy.builder
      .numRetries(CloudWatchMetricsRepositoryBuilder.CLIENT_NUM_RETRIES)
      .build

    val overrideConfig = ClientOverrideConfiguration.builder
      .apiCallAttemptTimeout(
        Duration.ofSeconds(CloudWatchMetricsRepositoryBuilder.CLIENT_API_TIMEOUT_SECONDS)
      )
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

object CloudWatchMetricsRepositoryBuilder {
  private val CLIENT_MAX_CONNECTIONS     = 5000
  private val CLIENT_NUM_RETRIES         = 10
  private val CLIENT_API_TIMEOUT_SECONDS = 20

  def builder: CloudWatchMetricsRepositoryBuilder = new CloudWatchMetricsRepositoryBuilder()
}
