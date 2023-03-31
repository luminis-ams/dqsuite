package dqsuite.repository.timestream

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient

import java.time.Duration

private[dqsuite] case class TimestreamMetricsRepositoryBuilder(
  databaseName: Option[String] = None,
  tableName: Option[String] = None,
) {
  def useTable(databaseName: String, tableName: String): TimestreamMetricsRepositoryBuilder =
    copy(databaseName = Some(databaseName), tableName = Some(tableName))

  def build: TimestreamMetricsRepository = {

    val httpClientBuilder = ApacheHttpClient.builder
      .maxConnections(5000)

    val retryPolicy = RetryPolicy.builder
      .numRetries(10)
      .build

    val overrideConfig = ClientOverrideConfiguration.builder
      .apiCallAttemptTimeout(Duration.ofSeconds(20))
      .retryPolicy(retryPolicy)
      .build

    val writeClient = TimestreamWriteClient.builder
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfig)
      .build

    val queryClient = TimestreamQueryClient.builder.build

    new TimestreamMetricsRepository(
      writeClient,
      queryClient,
      databaseName.get,
      tableName.get
    )
  }
}

object TimestreamMetricsRepositoryBuilder {
  def builder: TimestreamMetricsRepositoryBuilder = new TimestreamMetricsRepositoryBuilder()
}
