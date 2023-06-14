package dqsuite.repository.timestream

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient

import java.time.Duration

/** Builds a [[TimestreamMetricsRepository]] with the given configuration.
  * @param databaseName
  *   The Timestream database name
  * @param tableName
  *   The Timestream table name
  */
case class TimestreamMetricsRepositoryBuilder(
  databaseName: Option[String] = None,
  tableName: Option[String] = None
) {
  def useTable(databaseName: String, tableName: String): TimestreamMetricsRepositoryBuilder =
    copy(databaseName = Some(databaseName), tableName = Some(tableName))

  def build: TimestreamMetricsRepository = {

    val httpClientBuilder = ApacheHttpClient.builder
      .maxConnections(TimestreamMetricsRepositoryBuilder.CLIENT_MAX_CONNECTIONS)

    val retryPolicy = RetryPolicy.builder
      .numRetries(TimestreamMetricsRepositoryBuilder.CLIENT_NUM_RETRIES)
      .build

    val overrideConfig = ClientOverrideConfiguration.builder
      .apiCallAttemptTimeout(
        Duration.ofSeconds(TimestreamMetricsRepositoryBuilder.CLIENT_API_TIMEOUT_SECONDS)
      )
      .retryPolicy(retryPolicy)
      .build

    val writeClient = TimestreamWriteClient.builder
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfig)
      .build

    new TimestreamMetricsRepository(
      writeClient,
      databaseName.get,
      tableName.get
    )
  }
}

object TimestreamMetricsRepositoryBuilder {
  private val CLIENT_MAX_CONNECTIONS     = 5000
  private val CLIENT_NUM_RETRIES         = 10
  private val CLIENT_API_TIMEOUT_SECONDS = 20

  def builder: TimestreamMetricsRepositoryBuilder = new TimestreamMetricsRepositoryBuilder()
}
