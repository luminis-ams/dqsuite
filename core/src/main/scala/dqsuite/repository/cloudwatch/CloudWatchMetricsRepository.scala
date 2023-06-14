package dqsuite.repository.cloudwatch

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.logging.log4j.LogManager
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.{CloudWatchException, PutMetricDataRequest}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

/** Implements [[com.amazon.deequ.repository.MetricsRepository]] to save metrics to AWS CloudWatch.
  * @param cloudWatchClient
  *   The AWS SDK CloudWatch client
  * @param namespace
  *   The CloudWatch namespace to save metrics to
  */
private[dqsuite] class CloudWatchMetricsRepository(
  cloudWatchClient: CloudWatchClient,
  namespace: String
) extends MetricsRepository {
  val logger = LogManager.getLogger()

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val data = CloudWatchAnalysisResultSerde.analysisResultToCloudWatchDatums(resultKey, analyzerContext)

    val dataset          = resultKey.tags("dataset")
    val datasetNamespace = s"${namespace}/${dataset}"

    val request = PutMetricDataRequest
      .builder()
      .namespace(datasetNamespace)
      .metricData(data.asJava)
      .build()

    try {
      val response = cloudWatchClient.putMetricData(request)
      logger.info(s"PutMetricsData Status: ${response.sdkHttpResponse().statusCode()}")
    } catch {
      case e: CloudWatchException => {
        logger.error(s"CloudWatchException: ${e.getMessage}")
      }
      case e: Exception => {
        logger.error(s"Exception: ${e.getMessage}")
      }
    }
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = None

  override def load(): MetricsRepositoryMultipleResultsLoader = new CloudWatchMetricsRepositoryMultipleResultsLoader
}
