package dqsuite.repository.timestream

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.logging.log4j.LogManager
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient
import software.amazon.awssdk.services.timestreamwrite.model.{RejectedRecordsException, WriteRecordsRequest}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

private[dqsuite] class TimestreamMetricsRepository(
  timestreamWriteClient: TimestreamWriteClient,
  databaseName: String,
  tableName: String,
) extends MetricsRepository {
  val logger = LogManager.getLogger()

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val (commonAttributes, records) = TimestreamAnalysisResultSerde.analysisResultToTimescaleRecords(resultKey, analyzerContext)

    val writeRecordsStatement = WriteRecordsRequest
      .builder
      .databaseName(databaseName)
      .tableName(tableName)
      .records(records.asJava)
      .commonAttributes(commonAttributes)
      .build

    try {
      val writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsStatement)
      logger.info(s"WriteRecords Status: ${writeRecordsResponse.sdkHttpResponse().statusCode()}")
    } catch {
      case e: RejectedRecordsException => {
        logger.error(s"RejectedRecordsException: ${e.getMessage}")
        e.rejectedRecords().forEach(record => {
          logger.error(s"RejectedRecord: ${record.toString}")
        })
      }
      case e: Exception => {
        logger.error(s"Exception: ${e.getMessage}")
      }
    }
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = None

  override def load(): MetricsRepositoryMultipleResultsLoader = new TimestreamMetricsRepositoryMultipleResultsLoader
}