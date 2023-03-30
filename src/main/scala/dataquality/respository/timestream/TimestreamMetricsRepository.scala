package dataquality.respository.timestream

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.amazonaws.services.glue.log.GlueLogger
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient
import software.amazon.awssdk.services.timestreamwrite.model.{RejectedRecordsException, WriteRecordsRequest}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class TimestreamMetricsRepository(
  timestreamWriteClient: TimestreamWriteClient,
  timestreamQueryClient: TimestreamQueryClient,
  databaseName: String,
  tableName: String,
) extends MetricsRepository {
  val log = new GlueLogger()

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
      log.info(s"WriteRecords Status: ${writeRecordsResponse.sdkHttpResponse().statusCode()}")
    } catch {
      case e: RejectedRecordsException => {
        log.error(s"RejectedRecordsException: ${e.getMessage}")
        e.rejectedRecords().forEach(record => {
          log.error(s"RejectedRecord: ${record.toString}")
        })
      }
      case e: Exception => {
        log.error(s"Exception: ${e.getMessage}")
      }
    }
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = None

  override def load(): MetricsRepositoryMultipleResultsLoader = new TimestreamMetricsRepositoryMultipleResultsLoader
}