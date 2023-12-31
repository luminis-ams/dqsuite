package dqsuite

import com.amazon.deequ.repository.MetricsRepository
import dqsuite.config.DQSuiteConfig
import dqsuite.repository.cloudwatch.CloudWatchMetricsRepositoryBuilder
import dqsuite.repository.timestream.TimestreamMetricsRepositoryBuilder
import dqsuite.utils.{HdfsUtils, PathUtils}
import org.apache.spark.sql.SparkSession

import java.net.URI

case class DQSuiteContextBuilder(
  configPath: Option[URI] = None,
  metricsPath: Option[URI] = None,
  resultPath: Option[URI] = None,
  empheralRepositories: Seq[MetricsRepository] = Seq.empty,
  spark: Option[SparkSession] = None
) {
  def withConfigPath(configPath: URI): DQSuiteContextBuilder = copy(configPath = Some(configPath))

  def withConfigPath(configPath: String): DQSuiteContextBuilder =
    copy(configPath = Some(URI.create(PathUtils.ensureTrailingSlash(configPath))))

  def withMetricsPath(metricsPath: URI): DQSuiteContextBuilder = copy(metricsPath = Some(metricsPath))

  def withMetricsPath(metricsPath: String): DQSuiteContextBuilder =
    copy(metricsPath = Some(URI.create(PathUtils.ensureTrailingSlash(metricsPath))))

  def withResultPath(resultPath: URI): DQSuiteContextBuilder = copy(resultPath = Some(resultPath))

  def withResultPath(resultPath: String): DQSuiteContextBuilder =
    copy(resultPath = Some(URI.create(PathUtils.ensureTrailingSlash(resultPath))))

  def withEmpheralRepository(repository: MetricsRepository): DQSuiteContextBuilder =
    copy(empheralRepositories = empheralRepositories :+ repository)

  def withSparkSession(spark: SparkSession): DQSuiteContextBuilder = copy(spark = Some(spark))

  def build: DQSuiteContext = {
    DQSuiteContext(
      HdfsUtils.readFromFileOnDfs(spark.get, configPath.get.toString)(DQSuiteConfig.loadStream),
      metricsPath.get,
      resultPath.get,
      empheralRepositories,
      spark.get
    )
  }
}

object DQSuiteContextBuilder {
  def builder: DQSuiteContextBuilder = DQSuiteContextBuilder()
}
