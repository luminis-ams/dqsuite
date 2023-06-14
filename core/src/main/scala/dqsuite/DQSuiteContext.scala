package dqsuite

import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import dqsuite.config.DQSuiteConfig
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.time.Instant

case class DQSuiteContext(
  config: DQSuiteConfig,
  metricsPath: URI,
  resultPath: URI,
  empheralRespositories: Seq[MetricsRepository],
  spark: SparkSession
) {
  def withDataset(
    name: String,
    datasetTimestamp: Option[Instant] = None,
    runName: Option[String] = None,
    partition: Option[String] = None
  ): DQSuiteDatasetContext = {
    val sourceConfig = config.sources.get(name) match {
      case Some(config) => config
      case None         => throw new RuntimeException(s"No config found for source: '$name'")
    }

    val runNameOuter = runName.getOrElse(s"${Instant.now().toString}")

    // Create result key (for storing metrics).
    val resultKey = ResultKey(
      datasetTimestamp.getOrElse(Instant.now()).toEpochMilli,
      Map(
        "dataset"                    -> name,
        "run_name"                   -> runNameOuter
      ) ++ partition.map("partition" -> _) ++ sourceConfig.tags
    )

    val resultPath     = this.resultPath.resolve(s"$name/").resolve(s"./runName=$runNameOuter/")
    val metricsPath    = this.metricsPath.resolve(s"$name/")
    val repositoryPath = metricsPath.resolve("repository/").resolve("metrics.json")

    val repository = FileSystemMetricsRepository(spark, repositoryPath.toString)

    DQSuiteDatasetContext(
      suiteContext = this,
      config = sourceConfig,
      metricsPath = metricsPath,
      resultPath = resultPath,
      resultKey = resultKey,
      repository = repository
    )
  }
}
