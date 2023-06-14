package dqsuite.deequ

import com.amazon.deequ.AnomalyCheckConfig
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.metrics.Metric
import dqsuite.config.{AnomalyDetectionConfig, SourceConfig}
import dqsuite.utils.RuntimeCompileUtils

import java.time.Instant

/** Factory for building Deequ Analyzers from configuration.
  */
private[dqsuite] object DeequAnomalyDetectorFactory {
  private def build(
    datasetDate: Option[Instant],
    config: AnomalyDetectionConfig,
    withTags: Map[String, String] = Map.empty
  ): AnomalyDetectionInstance = {
    val expression = config.strategy.replace("@", s""""${config.column}"""")
    val source =
      s"""
         |import com.amazon.deequ.anomalydetection._
         |$expression
    """.stripMargin
    val strategy = RuntimeCompileUtils.evaluate(source).asInstanceOf[AnomalyDetectionStrategy]

    val analyser =
      DeequAnalyzerFactory.build(config.analyser).head.asInstanceOf[Analyzer[_ <: State[_], Metric[Double]]]

    val level       = CheckLevel.withName(config.level.toString.capitalize)
    val description = config.description.getOrElse(s"Anomaly detected for ${config.column}")

    val dateTo   = datasetDate.getOrElse(Instant.now()).toEpochMilli
    val dateFrom = config.historyWindow.map(w => dateTo - w)

    val anomalyCheckConfig = AnomalyCheckConfig(
      level,
      description,
      config.withTags ++ withTags,
      dateFrom,
      dateFrom.map(_ => dateTo)
    )

    AnomalyDetectionInstance(analyser, strategy, anomalyCheckConfig)
  }

  def buildSeq(
    datasetDate: Option[Instant],
    config: SourceConfig,
    withTags: Map[String, String] = Map.empty
  ): Seq[AnomalyDetectionInstance] = {
    config.anomalyDetection
      .filter(_.enabled)
      .map(DeequAnomalyDetectorFactory.build(datasetDate, _, config.tags ++ withTags))
  }
}

private[dqsuite] case class AnomalyDetectionInstance(
  analyser: Analyzer[_ <: State[_], Metric[Double]],
  strategy: AnomalyDetectionStrategy,
  config: AnomalyCheckConfig
) {}
