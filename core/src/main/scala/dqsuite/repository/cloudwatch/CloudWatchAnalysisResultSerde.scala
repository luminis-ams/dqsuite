package dqsuite.repository.cloudwatch

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.ResultKey
import dqsuite.repository.cloudwatch.CloudWatchAnalysisResultSerde.logger
import dqsuite.repository.timestream.TimestreamAnalysisResultSerde.logger
import org.apache.logging.log4j.LogManager
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum, StandardUnit}
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType

import java.time.Instant
import scala.jdk.CollectionConverters.seqAsJavaListConverter
import scala.util.Success

private[dqsuite] object CloudWatchAnalysisResultSerde {
  val logger = LogManager.getLogger()

  val TAGS_PREFIX_FIELD = "tags_"
  val ENTITY_FIELD      = "entity"
  val INSTANCE_FIELD    = "instance"

  def analysisResultToCloudWatchDatums(
    resultKey: ResultKey,
    analyzerContext: AnalyzerContext
  ): Seq[MetricDatum] = {
    val tags = resultKey.tags
      .map(tag => {
        Dimension
          .builder()
          .name(s"$TAGS_PREFIX_FIELD${tag._1}")
          .value(tag._2)
          .build()
      })
      .toSeq

    val time = Instant.ofEpochMilli(resultKey.dataSetDate)
    val baseMetricBuilder = MetricDatum
      .builder()
      .unit(StandardUnit.NONE)
      .timestamp(time)
      .dimensions(tags.asJava)

    val datums = analyzerContext.allMetrics
      .groupBy(metric => (metric.entity.toString, metric.instance, metric.name))
      .map({ case (key, metricGroup) =>
        if (metricGroup.size > 1) {
          logger.warn(s"Multiple metrics with the same identity $key.")
        }
        metricGroup.head
      })
      .flatMap(metric => metricToRecord(metric, baseMetricBuilder))
      .toSeq

    datums
  }

  def metricToRecord(
    metric: Metric[_],
    baseMetric: MetricDatum.Builder
  ): Option[MetricDatum] = {
    val dimensions = Seq(
      Dimension
        .builder()
        .name(ENTITY_FIELD)
        .value(metric.entity.toString)
        .build(),
      Dimension
        .builder()
        .name(INSTANCE_FIELD)
        .value(metric.instance)
        .build()
    )

    val measureValue = metric match {
      case DoubleMetric(_, _, _, value, _) =>
        value match {
          case Success(v) => v
          case _ =>
            logger.warn(s"Metric ${metric.name} has a NaN value")
            return None
        }
      case _ => {
        logger.warn(s"Unsupported metric type ${metric.getClass} for ${metric.name}")
        return None
      }
    }

    val datumBuilder = baseMetric
      .copy()
      .dimensions(dimensions.asJava)
      .metricName(metric.name)
      .value(measureValue)

    Some(datumBuilder.build())
  }
}
