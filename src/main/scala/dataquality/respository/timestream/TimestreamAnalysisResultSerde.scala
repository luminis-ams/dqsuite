package dataquality.respository.timestream

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.ResultKey
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.timestreamwrite.model.MeasureValueType
import software.amazon.awssdk.services.timestreamwrite.model.{Dimension, Record, TimeUnit}

import scala.jdk.CollectionConverters.seqAsJavaListConverter
import scala.tools.nsc.util.HashSet
import scala.util.Success

object TimestreamAnalysisResultSerde {
  val log = new GlueLogger()

//  val DATASET_DATE_FIELD = "dataSetDate"
  val TAGS_PREFIX_FIELD = "tags_"
  val ENTITY_FIELD = "entity"
  val INSTANCE_FIELD = "instance"
  val NAME_FIELD = "name"
//  val VALUE_FIELD = "value"

  def analysisResultToTimescaleRecords(
    resultKey: ResultKey,
    analyzerContext: AnalyzerContext
  ): (Record, Seq[Record]) = {
    val tags = resultKey.tags
      .map(tag => {
        Dimension
          .builder()
          .name(s"$TAGS_PREFIX_FIELD${tag._1}")
          .value(tag._2)
          .build()
      })
      .toSeq

    val time = resultKey.dataSetDate
    val commonAttributes = Record
      .builder()
      .dimensions(tags.asJava)
      .time(time.toString)
      .timeUnit(TimeUnit.MILLISECONDS)
      .version(System.currentTimeMillis)

    val records = analyzerContext.allMetrics
      .groupBy(metric => (metric.entity.toString, metric.instance, metric.name))
      .map({case (key, metricGroup) =>
        if (metricGroup.size > 1) {
          log.warn(s"Multiple metrics with the same identity ${key}.")
        }
        metricGroup.head
      })
      .flatMap(metricToRecord)
      .toSeq

    (commonAttributes.build(), records)
  }

  def metricToRecord(
    metric: Metric[_],
  ): Option[Record] = {
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
        .build(),
      Dimension
        .builder()
        .name(NAME_FIELD)
        .value(metric.name)
        .build()
    )

    val (measureValueType, measureValue) = metric match {
      case DoubleMetric(_, _, _, value, _) =>
        (
          MeasureValueType.DOUBLE.toString,
          value match {
            case Success(v) => v.toString
            case _ =>
              log.warn(s"Metric ${metric.name} has a NaN value")
              return None
          }
        )
      case _ => {
        log.warn(s"Unsupported metric type ${metric.getClass} for ${metric.name}")
        return None
      }
    }

    val record = Record
      .builder()
      .dimensions(dimensions.asJava)
      .measureName(metric.name)
      .measureValue(measureValue)
      .measureValueType(measureValueType)

    Some(record.build())
  }

}
