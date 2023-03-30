package dataquality.respository.timestream

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepositoryMultipleResultsLoader}

class TimestreamMetricsRepositoryMultipleResultsLoader extends MetricsRepositoryMultipleResultsLoader {
  override def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = ???

  override def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader = ???

  override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = ???

  override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = ???

  override def get(): Seq[AnalysisResult] = ???
}
