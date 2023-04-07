package dqsuite.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric
import dqsuite.config.{AnalyzerConfig, SourceConfig}
import dqsuite.utils.RuntimeCompileUtils

private[dqsuite] object DeequAnalyserFactory {
  private[deequ] def build(
    config: AnalyzerConfig,
  ): Seq[Analyzer[_, Metric[_]]] = {
    if (!config.enabled) {
      return Seq.empty
    }

    val expression = config.expression.replace("@", s""""${config.column}"""")
    val source =
      s"""
         |import com.amazon.deequ.analyzers._
         |$expression :: Nil
         |""".stripMargin

    val analyzers = RuntimeCompileUtils.evaluate(source).asInstanceOf[Seq[Analyzer[_, Metric[_]]]]
    analyzers
  }

  def buildSeq(
    config: SourceConfig,
  ): Seq[Analyzer[_, Metric[_]]] = {
    config.analyzers.flatMap(DeequAnalyserFactory.build)
  }
}
