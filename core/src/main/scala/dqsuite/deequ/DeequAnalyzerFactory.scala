package dqsuite.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric
import dqsuite.config.{AnalyzerConfig, SourceConfig}
import dqsuite.utils.RuntimeCompileUtils

/** Factory for building Deequ Analyzers from configuration.
  */
private[dqsuite] object DeequAnalyzerFactory {
  private[deequ] def build(
    config: AnalyzerConfig
  ): Seq[Analyzer[_, Metric[_]]] = {
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
    config: SourceConfig
  ): Seq[Analyzer[_, Metric[_]]] = {
    config.analyzers
      .filter(_.enabled)
      .flatMap(DeequAnalyzerFactory.build)
  }
}
