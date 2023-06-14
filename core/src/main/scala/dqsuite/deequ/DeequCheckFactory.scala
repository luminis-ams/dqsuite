package dqsuite.deequ

import com.amazon.deequ.checks.Check
import dqsuite.config.{CheckConfig, SourceConfig}
import dqsuite.utils.RuntimeCompileUtils

/** Factory for building Deequ Checks from configuration.
  */
private[dqsuite] object DeequCheckFactory {
  private def build(config: CheckConfig): Check = {
    val level      = config.level.toString
    val name       = config.name.getOrElse(s"Check failed for ${config.column}")
    val expression = config.expression.replace("@", s""""${config.column}"""")
    val source =
      s"""
         |import com.amazon.deequ.constraints.ConstrainableDataTypes
         |com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.$level, \"$name\")
         |$expression
    """.stripMargin

    val check = RuntimeCompileUtils.evaluate(source).asInstanceOf[Check]
    check
  }

  def buildSeq(
    config: SourceConfig
  ): Seq[Check] = {
    config.checks
      .filter(_.enabled)
      .map(DeequCheckFactory.build)
  }
}
