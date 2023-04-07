package dqsuite.deequ

import com.amazon.deequ.checks.Check
import dqsuite.config.{CheckConfig, SourceConfig}
import dqsuite.utils.RuntimeCompileUtils

object DeequCheckFactory {
  private def build(config: CheckConfig): Option[Check] = {
    if (!config.enabled) {
      return None
    }

    val level = config.level.toString
    val name = config.name.getOrElse(s"Check failed for ${config.column}")
    val expression = config.expression.replace("@", s""""${config.column}"""")
    val source =
      s"""
         |import com.amazon.deequ.constraints.ConstrainableDataTypes
         |com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.$level, \"$name\")
         |$expression
    """.stripMargin

    val check = RuntimeCompileUtils.evaluate(source).asInstanceOf[Check]
    Some(check)
  }

  def buildSeq(
    config: SourceConfig,
  ): Seq[Check] = {
    config.checks.flatMap(DeequCheckFactory.build)
  }
}
