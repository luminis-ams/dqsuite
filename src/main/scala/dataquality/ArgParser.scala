package dataquality

import dataquality.Action.Action
import net.sourceforge.argparse4j.ArgumentParsers

import java.net.URI
import java.time.Instant

object ArgParser {
  def parse(args: Array[String]) = {
    val parser = ArgumentParsers
      .newArgumentParser("dataquality")

    parser.addArgument("--config_path").required(true)
    parser.addArgument("--source_name").required(true)
    parser.addArgument("--actions").required(true)
    parser.addArgument("--metrics_path").required(true)
    parser.addArgument("--result_path").required(true)
    parser.addArgument("--run_name")
    parser.addArgument("--partition")
    parser.addArgument("--dataset_timestamp")

    parser.addArgument("--glue_database")
    parser.addArgument("--glue_table")

    parser.addArgument("--input_file_path")

    parser.addArgument("--timestream_database")
    parser.addArgument("--timestream_table")

    val unkownArgs = new java.util.ArrayList[String]()

    val rawArgs = parser.parseKnownArgs(args, unkownArgs)

    val dataSource = (rawArgs.getString("glue_database"), rawArgs.getString("glue_table"), rawArgs.getString("input_file_path")) match {
      case (null, null, null) => throw new RuntimeException("No data source specified")
      case (null, null, path) => FilesystemDataSourceArgs(path)
      case (database, table, null) => GlueTableDataSourceArgs(database, table)
      case _ => throw new RuntimeException("Only one data source can be specified")
    }
    val datasetTimestamp = rawArgs.getString("dataset_timestamp") match {
      case null => Instant.now()
      case timestamp => Instant.parse(timestamp)
    }
    val sourceName = rawArgs.getString("source_name")
    val runName = rawArgs.getString("run_name") match {
      case null => s"${Instant.now().toString.replace(":", "-")}"
      case name => name
    }

    val actions = rawArgs.getString("actions").split(",").map(_.trim).map(_.toUpperCase).toSet
    actions.foreach(action => {
      if (!Action.values.contains(action)) {
        throw new RuntimeException(s"Invalid action: $action")
      }
    })

    val timestreamRepository = (rawArgs.getString("timestream_database"), rawArgs.getString("timestream_table")) match {
      case (null, null) => None
      case (database, table) => Some(TimestreamMetricsRepository(database, table))
      case _ => throw new RuntimeException("Missing timestream database or table")
    }

    Args(
      rawArgs.get("config_path"),
      sourceName,
      actions,
      URI.create(rawArgs.getString("metrics_path").stripSuffix("/") + "/"),
      URI.create(rawArgs.getString("result_path").stripSuffix("/") + "/"),
      runName,
      Option(rawArgs.get("partition")),
      datasetTimestamp,
      dataSource,
      timestreamRepository
    )
  }
}

case class Args(
  configPath: String,
  sourceName: String,
  actions: Set[Action],
  metricsPath: URI,
  resultPath: URI,
  runName: String,
  partition: Option[String],
  datasetTimestamp: Instant,
  dataSource: DataSourceArgs,

  timestreamRepository: Option[TimestreamMetricsRepository],
)

sealed trait DataSourceArgs
case class GlueTableDataSourceArgs(database: String, table: String) extends DataSourceArgs
case class FilesystemDataSourceArgs(path: String) extends DataSourceArgs

sealed trait MetricsRepositoryArgs
case class TimestreamMetricsRepository(database: String, table: String) extends MetricsRepositoryArgs

object Action {
  type Action = String
  val PROFILE = "PROFILE"
  val VALIDATE = "VALIDATE"
  val values: Set[Action] = Set(PROFILE, VALIDATE)
}

