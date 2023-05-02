from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from dqsuite.dqsuite_dataset_context import DQSuiteDatasetContext
from dqsuite.utils import PY4JClassWrapper


class DQSuiteContextBuilder(PY4JClassWrapper):
    @staticmethod
    def builder(spark_session: SparkSession) -> "DQSuiteContextBuilder":
        return DQSuiteContextBuilder(
            spark_session,
            spark_session._jvm.dqsuite.DQSuiteContextBuilder.builder()
            .withSparkSession(spark_session._jsparkSession)
        )

    def withConfigPath(self, config_path: str) -> "DQSuiteContextBuilder":
        self._instance = self._callj("withConfigPath", config_path)
        return self

    def withMetricsPath(self, metrics_path: str) -> "DQSuiteContextBuilder":
        self._instance = self._callj("withMetricsPath", metrics_path)
        return self

    def withResultPath(self, result_path: str) -> "DQSuiteContextBuilder":
        self._instance = self._callj("withResultPath", result_path)
        return self

    def withTimestreamRepository(self, database: str, table: str) -> "DQSuiteContextBuilder":
        builder = self._spark_session._jvm.dqsuite.repository.timestream.TimestreamMetricsRepositoryBuilder.builder()
        builder = builder.useTable(database, table)
        self._instance = self._callj("withEmpheralRepository", builder.build())
        return self

    def withCloudwatchRepository(self, namespace: str) -> "DQSuiteContextBuilder":
        builder = self._spark_session._jvm.dqsuite.repository.cloudwatch.CloudWatchMetricsRepositoryBuilder.builder()
        builder = builder.useNamespace(namespace)
        self._instance = self._callj("withEmpheralRepository", builder.build())
        return self

    def build(self) -> "DQSuiteContext":
        return DQSuiteContext(self._spark_session, self._instance.build())


class DQSuiteContext(PY4JClassWrapper):
    def withDataset(
            self,
            name: str,
            datasetTimestamp: Optional[datetime] = None,
            runName: Optional[str] = None,
            partition: Optional[str] = None,
    ) -> "DQSuiteDatasetContext":
        context = self._callj(
            "withDataset",
            name,
            self._to_optionj(datasetTimestamp),
            self._to_optionj(runName),
            self._to_optionj(partition),
        )
        return DQSuiteDatasetContext(self._spark_session, context)
