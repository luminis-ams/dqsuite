from datetime import datetime
from typing import Optional, Any

from pydeequ.verification import VerificationResult
from pyspark.sql import SparkSession


class PY4JClassWrapper:
    def __init__(self, spark_session: SparkSession, instance: Any) -> None:
        super().__init__()
        self._spark_session = spark_session
        self._instance = instance

    def _callj(self, attr, *args, **kwargs):
        return getattr(self._instance, attr)(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._instance, attr)

    def _to_optionj(self, value):
        return self._spark_session._jvm.scala.Option.apply(value)


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
        self._instance = self._callj("withTimestreamRepository", database, table)
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


class DQSuiteDatasetContext(PY4JClassWrapper):
    def profile(self, df) -> Any:
        return self._callj("profile", df._jdf)

    def validate(self, df) -> Any:
        return VerificationResult(self._spark_session, self._callj("validate", df._jdf))
