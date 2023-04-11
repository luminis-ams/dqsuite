from typing import Any

from pydeequ.verification import VerificationResult
from pyspark.sql import DataFrame, SQLContext

from dqsuite.schema_check_result import SchemaCheckResult
from dqsuite.utils import PY4JClassWrapper


class DQSuiteDatasetContext(PY4JClassWrapper):
    def profile(self, df) -> Any:
        return self._callj("profile", df._jdf)

    def checkSchema(self, df, emptyStringAsNull: bool = True) -> SchemaCheckResult:
        return SchemaCheckResult(self._spark_session, self._callj("checkSchema", df._jdf, emptyStringAsNull))

    def validate(self, df, anomalyDetection: bool = True) -> VerificationResult:
        return VerificationResult(self._spark_session, self._callj("validate", df._jdf, anomalyDetection))

    def postprocess(self, df) -> DataFrame:
        dfj = self._callj("postprocess", df._jdf)

        sql_ctx = SQLContext(
            sparkContext=self._spark_session._sc,
            sparkSession=self._spark_session,
            jsqlContext=self._spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(dfj, sql_ctx)
