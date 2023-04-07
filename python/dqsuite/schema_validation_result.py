from pyspark.sql import DataFrame, SQLContext

from dqsuite.utils import PY4JClassWrapper


class SchemaValidationResult(PY4JClassWrapper):
    @property
    def numValidRows(self) -> int:
        return int(self._instance.numValidRows())

    @property
    def numInvalidRows(self) -> int:
        return int(self._instance.numInvalidRows())

    @property
    def validRows(self) -> DataFrame:
        dfj = self._callj("validRows")

        sql_ctx = SQLContext(
            sparkContext=self._spark_session._sc,
            sparkSession=self._spark_session,
            jsqlContext=self._spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(dfj, sql_ctx)

    @property
    def invalidRows(self) -> DataFrame:
        dfj = self._callj("invalidRows")

        sql_ctx = SQLContext(
            sparkContext=self._spark_session._sc,
            sparkSession=self._spark_session,
            jsqlContext=self._spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(dfj, sql_ctx)
