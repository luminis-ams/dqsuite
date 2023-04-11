from typing import List

from pyspark.sql import DataFrame, SQLContext

from dqsuite.utils import PY4JClassWrapper


class SchemaCheckResult(PY4JClassWrapper):
    @property
    def missingColumns(self) -> List[str]:
        return seq_to_list(self._instance.missingColumns())

    @property
    def extraColumns(self) -> List[str]:
        return seq_to_list(self._instance.extraColumns())

    @property
    def numValidRows(self) -> int:
        return int(self._instance.numValidRows())

    @property
    def numInvalidRows(self) -> int:
        return int(self._instance.numInvalidRows())

    @property
    def isValid(self) -> bool:
        return bool(self._instance.isValid())

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

    def __str__(self):
        return f"SchemaCheckResult(" \
               f"missingColumns={self.missingColumns}, " \
               f"extraColumns={self.extraColumns}, " \
               f"numValidRows={self.numValidRows}, " \
               f"numInvalidRows={self.numInvalidRows}, " \
               f"isValid={self.isValid}" \
               f")"


def seq_to_list(seq):
    return [seq.apply(i) for i in range(seq.size())]
