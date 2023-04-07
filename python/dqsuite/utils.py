from typing import Any

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
