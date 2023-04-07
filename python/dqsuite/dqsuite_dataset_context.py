from typing import Any

from pydeequ.verification import VerificationResult

from dqsuite.schema_check_result import SchemaCheckResult
from dqsuite.utils import PY4JClassWrapper


class DQSuiteDatasetContext(PY4JClassWrapper):
    def profile(self, df) -> Any:
        return self._callj("profile", df._jdf)

    def checkSchema(self, df) -> SchemaCheckResult:
        return SchemaCheckResult(self._spark_session, self._callj("checkSchema", df._jdf))

    def validate(self, df, anomalyDetection: bool = True) -> VerificationResult:
        return VerificationResult(self._spark_session, self._callj("validate", df._jdf, anomalyDetection))
