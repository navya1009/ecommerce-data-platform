class PipelineError(Exception):
    pass


class PipelineDataQualityError(PipelineError):
    def __init__(
        self, message, suite_name=None, failed_expectations=None, statistics=None
    ):
        self.suite_name = suite_name
        self.failed_expectations = failed_expectations or []
        self.statistics = statistics or {}
        super().__init__(message)
