import logging
from collections.abc import Callable

from tenacity import RetryCallState

from cimsparql.utils import query_name

logger = logging.getLogger()


class RetryCallback:
    def __init__(self) -> None:
        self.query_name = ""

    def pre_call(self, query: str) -> None:
        try:
            self.query_name = query_name(query)
        except Exception as exc:
            logger.warning("pre_call: %s", exc)

    def before(self, _: RetryCallState) -> None: ...

    def after(self, retry_state: RetryCallState) -> None:
        if outcome := retry_state.outcome:
            logger.info(
                "Query: %s: Attempt %s ended with: %s",
                self.query_name,
                retry_state.attempt_number,
                outcome.exception(),
            )


RetryCallbackFactory = Callable[[], RetryCallback]
