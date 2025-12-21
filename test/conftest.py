import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover - sólo para entornos CI sin dependencias opcionales
    import loguru  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover
    import logging
    import types

    logging.basicConfig(level=logging.INFO)

    class _StubLogger:
        def __init__(self) -> None:
            self._logger = logging.getLogger("loguru_stub")

        def debug(self, *args, **kwargs):
            return self._logger.debug(*args, **kwargs)

        def info(self, *args, **kwargs):
            return self._logger.info(*args, **kwargs)

        def warning(self, *args, **kwargs):
            return self._logger.warning(*args, **kwargs)

        def error(self, *args, **kwargs):
            return self._logger.error(*args, **kwargs)

        def exception(self, *args, **kwargs):
            return self._logger.exception(*args, **kwargs)

        def success(self, *args, **kwargs):
            return self._logger.info(*args, **kwargs)

    stub = types.SimpleNamespace(logger=_StubLogger())
    sys.modules["loguru"] = stub

try:  # pragma: no cover - prometheus opcional en pruebas offline
    import prometheus_client  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover
    import types

    class _Metric:
        def __init__(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    prometheus_stub = types.SimpleNamespace(
        Counter=_Metric, Gauge=_Metric, Summary=_Metric, Histogram=_Metric, start_http_server=lambda *args, **kwargs: None
    )
    sys.modules["prometheus_client"] = prometheus_stub
