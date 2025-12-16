#=======================================
# file : oraculo/rules/router.py
#=======================================
from __future__ import annotations

import os
import asyncio
import time
import datetime as dt
from typing import Any, Dict, Optional

from loguru import logger
from telegram import Bot
from telegram.error import TelegramError

from oraculo.db import DB
from oraculo.obs import metrics as obs_metrics


def _as_mapping(obj: Any) -> Dict[str, Any]:
    """Best-effort to turn dict / pydantic / dataclass-like into a plain dict."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    # generic object with attributes
    try:
        keys = [k for k in dir(obj) if not k.startswith("_") and not callable(getattr(obj, k))]
        return {k: getattr(obj, k) for k in keys}
    except Exception:
        return {}


class TelegramRouter:
    """Envía mensajes a Telegram con rate-limit (token bucket) y logging en DB.

    Acepta configuración como dict o como modelos Pydantic (v1/v2):
      telegram:
        bot_events: { token: "...", chat_id: 123 }
        bot_rules:  { token: "...", chat_id: 456 }
        bot_errors: { token: "...", chat_id: 789 }
    """

    def __init__(self, cfg: Dict[str, Any] | Any, *, db: Optional[DB] = None, rate_limit_per_min: int = 60) -> None:
        # Normaliza raíz
        root = _as_mapping(cfg)
        tg = _as_mapping(root.get("telegram") or root)

        def norm_bot(bot: Dict[str, Any], env_token: str, env_chat: str) -> Dict[str, Any]:
            bot = _as_mapping(bot)
            token = bot.get("token") or os.getenv(env_token)
            chat_id = bot.get("chat_id") or os.getenv(env_chat) or 0
            try:
                chat_id = int(chat_id)
            except Exception:
                chat_id = 0
            return {"token": token, "chat_id": chat_id}

        self._targets: Dict[str, Dict[str, Any]] = {
            "events": norm_bot(tg.get("bot_events"), "TELEGRAM_BOT_EVENTS_TOKEN", "TELEGRAM_CHAT_EVENTS"),
            "rules":  norm_bot(tg.get("bot_rules"),  "TELEGRAM_BOT_RULES_TOKEN",  "TELEGRAM_CHAT_RULES"),
            "errors": norm_bot(tg.get("bot_errors"), "TELEGRAM_BOT_ERRORS_TOKEN", "TELEGRAM_CHAT_ERRORS"),
        }

        self._db = db
        # Rate limiting: token bucket (per minuto)
        self._bucket_cap = max(1, int(rate_limit_per_min))
        self._tokens = self._bucket_cap
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_refill
        if elapsed >= 60.0:
            units = int(elapsed // 60.0)
            self._tokens = min(self._bucket_cap, self._tokens + units * self._bucket_cap)
            self._last_refill = now

    async def send(self, kind: str, text: str, *, alert_id: Optional[int] = None, ts_first: Optional[Any] = None) -> None:
        # RL
        async with self._lock:
            self._refill()
            if self._tokens <= 0:
                logger.warning("[telegram] token bucket exhausted")
                channel = f"telegram/{kind}"
                obs_metrics.dispatch_attempts_total.labels(channel=channel).inc()
                obs_metrics.dispatch_last_attempt_ts.labels(channel=channel).set(time.time())
                obs_metrics.dispatch_dropped_total.labels(
                    channel=channel, reason="rate_limit"
                ).inc()
                return
            self._tokens -= 1

        target_cfg = self._targets.get(kind) or {}
        token = target_cfg.get("token")
        chat_id = int(target_cfg.get("chat_id") or 0)
        channel = f"telegram/{kind}"

        obs_metrics.dispatch_attempts_total.labels(channel=channel).inc()
        obs_metrics.dispatch_last_attempt_ts.labels(channel=channel).set(time.time())

        status = "sent=0"
        err: Optional[str] = None

        if not token or chat_id == 0:
            err = "missing credentials"
            logger.warning(f"[telegram] missing credentials for kind={kind}")
            logger.info(f"[telegram] {kind}@{chat_id} (skipped): {text}")
            obs_metrics.dispatch_dropped_total.labels(
                channel=channel, reason="disabled"
            ).inc()
        else:
            try:
                bot = Bot(token=token)
                await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
                logger.info(f"[telegram] {kind}@{chat_id}: {text}")
                status = "sent=1"
                obs_metrics.dispatch_success_total.labels(channel=channel).inc()
                obs_metrics.dispatch_last_success_ts.labels(channel=channel).set(time.time())
            except TelegramError as e:
                err = str(e)
                logger.error(f"[telegram] error: {e!s}")
                obs_metrics.dispatch_fail_total.labels(
                    channel=channel, kind=type(e).__name__
                ).inc()
            except Exception as e:
                err = str(e)
                logger.error(f"[telegram] unexpected error: {e!s}")
                obs_metrics.dispatch_fail_total.labels(
                    channel=channel, kind=type(e).__name__
                ).inc()

        # DB logging (FK-safe). Ignora si no hay DB o FK inválida.
        try:
            if self._db and alert_id is not None:
                # Obtiene ts_first de la tabla canónica. Si no existe, NO inserta (evita FK error).
                ts_first_db = await self._db.fetchval(
                    "SELECT ts_first FROM oraculo.rule_alerts WHERE id=$1",
                    alert_id
                )
                if ts_first_db is None:
                    logger.warning(f"[telegram] skip dispatch log: alert_id={alert_id} not found in rule_alerts")
                    obs_metrics.dispatch_dropped_total.labels(
                        channel=channel, reason="stale"
                    ).inc()
                else:
                    # event_time: ahora mismo
                    event_time = dt.datetime.now(dt.timezone.utc)
                    # target NO nulo (antes estaba NULL y rompía el NOT NULL)
                    target = f"{kind}@{chat_id}"
                    t0 = time.perf_counter()
                    await self._db.execute(
                        """
                        INSERT INTO oraculo.alert_dispatch_log(
                          alert_id,
                          alert_ts_first,
                          event_time,
                          target,
                          status,
                          error,
                          meta,
                          channel,
                          text,
                          extra
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb)
                        """,
                        alert_id,
                        ts_first_db,
                        event_time,
                        target,
                        status,
                        err,
                        "{}",                      # meta
                        f"telegram/{kind}",        # channel
                        text,                      # text enviado a Telegram
                        "{}",                      # extra
                    )
                    obs_metrics.db_insert_dispatch_log_ms.observe(
                        (time.perf_counter() - t0) * 1000
                    )
        except Exception as e:
            logger.error(f"[telegram] failed to log dispatch: {e!s}")
