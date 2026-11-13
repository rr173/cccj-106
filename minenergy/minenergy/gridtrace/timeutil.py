"""带偏移时刻的解析。

现场数据里的所有时刻都是**带 UTC 偏移的 ISO 8601 字符串**（如
``2026-01-01T08:00:00+08:00``、``2026-01-01T00:00:00Z``）。模块内部统一
折算成 UTC 微秒整数参与比较；没有偏移的朴素时刻一律拒绝，避免按本地时区
猜测。
"""
from __future__ import annotations

from datetime import datetime, timezone

MICROSECOND_US = 1
SECOND_US = 1_000_000
MINUTE_US = 60 * SECOND_US


def parse_offset_time(value: object, *, location: str) -> int:
    """把带偏移的 ISO 8601 字符串解析为 UTC 微秒整数。

    只接受 ``str``；无时区信息（朴素时刻）、类型错误、无法解析都会抛
    :class:`ValueError`，消息里带 ``location`` 便于上层定位。
    """
    if not isinstance(value, str):
        raise ValueError(
            f"{location}: 时刻必须是带偏移的 ISO 8601 字符串，收到 {type(value).__name__}"
        )
    text = value.strip()
    if not text:
        raise ValueError(f"{location}: 时刻为空字符串")
    # datetime.fromisoformat 在 3.11 已支持 'Z'，但显式归一化更清楚。
    candidate = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{location}: 无法解析时刻 {value!r}（{exc}）") from None
    if dt.tzinfo is None:
        raise ValueError(
            f"{location}: 时刻 {value!r} 缺少 UTC 偏移（如 +08:00 / Z），"
            "不接受朴素时刻"
        )
    dt_utc = dt.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt_utc - epoch
    return delta.days * 86_400 * SECOND_US + delta.seconds * SECOND_US + delta.microseconds


def format_offset_time(epoch_us: int) -> str:
    """UTC 微秒整数 -> ``...Z`` 字符串，仅用于展示与落盘。"""
    total_seconds, micros = divmod(epoch_us, SECOND_US)
    dt = datetime.fromtimestamp(total_seconds, tz=timezone.utc).replace(microsecond=micros)
    return dt.isoformat().replace("+00:00", "Z")
