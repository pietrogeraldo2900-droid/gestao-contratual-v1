from __future__ import annotations

import json
from typing import Any

from app.database.connection import DatabaseManager


class AdminAuditRepository:
    def __init__(self, db: DatabaseManager):
        self._db = db

    def log_action(
        self,
        actor_user_id: int,
        target_user_id: int | None,
        action: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        sql = """
        INSERT INTO admin_audit_log (actor_user_id, target_user_id, action, metadata_json)
        VALUES (%s, %s, %s, %s::jsonb)
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(actor_user_id), target_user_id, action, payload))
            conn.commit()

