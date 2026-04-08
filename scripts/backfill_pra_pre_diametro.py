from __future__ import annotations

import re
from pathlib import Path

from app.database.connection import build_database_manager
from config.settings import load_settings


def _extract_diametro_mm(*texts: object) -> int | None:
    probe = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if not probe:
        return None
    for pattern in (
        r"(?:\u00D8|\u00F8|Ã˜|Ã¸)\s*(\d{1,3})\b",
        r"\bdn\s*(\d{1,3})\b",
        r"\bdiam(?:etro)?\.?\s*(\d{1,3})\b",
        r"\b(?:pead|pvc)\s*(\d{1,3})\b",
        r"\b(?:pra|pre)\s*(?:[-_/]|(?:\u00D8|\u00F8|Ã˜|Ã¸)|\?)?\s*(\d{1,3})\b",
    ):
        match = re.search(pattern, probe, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            diametro = int(str(match.group(1) or "").strip())
        except Exception:
            continue
        if 10 <= diametro <= 400:
            return diametro
    return None


def main() -> None:
    settings = load_settings(Path(".").resolve())
    db = build_database_manager(settings)
    if db is None:
        raise RuntimeError("Banco indisponivel. Defina DB_ENABLED=1 e DATABASE_URL.")

    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE management_execucao ADD COLUMN IF NOT EXISTS diametro_mm INTEGER")
            cur.execute(
                """
                SELECT id, servico_oficial, servico_bruto, item_original, diametro_mm
                FROM management_execucao
                WHERE UPPER(COALESCE(NULLIF(servico_oficial, ''), '')) IN ('PRA', 'PRE')
                """
            )
            rows = cur.fetchall() or []

            updates: list[tuple[int | None, str, int]] = []
            for row in rows:
                row_id = int(row[0])
                servico_oficial = str(row[1] or "").strip().upper()
                servico_bruto = str(row[2] or "").strip()
                item_original = str(row[3] or "").strip()
                diametro_atual = row[4]
                try:
                    diametro = int(diametro_atual) if diametro_atual is not None else None
                except Exception:
                    diametro = None

                if diametro is None:
                    diametro = _extract_diametro_mm(servico_bruto, item_original)

                if diametro is None:
                    continue

                servico_bruto_novo = f"{servico_oficial} Ø{diametro}"
                if servico_bruto_novo != servico_bruto or diametro_atual != diametro:
                    updates.append((diametro, servico_bruto_novo, row_id))

            if updates:
                cur.executemany(
                    """
                    UPDATE management_execucao
                    SET diametro_mm = %s,
                        servico_bruto = %s
                    WHERE id = %s
                    """,
                    updates,
                )
            conn.commit()

    print(f"Registros PRA/PRE avaliados: {len(rows)}")
    print(f"Registros atualizados: {len(updates)}")


if __name__ == "__main__":
    main()
