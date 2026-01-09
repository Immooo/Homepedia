from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class PricePoint:
    metric_uid: str
    metric_name: str
    geo: str
    unit: str
    period: str
    value: float
    source: str
    source_url: str
    scraped_at: datetime


def _to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def _normalize_period(s: str) -> Optional[str]:
    """
    Accept common quarter formats:
      - 2025-T3, 2025 T3, 2025T3, T3 2025
    Return: 2025-T3
    """
    if not s:
        return None
    s = s.strip().upper()
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)

    # 2025-T3 / 2025 T3 / 2025T3
    m = re.search(r"(\d{4})\s*[- ]?\s*T\s*([1-4])", s)
    if m:
        return f"{m.group(1)}-T{m.group(2)}"

    # T3 2025
    m = re.search(r"T\s*([1-4])\s*(\d{4})", s)
    if m:
        return f"{m.group(2)}-T{m.group(1)}"

    return None


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "col"


def _table_full_text(table) -> str:
    return table.get_text(" ", strip=True).replace("\xa0", " ")


def _score_table(table, kind: str) -> int:
    """
    Heuristic scoring to pick the best relevant table.
    kind: "indices" or "yoy"
    """
    txt = _table_full_text(table).lower()

    score = 0
    if "logements anciens" in txt:
        score += 3

    if kind == "indices":
        if "indice" in txt or "indices" in txt:
            score += 3
        if "base" in txt and "100" in txt:
            score += 1
        if "ile-de-france" in txt or "île-de-france" in txt:
            score += 2
        if "province" in txt:
            score += 2
        if "appartement" in txt:
            score += 1
        if "maison" in txt:
            score += 1

    if kind == "yoy":
        if "variation" in txt:
            score += 3
        if "sur un an" in txt or "annuelle" in txt:
            score += 2
        if "%" in txt or "pourcentage" in txt:
            score += 1
        if "france" in txt:
            score += 2
        if "appartement" in txt:
            score += 1
        if "maison" in txt:
            score += 1

    # must contain at least one quarter-like token somewhere
    if re.search(r"\d{4}\s*[- ]?\s*T\s*[1-4]", txt, flags=re.IGNORECASE):
        score += 2

    return score


def _extract_headers(table) -> list[str]:
    """
    Try to extract a usable header list aligned with data columns.
    - INSEE tables may have multi-row headers; we take the LAST header row with enough cells.
    """
    header_rows = table.find_all("tr")
    candidates: list[list[str]] = []
    for tr in header_rows:
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        # header row usually has th and no td
        if ths and not tds:
            labels = [th.get_text(" ", strip=True).replace("\xa0", " ") for th in ths]
            candidates.append(labels)

    # Prefer the last candidate with >= 2 columns
    for labels in reversed(candidates):
        if len(labels) >= 2:
            return labels

    # fallback: any th in table
    return [th.get_text(" ", strip=True).replace("\xa0", " ") for th in table.find_all("th")]


def _find_latest_data_row(table) -> tuple[Optional[str], list[str]]:
    """
    Return (period, row_cells_texts) for the first row that looks like a quarter.
    Handles cases where the period is in <th scope="row">.
    """
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        texts = [c.get_text(" ", strip=True).replace("\xa0", " ") for c in cells]
        if not texts:
            continue

        period = _normalize_period(texts[0])
        if period and len(texts) >= 2:
            return period, texts

    return None, []


def _infer_geo(label: str) -> str:
    l = label.lower()
    if "ile-de-france" in l or "île-de-france" in l or "idf" in l:
        return "IDF"
    if "province" in l:
        return "PROVINCE"
    if "france" in l:
        return "FR"
    return "NA"


def scrape_insee_housing_prices(source_url: str, timeout_seconds: int = 20) -> tuple[list[PricePoint], dict]:
    """
    Scrape housing price figures from INSEE page:
    - pick best "indices" table
    - pick best "yoy variation" table
    Parse latest quarter row and map columns to metrics.
    """
    now = datetime.now(timezone.utc)
    headers = {
        "User-Agent": "Mozilla/5.0 (Homepedia school project; +https://example.local)",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
    }

    resp = requests.get(source_url, headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table")
    raw_debug: dict[str, Any] = {
        "source_url": source_url,
        "scraped_at": now.isoformat(),
        "tables_found": len(tables),
    }

    if not tables:
        raw_debug["error"] = "no_tables_found_in_html"
        return [], raw_debug

    # pick best tables
    scored_indices = sorted((( _score_table(t, "indices"), i, t) for i, t in enumerate(tables)), reverse=True, key=lambda x: x[0])
    scored_yoy = sorted((( _score_table(t, "yoy"), i, t) for i, t in enumerate(tables)), reverse=True, key=lambda x: x[0])

    best_indices_score, best_indices_i, best_indices = scored_indices[0]
    best_yoy_score, best_yoy_i, best_yoy = scored_yoy[0]

    raw_debug["best_indices"] = {"score": best_indices_score, "table_index": best_indices_i}
    raw_debug["best_yoy"] = {"score": best_yoy_score, "table_index": best_yoy_i}

    # thresholds: if score too low, skip
    points: list[PricePoint] = []

    def parse_table(table, kind: str) -> None:
        period, row = _find_latest_data_row(table)
        if not period or not row:
            return

        hdrs = _extract_headers(table)

        # Align headers to row values:
        # row = [period, v1, v2, ...]
        # hdrs might be longer/shorter; we take last N headers for values if possible.
        values = row[1:]
        if len(hdrs) >= len(values) + 1:
            # typical: hdrs includes the first label for period column
            col_labels = hdrs[-len(values):]
        else:
            # fallback: generate labels col_1, col_2...
            col_labels = [f"col_{i+1}" for i in range(len(values))]

        unit = "pct" if kind == "yoy" else "index_base"
        for label, val_txt in zip(col_labels, values):
            v = _to_float(val_txt)
            if v is None:
                continue

            geo = _infer_geo(label)
            metric_uid = f"insee_{kind}:{_slugify(label)}"
            metric_name = f"INSEE {kind} — {label}"

            points.append(
                PricePoint(
                    metric_uid=metric_uid,
                    metric_name=metric_name,
                    geo=geo,
                    unit=unit,
                    period=period,
                    value=v,
                    source="insee",
                    source_url=source_url,
                    scraped_at=now,
                )
            )

    if best_indices_score >= 4:
        parse_table(best_indices, "indices")
    if best_yoy_score >= 4:
        parse_table(best_yoy, "yoy")

    raw_debug["points_count"] = len(points)

    # Add a tiny sample of headers for debugging
    try:
        raw_debug["indices_headers_sample"] = _extract_headers(best_indices)[:12]
        raw_debug["yoy_headers_sample"] = _extract_headers(best_yoy)[:12]
    except Exception:
        pass

    return points, raw_debug
