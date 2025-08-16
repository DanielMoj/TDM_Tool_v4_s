#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# deps_postprocess.py
# Liest eine deps.json (Projekt-Abhängigkeiten) und erzeugt Folgeanalysen:
# - Normalisierte & deduplizierte JSON
# - Liste fehlender Abhängigkeiten (CSV + Markdown)
# - Statistiken pro Typ und pro Quelle (CSV)
# - DOT/PNG für Graph der fehlenden Kanten
# - Zusammenfassendes Markdown-Report

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from shutil import which
import subprocess


def normalize_path(s: str, mode: str) -> str:
    if mode == "posix":
        return s.replace("\\", "/")
    elif mode == "windows":
        return s.replace("/", "\\")
    return s


def main():
    ap = argparse.ArgumentParser(description="Folgeanalyse für deps.json")
    ap.add_argument("--in-json", required=True, help="Pfad zur deps.json")
    ap.add_argument("--out-dir", default="analysis_reports", help="Zielordner für Reports")
    ap.add_argument(
        "--normalize-paths",
        choices=["posix", "windows", "none"],
        default="posix",
        help="Pfade normalisieren",
    )
    ap.add_argument("--dedupe", action="store_true", help="Kanten deduplizieren")
    ap.add_argument("--png", action="store_true", help="PNG für Missing-Graph erzeugen (Graphviz erforderlich)")
    args = ap.parse_args()

    in_path = Path(args.in_json)
    if not in_path.exists():
        raise SystemExit(f"deps.json nicht gefunden: {in_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = json.loads(in_path.read_text(encoding="utf-8"))

    # Normalisierung & optionales Deduplizieren
    dedup_edges = set()
    normalized: dict[str, list[dict]] = defaultdict(list)
    missing_edges: list[tuple[str, str, str]] = []  # (src, tgt, typ)
    type_counter_total: Counter[str] = Counter()
    type_counter_missing: Counter[str] = Counter()
    src_total: Counter[str] = Counter()
    src_missing: Counter[str] = Counter()

    for src, edges in data.items():
        nsrc = normalize_path(src, args.normalize_paths)
        for e in edges:
            tgt = e.get("target", "")
            typ = e.get("type", "")
            exi = bool(e.get("exists", True))
            ntgt = normalize_path(tgt, args.normalize_paths)

            if args.dedupe:
                key = (nsrc, ntgt, typ, exi)
                if key in dedup_edges:
                    continue
                dedup_edges.add(key)

            normalized[nsrc].append({"target": ntgt, "type": typ, "exists": exi})
            # Statistiken
            type_counter_total[typ] += 1
            src_total[nsrc] += 1
            if not exi:
                missing_edges.append((nsrc, ntgt, typ))
                type_counter_missing[typ] += 1
                src_missing[nsrc] += 1

    # Dateien schreiben
    norm_json_path = out_dir / "deps_normalized.json"
    norm_json_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")

    # Fehlende Abhängigkeiten (CSV + MD)
    missing_csv = out_dir / "missing_dependencies.csv"
    with missing_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "target", "type"])
        for s, t, ty in sorted(missing_edges):
            w.writerow([s, t, ty])

    missing_md = out_dir / "MISSING_DEPENDENCIES.md"
    lines = []
    lines.append("# Fehlende Abhängigkeiten")
    lines.append("")
    lines.append(f"Anzahl fehlend: **{len(missing_edges)}**")
    lines.append("")
    lines.append("| Quelle | Ziel | Typ |")
    lines.append("|---|---|---|")
    for s, t, ty in sorted(missing_edges):
        lines.append(f"| `{s}` | `{t}` | `{ty}` |")
    missing_md.write_text("\n".join(lines), encoding="utf-8")

    # Statistiken pro Typ
    stats_type_csv = out_dir / "stats_by_type.csv"
    with stats_type_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["type", "total", "missing", "missing_pct"])
        for ty in sorted(type_counter_total.keys() | type_counter_missing.keys()):
            tot = type_counter_total.get(ty, 0)
            mis = type_counter_missing.get(ty, 0)
            pct = (mis / tot * 100.0) if tot else 0.0
            w.writerow([ty, tot, mis, f"{pct:.1f}"])

    # Statistiken pro Quelle
    stats_src_csv = out_dir / "stats_by_source.csv"
    with stats_src_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "total_edges", "missing_edges", "missing_pct"])
        for s in sorted(src_total.keys() | src_missing.keys()):
            tot = src_total.get(s, 0)
            mis = src_missing.get(s, 0)
            pct = (mis / tot * 100.0) if tot else 0.0
            w.writerow([s, tot, mis, f"{pct:.1f}"])

    # Missing-only Graph (DOT + PNG optional)
    dot_path = out_dir / "missing_graph.dot"
    lines = []
    lines.append('digraph "missing_deps" {')
    lines.append("  rankdir=LR;")
    lines.append('  node [shape=box, style="rounded,filled", fillcolor="#fff7f7", fontname="Helvetica"];')
    lines.append('  edge [color="red", style="dashed", fontname="Helvetica"];')

    nodes = set()
    for s, t, ty in missing_edges:
        nodes.add(s)
        nodes.add(t)

    def dq(s: str) -> str:
        return json.dumps(s)

    for n in sorted(nodes):
        label = Path(n).name or n
        lines.append(f"  {dq(n)} [label={dq(label)}, tooltip={dq(n)}];")

    for s, t, ty in missing_edges:
        lines.append(f"  {dq(s)} -> {dq(t)} [label={dq(ty)}];")

    lines.append("}")
    dot_path.write_text("\n".join(lines), encoding="utf-8")

    png_path = out_dir / "missing_graph.png"
    if args.png and which("dot"):
        try:
            subprocess.run(["dot", "-Tpng", str(dot_path), "-o", str(png_path)], check=True)
        except subprocess.CalledProcessError:
            pass

    # Zusammenfassender Report
    report_md = out_dir / "DEPS_FOLGEANALYSE.md"
    total_edges = sum(type_counter_total.values())
    total_missing = sum(type_counter_missing.values())
    total_sources = len(normalized.keys())
    top_sources = sorted(src_missing.items(), key=lambda kv: kv[1], reverse=True)[:15]
    top_types = sorted(type_counter_missing.items(), key=lambda kv: kv[1], reverse=True)[:10]

    rep = []
    rep.append("# Folgeanalyse der Abhängigkeiten")
    rep.append("")
    rep.append(f"- **Gesamt-Knoten (Quellen)**: {total_sources}")
    rep.append(f"- **Gesamt-Kanten**: {total_edges}")
    rep.append(f"- **Fehlende Kanten**: {total_missing} ({(total_missing/total_edges*100.0 if total_edges else 0):.1f}%)")
    rep.append("")
    rep.append("## Top-Quellen mit fehlenden Abhängigkeiten")
    rep.append("")
    rep.append("| Quelle | Fehlend |")
    rep.append("|---|---:|")
    for s, c in top_sources:
        rep.append(f"| `{s}` | {c} |")
    rep.append("")
    rep.append("## Fehlende pro Typ")
    rep.append("")
    rep.append("| Typ | Fehlend |")
    rep.append("|---|---:|")
    for ty, c in top_types:
        rep.append(f"| {ty} | {c} |")
    rep.append("")
    rep.append("## Artefakte")
    rep.append("")
    rep.append(f"- Normalisierte JSON: `{norm_json_path}`")
    rep.append(f"- Fehlende (CSV): `{missing_csv}`")
    rep.append(f"- Fehlende (Markdown): `{missing_md}`")
    rep.append(f"- Stats (Typ): `{stats_type_csv}`")
    rep.append(f"- Stats (Quelle): `{stats_src_csv}`")
    rep.append(f"- Missing-Graph (DOT): `{dot_path}`")
    if args.png and (png_path.exists()):
        rep.append(f"- Missing-Graph (PNG): `{png_path}`")
    report_md.write_text("\n".join(rep), encoding="utf-8")

    print(f"📦 Folgeanalyse geschrieben nach: {out_dir.resolve()}")
    print(f"  - deps_normalized.json")
    print(f"  - MISSING_DEPENDENCIES.md / .csv")
    print(f"  - stats_by_type.csv, stats_by_source.csv")
    print(f"  - missing_graph.dot{' / .png' if args.png else ''}")
    print(f"  - DEPS_FOLGEANALYSE.md")


if __name__ == "__main__":
    main()
