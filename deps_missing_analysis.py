#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
deps_missing_analysis.py

Analyse fehlender Abhängigkeiten aus deps.json + optionaler Auto-Fix
mit maximaler Sicherheit:
- Byte-exakte Ersetzung ausschließlich der Pfad-Literals in source()/sys.source()
- Kommentar-Schutz (Matches in Kommentaren werden ignoriert)
- Keine Änderung an Encoding oder Zeilenenden
- Optional: Verifikation, Rollback, Patch-Datei (Unified Diff)
- Optional: Interaktive Bestätigung pro Änderung mit Kontextvorschau

Beispiele
---------
Dry-Run (nur Analyse):
    python deps_missing_analysis.py --deps-json deps.json --root . --out-csv missing_analysis.csv

Auto-Fixes schreiben (nur eindeutige source()/sys.source()):
    python deps_missing_analysis.py --deps-json deps.json --root . --apply --verify

Interaktiv bestätigen + Patch-Datei erzeugen:
    python deps_missing_analysis.py --deps-json deps.json --root . --interactive --apply --verify --write-patch fixes.patch

Windows-Backslashes statt "/":
    python deps_missing_analysis.py --deps-json deps.json --root . --apply --slash-style windows
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import difflib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional


# ----------------------------- Datentypen -----------------------------

@dataclass
class MissingEdge:
    source: str        # Quell-Datei (relativ lt. deps.json)
    target: str        # Zielpfad (aus deps.json)
    edge_type: str     # z. B. "sources"
    basename: str      # Dateiname ohne Pfad
    src_abs: Path      # absoluter Pfad zur Quell-Datei
    unique_target_abs: Optional[Path]  # falls im Projekt genau ein Kandidat existiert
    reason: str        # Diagnose-Text


@dataclass
class PlannedReplacement:
    file: Path
    # Byte-Offsets im Original
    start: int
    end: int
    new_bytes: bytes
    original_bytes: bytes
    which: str  # Kontextinfo, z. B. source('...')


# ----------------------------- Utilities -----------------------------

def to_posix(s: str) -> str:
    return s.replace("\\", "/")


def norm_slashes(s: str, style: str) -> str:
    return s.replace("\\", "/") if style == "posix" else s.replace("/", "\\")


def is_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def read_bytes(p: Path) -> bytes:
    return p.read_bytes()


def write_bytes(p: Path, data: bytes) -> None:
    p.write_bytes(data)


def backup_file(p: Path) -> Path:
    bak = p.with_suffix(p.suffix + ".bak")
    if not bak.exists():
        bak.write_bytes(p.read_bytes())
    return bak


def rel_from_to(target: Path, start_dir: Path, slash_style: str) -> str:
    rel = os.path.relpath(target, start=start_dir)
    return norm_slashes(rel, slash_style)


def is_r_file(p: Path) -> bool:
    return p.suffix.lower() in {".r", ".rmd"}


# ---------------------- R-spezifische Erkennung -----------------------

# Byte-RegEx: source(...) oder sys.source(...), optional 'file=' und Stringliteral
SOURCE_CALL_RE_B = re.compile(
    rb"""(?P<func>\bsource|\bsys\.source)\s*\(\s*(?:file\s*=\s*)?(?P<quote>["'])(?P<path>[^"']+)(?P=quote)""",
    re.VERBOSE,
)


def line_start_index(data: bytes, pos: int) -> int:
    nl = data.rfind(b"\n", 0, pos)
    return 0 if nl == -1 else nl + 1


def is_commented_line_prefix(data: bytes, line_start: int, pos: int) -> bool:
    """
    Prüft im Bereich [line_start, pos), ob ein '#' außerhalb von String-Literalen vorkommt.
    Einfache String-Logik mit Quote-/Escape-Tracking (Byte-basiert).
    """
    in_q: Optional[int] = None  # 34 oder 39
    escaped = False
    i = line_start
    while i < pos:
        b = data[i]
        if in_q is not None:
            if not escaped and b == in_q:
                in_q = None
                i += 1
                continue
            escaped = (not escaped and b == 92)  # backslash
            i += 1
            continue
        else:
            if b == 35:  # '#'
                return True
            if b in (34, 39):  # " oder '
                in_q = b
            i += 1
    return False


# --------------------------- Kernlogik --------------------------------

def collect_missing_edges(deps_json: Path, root: Path) -> List[MissingEdge]:
    data = json.loads(deps_json.read_text(encoding="utf-8"))
    # Projektdateien indexieren (by basename)
    all_files: List[Path] = [p for p in root.rglob("*") if p.is_file()]
    by_name: Dict[str, List[Path]] = defaultdict(list)
    for p in all_files:
        by_name[p.name.lower()].append(p)

    missing: List[MissingEdge] = []
    for src_rel, edges in data.items():
        src_abs = (root / Path(src_rel)).resolve()
        for e in edges:
            if e.get("exists", True):
                continue
            tgt = e.get("target", "")
            edge_type = e.get("type", "")
            bn = Path(tgt).name.lower()

            candidates = by_name.get(bn, [])
            if len(candidates) == 1:
                reason = f"Datei existiert in anderem Ordner: {candidates[0].relative_to(root)}"
                unique = candidates[0]
            elif len(candidates) > 1:
                reason = f"Mehrdeutig: {len(candidates)} Treffer für {Path(tgt).name}"
                unique = None
            else:
                reason = "nicht gefunden"
                unique = None

            missing.append(MissingEdge(
                source=src_rel,
                target=tgt,
                edge_type=edge_type,
                basename=Path(tgt).name,
                src_abs=src_abs,
                unique_target_abs=unique,
                reason=reason
            ))
    return missing


def print_report(items: List[MissingEdge]) -> None:
    print(f"🔍 Analyse fehlender Abhängigkeiten ({len(items)} Einträge)")
    for it in items:
        print(f"- {to_posix(it.source)} → {to_posix(it.target)} ({it.edge_type}): {it.reason}")


def write_csv(items: List[MissingEdge], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "target", "type", "reason"])
        for it in items:
            w.writerow([to_posix(it.source), to_posix(it.target), it.edge_type, it.reason])
    print(f"💾 CSV geschrieben: {out_csv}")


# ----------------------- Auto-Fix (byte-genau) ------------------------

def plan_r_source_fixes_bytes(missings: List[MissingEdge], root: Path, slash_style: str) -> Tuple[List[PlannedReplacement], List[str]]:
    """
    Plane byte-genaue Ersetzungen ausschließlich für source()/sys.source()
    mit String-Literals in R/Rmd-Dateien – nur bei eindeutiger Ziel-Datei.
    """
    plans: List[PlannedReplacement] = []
    warnings: List[str] = []

    # Gruppiere nach Quell-Datei
    per_source: Dict[Path, List[MissingEdge]] = defaultdict(list)
    for it in missings:
        if it.edge_type != "sources":
            continue
        if not is_r_file(it.src_abs):
            continue
        if it.unique_target_abs is None:
            continue
        per_source[it.src_abs].append(it)

    for src_file, items in per_source.items():
        data = read_bytes(src_file)
        if not data:
            continue

        # Finde alle Kandidaten im File
        matches = list(SOURCE_CALL_RE_B.finditer(data))
        if not matches:
            continue

        # Pro MissingEdge: suche genau EIN Matching mit gleichem Basename & nicht in Kommentar
        used_spans: List[Tuple[int, int]] = []
        for it in items:
            want_bn = it.basename
            if not is_ascii(want_bn):  # Sicherheit: nur ASCII-Dateinamen automatisch ersetzen
                warnings.append(f"Nicht-ASCII-Dateiname, übersprungen: {src_file} → {want_bn}")
                continue

            found_m: Optional[re.Match] = None
            for m in matches:
                path_bytes = m.group("path")
                lit_bn = path_bytes.replace(b"\\", b"/").split(b"/")[-1].lower()
                if lit_bn == want_bn.encode("ascii").lower():
                    ls = line_start_index(data, m.start())
                    if is_commented_line_prefix(data, ls, m.start()):
                        continue
                    span = (m.start("path"), m.end("path"))
                    if any(not (span[1] <= s or span[0] >= e) for s, e in used_spans):
                        continue
                    found_m = m
                    break

            if not found_m:
                warnings.append(f"Kein eindeutiger source()-Treffer: {src_file} → {want_bn}")
                continue

            new_rel_str = rel_from_to(it.unique_target_abs, start_dir=src_file.parent, slash_style=slash_style)
            if not is_ascii(new_rel_str):
                warnings.append(f"Neuer Pfad nicht ASCII, übersprungen: {src_file} → {new_rel_str}")
                continue

            plans.append(PlannedReplacement(
                file=src_file,
                start=found_m.start("path"),
                end=found_m.end("path"),
                new_bytes=new_rel_str.encode("ascii"),
                original_bytes=found_m.group("path"),
                which=f"{found_m.group('func').decode('ascii','ignore')}('{found_m.group('path').decode('ascii','ignore')}')"
            ))
            used_spans.append((found_m.start("path"), found_m.end("path")))

    return plans, warnings


def make_one_change(original: bytes, plan: PlannedReplacement) -> bytes:
    return original[:plan.start] + plan.new_bytes + original[plan.end:]


def apply_plans_bytes(plans: List[PlannedReplacement], *, apply_files: bool, verify: bool, make_backup: bool, write_patch: Optional[Path]) -> Tuple[int, List[str]]:
    """
    Wendet geplante Byte-Ersetzungen dateiweise an.
    - apply_files=False: schreibt nur Patch (falls gewünscht), keine Dateien
    - verify=True: Unterschiede nur innerhalb der geplanten Bereiche erlaubt
    """
    changed_files = 0
    notes: List[str] = []

    # Nach Datei gruppieren
    per_file: Dict[Path, List[PlannedReplacement]] = defaultdict(list)
    for p in plans:
        per_file[p.file].append(p)

    for f, repls in per_file.items():
        original = read_bytes(f)
        if not original:
            continue

        # sortiere nach Start
        repls_sorted = sorted(repls, key=lambda r: r.start)

        # modifizierte Bytes erzeugen
        new_chunks: List[bytes] = []
        cursor = 0
        for r in repls_sorted:
            new_chunks.append(original[cursor:r.start])
            new_chunks.append(r.new_bytes)
            cursor = r.end
        new_chunks.append(original[cursor:])
        modified = b"".join(new_chunks)

        # Patch schreiben?
        if write_patch:
            o_txt = original.decode("latin-1").splitlines(keepends=True)
            n_txt = modified.decode("latin-1").splitlines(keepends=True)
            diff = difflib.unified_diff(o_txt, n_txt, fromfile=str(f), tofile=str(f), lineterm="")
            patch_text = "\n".join(diff) + "\n"
            if patch_text.strip():  # nur wenn es Änderungen gibt
                write_patch.parent.mkdir(parents=True, exist_ok=True)
                with write_patch.open("a", encoding="utf-8") as pf:
                    pf.write(patch_text)

        if not apply_files:
            # Nur Patch/Demo – keine Dateien schreiben
            continue

        # Verifikation (außerhalb der ersetzten Bereiche muss identisch bleiben)
        if verify:
            ok = True
            oi = 0
            ni = 0
            for r in repls_sorted:
                seg_len = r.start - oi
                if original[oi:oi+seg_len] != modified[ni:ni+seg_len]:
                    ok = False
                    break
                oi += seg_len
                ni += seg_len
                # ersetzter Bereich überspringen
                oi = r.end
                ni = ni + len(r.new_bytes)
            # Rest vergleichen
            if ok:
                if original[oi:] != modified[ni:ni + len(original) - oi]:
                    ok = False
            if not ok:
                notes.append(f"✖ Verifikation fehlgeschlagen, Datei unverändert: {f}")
                continue

        # Backup & Schreiben
        try:
            if make_backup:
                backup_file(f)
            write_bytes(f, modified)
            changed_files += 1
            for r in repls_sorted:
                notes.append(f"✔ {f}: {r.which}  →  '{r.new_bytes.decode('ascii', 'ignore')}'")
        except Exception as ex:
            notes.append(f"✖ Fehler beim Schreiben {f}: {ex}")

    return changed_files, notes


# ----------------------- Interaktive Vorschau -------------------------

def _context_window(data: bytes, start: int, end: int, radius_lines: int = 2) -> str:
    # Finde radius_lines Zeilen vorher/nachher
    a = start
    for _ in range(radius_lines):
        prev = data.rfind(b"\n", 0, a)
        if prev == -1:
            a = 0
            break
        a = prev if prev == 0 else prev
        if a == 0:
            break
    b = end
    for _ in range(radius_lines):
        nxt = data.find(b"\n", b)
        if nxt == -1:
            b = len(data)
            break
        b = nxt + 1
    snippet = data[a:b].decode("utf-8", errors="replace")
    return snippet


def interactive_filter(plans: List[PlannedReplacement]) -> List[PrannedReplacement]:
    # Typo fix (we will correct below)
    pass
# --- Fortsetzung: Interaktive Filterung (korrekte Implementierung) ---

def interactive_filter(plans: List[PlannedReplacement]) -> List[PlannedReplacement]:
    if not sys.stdin.isatty():
        print("ℹ️  Keine TTY erkannt – interaktiver Modus nicht möglich. Alle geplanten Änderungen werden vorgeschlagen.")
        return plans

    accepted: List[PlannedReplacement] = []
    total = len(plans)

    # Gruppiere nach Datei für bessere Übersicht
    by_file: Dict[Path, List[PlannedReplacement]] = defaultdict(list)
    for p in plans:
        by_file[p.file].append(p)

    idx = 0
    for f in sorted(by_file.keys()):
        data = read_bytes(f)
        file_plans = by_file[f]
        for p in file_plans:
            idx += 1
            print("\n" + "-" * 72)
            print(f"[{idx}/{total}] Datei: {f}")
            print(f"Fundstelle: {p.which}")
            try:
                old = p.original_bytes.decode("utf-8")
            except UnicodeDecodeError:
                old = p.original_bytes.decode("latin-1")
            try:
                new = p.new_bytes.decode("utf-8")
            except UnicodeDecodeError:
                new = p.new_bytes.decode("latin-1")
            print(f"Vorschlag: '{old}'  →  '{new}'")

            ctx = _context_window(data, p.start, p.end, radius_lines=2)
            print("\nKontext (±2 Zeilen):")
            print("-" * 72)
            print(ctx.rstrip("\n"))
            print("-" * 72)

            while True:
                choice = input("[Enter]=übernehmen, (s)kippen, (d)iff, (a)bbrechen: ").strip().lower()
                if choice == "":
                    accepted.append(p)
                    break
                elif choice in ("s", "skip"):
                    print("… übersprungen.")
                    break
                elif choice in ("a", "abort"):
                    print("Abgebrochen.")
                    return accepted
                elif choice in ("d", "diff"):
                    original = data
                    modified = make_one_change(original, p)
                    o_txt = original.decode("latin-1").splitlines(keepends=True)
                    n_txt = modified.decode("latin-1").splitlines(keepends=True)
                    diff = difflib.unified_diff(o_txt, n_txt, fromfile=str(f), tofile=str(f), lineterm="")
                    print("\n".join(diff) or "(kein Diff)")
                else:
                    print("Bitte Eingabe wiederholen.")
    return accepted


# ------------------------------- CLI ----------------------------------

def main():
    ap = argparse.ArgumentParser(description="Analyse & (optional) byte-genaue Auto-Fixes fehlender Abhängigkeiten (nur source()/sys.source())")
    ap.add_argument("--deps-json", required=True, help="Pfad zu deps.json (vom Analyzer erzeugt)")
    ap.add_argument("--root", default=".", help="Projektwurzel (Default: aktuelles Verzeichnis)")
    ap.add_argument("--out-csv", default=None, help="Optional: schreibe Analyse als CSV")

    # Fix-Optionen
    ap.add_argument("--apply", action="store_true", help="Tatsächlich Änderungen vornehmen (sonst Dry-Run)")
    ap.add_argument("--interactive", "-i", action="store_true", help="Interaktive Bestätigung pro Änderung")
    ap.add_argument("--slash-style", choices=["posix", "windows"], default="posix", help="Pfadschreibweise der neuen Referenzen")
    ap.add_argument("--no-backup", action="store_true", help="Kein .bak-Backup anlegen (nicht empfohlen)")
    ap.add_argument("--verify", action="store_true", help="Nach dem Schreiben verifizieren (und ggf. nicht schreiben)")
    ap.add_argument("--write-patch", default=None, help="Unified-Diff Patchdatei schreiben (append), z. B. fixes.patch")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    deps_path = Path(args.deps_json).resolve()
    if not deps_path.exists():
        raise SystemExit(f"❌ deps.json nicht gefunden: {deps_path}")

    # 1) Missing-Kanten einsammeln & Diagnose
    missing = collect_missing_edges(deps_path, root)
    print_report(missing)
    if args.out_csv:
        write_csv(missing, Path(args.out_csv))

    # 2) Auto-Fixes planen (nur Typ 'sources' in R/Rmd, eindeutige Ziele)
    plans, warns = plan_r_source_fixes_bytes(missing, root, slash_style=args.slash_style)

    if plans:
        print("\n🛠️  Geplante byte-genaue Fixes (nur source()/sys.source()):")
        for p in plans:
            print(f"- {p.file}: '{p.original_bytes.decode('ascii','ignore')}'  →  '{p.new_bytes.decode('ascii','ignore')}'")
    else:
        print("\nℹ️  Keine eindeutig automatisierbaren source()/sys.source()-Referenzen gefunden.")

    for w in warns:
        print(f"⚠️  {w}")

    # 2b) Interaktive Auswahl
    if args.interactive and plans:
        plans = interactive_filter(plans)
        if not plans:
            print("\nℹ️  Keine Änderungen ausgewählt.")
    elif args.interactive and not plans:
        print("ℹ️  Nichts zur Bestätigung vorhanden.")

    # 3) Anwenden / Patch / Dry-Run
    patch_path = Path(args.write_patch) if args.write_patch else None

    if args.apply and plans:
        changed, notes = apply_plans_bytes(
            plans,
            apply_files=True,
            verify=args.verify,
            make_backup=not args.no_backup,
            write_patch=patch_path,
        )
        print(f"\n✅ Fixes angewendet in {changed} Datei(en).")
        for n in notes:
            print(n)
    else:
        # Optional: Patch erzeugen, ohne Dateien zu ändern
        if patch_path and plans:
            _ = apply_plans_bytes(
                plans,
                apply_files=False,   # nur Patch
                verify=False,
                make_backup=False,
                write_patch=patch_path,
            )
            print(f"\n🧩 Patch geschrieben: {patch_path}")
        print("\n🔎 Dry-Run (keine Dateien verändert). Mit '--apply' anwenden.")


if __name__ == "__main__":
    main()

