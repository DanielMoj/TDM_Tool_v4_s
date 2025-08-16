#!/usr/bin/env python3
import re
import csv
import argparse
from pathlib import Path

SOURCE_CALL_RE = re.compile(
    r'(source|sys\.source)\s*\(\s*(?:file\s*=\s*)?["\']([^"\']+)["\']'
)

def find_bak_files(root: Path):
    return list(root.rglob("*.bak"))

def compare_file(bak: Path, new_file: Path):
    """
    Vergleicht eine .bak-Datei mit ihrer aktuellen Version zeilenweise
    und erfasst ausschließlich Änderungen in source()/sys.source()-Aufrufen.
    """
    changes = []
    old_text = bak.read_text(encoding="utf-8", errors="ignore").splitlines()
    new_text = new_file.read_text(encoding="utf-8", errors="ignore").splitlines()

    # Nur bis zur kürzeren Länge vergleichen (Pfadänderungen werden so erfasst;
    # neu hinzugefügte source()-Aufrufe ohne Vorgänger zählen hier nicht als "geändert")
    for i, (old_line, new_line) in enumerate(zip(old_text, new_text), start=1):
        if old_line == new_line:
            continue
        mo_old = SOURCE_CALL_RE.search(old_line)
        mo_new = SOURCE_CALL_RE.search(new_line)
        if mo_old and mo_new:
            old_path = mo_old.group(2)
            new_path = mo_new.group(2)
            if old_path != new_path:
                changes.append({
                    "datei": str(new_file),
                    "zeile": i,
                    "alter_pfad": old_path,
                    "neuer_pfad": new_path,
                })
    return changes

def write_csv(changes, out_csv: Path):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["datei", "zeile", "alter_pfad", "neuer_pfad"])
        w.writeheader()
        for c in changes:
            w.writerow(c)

def write_md(changes, out_md: Path, root: Path):
    out_md.parent.mkdir(parents=True, exist_ok=True)
    with out_md.open("w", encoding="utf-8") as f:
        f.write("# Geänderte Pfade in `source()` / `sys.source()`\n\n")
        f.write(f"Wurzelverzeichnis: `{root}`\n\n")
        f.write(f"Anzahl Änderungen: **{len(changes)}**\n\n")
        f.write("| Datei | Zeile | Alter Pfad | Neuer Pfad |\n")
        f.write("|---|---:|---|---|\n")
        for c in changes:
            rel = str(Path(c["datei"]).resolve().relative_to(root))
            f.write(f"| {rel} | {c['zeile']} | `{c['alter_pfad']}` | `{c['neuer_pfad']}` |\n")

def main():
    ap = argparse.ArgumentParser(
        description="Vergleicht .bak-Dateien mit den neuen Dateien und listet Pfadänderungen in source()/sys.source() auf."
    )
    ap.add_argument("--root", default=".", help="Projektwurzel (Standard: .)")
    ap.add_argument(
        "--out-dir",
        default="analysis_reports",
        help="Zielordner für Ausgabedateien (Standard: analysis_reports). Beispiel: \"analysis report\"",
    )
    ap.add_argument(
        "--also-root",
        action="store_true",
        help="Zusätzlich auch im Projektwurzel-Verzeichnis CSV/MD ablegen.",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_dir = (root / args.out_dir).resolve()

    bak_files = find_bak_files(root)
    if not bak_files:
        print("❌ Keine .bak-Dateien gefunden.")
        return

    all_changes = []
    for bak in bak_files:
        new_file = bak.with_suffix("")  # gleiche Datei ohne .bak
        if not new_file.exists():
            print(f"⚠️  Keine neue Datei gefunden für {bak}")
            continue
        all_changes.extend(compare_file(bak, new_file))

    if not all_changes:
        print("ℹ️  Keine Pfadänderungen in source()/sys.source() gefunden.")
    else:
        print(f"✅ {len(all_changes)} Pfadänderungen gefunden:\n")
        # Konsole: Kurzliste
        for c in all_changes:
            rel = str(Path(c["datei"]).resolve().relative_to(root))
            print(f"- {rel} (Zeile {c['zeile']}): '{c['alter_pfad']}' → '{c['neuer_pfad']}'")

    # Dateien schreiben (immer in out_dir)
    out_csv = out_dir / "pfad_aenderungen.csv"
    out_md = out_dir / "pfad_aenderungen.md"
    write_csv(all_changes, out_csv)
    write_md(all_changes, out_md, root)

    print(f"\n💾 CSV geschrieben: {out_csv}")
    print(f"💾 Markdown geschrieben: {out_md}")

    # Optional zusätzlich in der Projektwurzel ablegen
    if args.also_root:
        root_csv = root / "pfad_aenderungen.csv"
        root_md = root / "pfad_aenderungen.md"
        write_csv(all_changes, root_csv)
        write_md(all_changes, root_md, root)
        print(f"📎 (Zusätzlich) CSV im Root: {root_csv}")
        print(f"📎 (Zusätzlich) MD im Root:  {root_md}")

if __name__ == "__main__":
    main()
