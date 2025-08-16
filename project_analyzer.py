# -*- coding: utf-8 -*-
# project_analyzer.py
# Führt eine umfassende Abhängigkeitsanalyse für ein multidisziplinäres Projekt durch.
# - Für R/Rmd: nutzt ein R-Hilfsskript (parser.R), das eine typisierte JSON-AST schreibt.
# - Für andere Dateitypen: nutzt gezielte Pattern-/RegEx-Erkennung.
# - Schreibt eine MASTER-DATEI-REFERENZ.md mit Übersicht & Statistik.
# - Optional: erzeugt Graphviz-DOT und PNG für den Abhängigkeitsgraphen.
# - Optional: ruft deps_postprocess.py auf, um Folgeanalysen zu erzeugen.
#
# CLI (Beispiel):
#   python project_analyzer.py --root . --rscript parser.R \
#       --out-json deps.json --include-missing \
#       --out-master MASTER-DATEI-REFERENZ.md \
#       --out-graph deps_graph.dot --out-graph-png deps_graph.png \
#       --postprocess --postprocess-outdir analysis_reports --pp-normalize-paths posix --pp-dedupe --pp-png \
#       --exclude .git --exclude node_modules
#
# Ausgabe:
#   - Konsolenbericht
#   - Optional JSON mit allen Kanten (Quelle → Ziel inkl. Typ, exists)
#   - MASTER-DATEI-REFERENZ.md (Tabelle + Statistik)
#   - Optional Graphviz-DOT (.dot) und PNG (.png)
#   - Optional Folgeanalyse-Artefakte (siehe deps_postprocess.py)

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shlex
import subprocess
import sys
from collections import defaultdict, Counter
from pathlib import Path
from shutil import which
from typing import Dict, List, Optional, Union, Any

AST_OUTPUT_DIR = "ast_json_output"


# ---------- Utilities ----------

def posix(path: Union[str, Path]) -> str:
    # Korrigierte Normalisierung: einzelne Backslashes → Slashes
    return str(path).replace("\\", "/")


def read_text_safe(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def resolve_path(source_file: Path, target: str) -> Path:
    """
    Löst einen (möglicherweise relativen) Zielpfad gegen die Quelldatei auf.
    """
    t = Path(target)
    if t.is_absolute():
        return t.resolve()
    # relative zum Ordner der Quelldatei
    return (source_file.parent / t).resolve()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def bytes_to_human(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for u in units:
        if size < 1024.0 or u == units[-1]:
            return f"{size:.2f} {u}".replace(".00", "")
        size /= 1024.0


def file_type_for(path: Path) -> str:
    name = path.name
    ext = path.suffix.lower().lstrip(".")
    if ext:
        return ext
    # Sonderfälle ohne Suffix
    low = name.lower()
    if low == "dockerfile":
        return "dockerfile"
    if low == "makefile":
        return "makefile"
    return "none"


# ---------- R AST Generation ----------

def ast_json_path_for(root: Path, file_path: Path) -> Path:
    # Spiegel den Ordnerbaum, hänge ".<ext>.json" an
    rel = file_path.resolve().relative_to(root.resolve())
    return Path(AST_OUTPUT_DIR) / rel.with_suffix(file_path.suffix + ".json")


def generate_r_asts(root: Path, r_files: List[Path], rscript_parser_path: Path, rscript_bin: str = "Rscript", timeout_sec: int = 60) -> None:
    ensure_dir(Path(AST_OUTPUT_DIR))
    # Prüfe, ob rscript_bin gefunden wird (bei vollem Pfad einfach existiert die Datei?)
    if os.path.sep in rscript_bin:
        rscript_ok = Path(rscript_bin).exists()
    else:
        rscript_ok = which(rscript_bin) is not None
    if not rscript_ok:
        print("⚠️  Hinweis: 'Rscript' wurde nicht gefunden. Überspringe R-AST-Generierung.")
        return

    for r_file in r_files:
        out_json = ast_json_path_for(root, r_file)
        ensure_dir(out_json.parent)
        try:
            res = subprocess.run(
                [rscript_bin, str(rscript_parser_path), str(r_file), str(out_json)],
                capture_output=True, text=True, timeout=timeout_sec
            )
            if res.returncode != 0:
                # Nur Kopf des Fehlers zeigen, um Lärm zu reduzieren
                err = (res.stderr or "").strip()
                print(f"⚠️  Rscript-Fehler für {posix(r_file)}:{'' if not err else ' ' + err.splitlines()[0]}")
        except subprocess.TimeoutExpired:
            print(f"⚠️  Timeout ({timeout_sec}s) bei der AST-Generierung für {posix(r_file)}")


# ---------- Analyzers ----------

def analyze_r_file(root: Path, file_path: Path, content: str) -> List[Dict[str, str]]:
    """
    Nutzt den JSON-AST (falls vorhanden) und ergänzende Regex-Fallbacks,
    um source()-Bezüge und typische Datei-Reads zu finden.
    """
    dependencies: List[Dict[str, str]] = []

    json_ast = ast_json_path_for(root, file_path)
    if json_ast.exists():
        try:
            ast_data: Any = json.loads(read_text_safe(json_ast))
        except json.JSONDecodeError:
            ast_data = []

        # Unterstütze zwei Formate:
        # 1) Typisierte Knoten (unser neuer parser.R):
        #    dict(type="call"/"literal"/"name", fun=?, args=?, value=?)
        # 2) Alte Listenform: ["source", "file.R", ...]
        def extract_string(node: Any) -> Optional[str]:
            # typisiertes Format – Stringliteral
            if isinstance(node, dict) and node.get("type") == "literal":
                v = node.get("value")
                if isinstance(v, str):
                    return v
                # atomare Vektoren -> nimm erste Komponente
                if isinstance(v, list) and v and isinstance(v[0], str):
                    return v[0]
                return None

            # Aufruf von here::here(...) / file.path(...)
            if isinstance(node, dict) and node.get("type") == "call" and node.get("fun") in ("here::here", "file.path", "here"):
                args = node.get("args", [])
                if isinstance(args, dict):
                    args = list(args.values())
                parts: List[str] = []
                for a in args:
                    s = extract_string(a)
                    if s:
                        parts.append(s)
                if parts:
                    return os.path.join(*parts)

            # Namenssymbol → nicht eindeutig
            return None

        def handle_source_call(args_node: Union[List[Any], Dict[str, Any]]):
            # args kann dict (benannte Argumente) oder Liste (positionale) sein
            candidates: List[Any] = []
            if isinstance(args_node, dict):
                # bevorzugt benanntes Argument 'file'
                if "file" in args_node:
                    candidates.append(args_node["file"])
                # auch positionslose Werte
                for k, v in args_node.items():
                    if not k:
                        candidates.append(v)
            elif isinstance(args_node, list):
                candidates = args_node

            for c in candidates:
                p = extract_string(c)
                if p:
                    dependencies.append({"target": p, "type": "sources"})

        def walk(node: Any):
            if isinstance(node, dict) and node.get("type") == "call":
                fun = node.get("fun")
                if fun == "source":
                    handle_source_call(node.get("args", {}))
                # ggf. weitere Funktionsaufrufe untersuchen
                args = node.get("args", {})
                if isinstance(args, dict):
                    for v in args.values():
                        walk(v)
                elif isinstance(args, list):
                    for v in args:
                        walk(v)
            elif isinstance(node, list):
                # altes einfaches Format
                if node and isinstance(node[0], str) and node[0] == "source":
                    if len(node) > 1 and isinstance(node[1], str):
                        dependencies.append({"target": node[1], "type": "sources"})
                for item in node:
                    walk(item)
            # Literale/Names werden ignoriert

        walk(ast_data)

    # Regex-Fallbacks (breiter gefasst)
    patterns = {
        "reads_data": r'(?:read|load)[._]\w*\s*\(\s*(?:here::here\(|file\.path\()?\s*["\']([^"\']+\.(?:csv|tsv|txt|json|rds))["\']',
        "loads_config": r'(?:read_yaml|yaml::read_yaml|fromJSON)\s*\(\s*["\']([^"\']+\.(?:ya?ml|json))["\']',
        "loads_model": r'(?:stan_model|rstan::stan_model|jags\.model)\s*\(\s*[^)]*file\s*=\s*["\']([^"\']+\.(?:stan|jags))["\']',
        "runs_sql": r'(?:read_file|readLines)\s*\(\s*["\']([^"\']+\.sql)["\']'
    }
    for dep_type, pattern in patterns.items():
        for m in re.finditer(pattern, content):
            dependencies.append({"target": m.group(1), "type": dep_type})

    return dependencies


def analyze_markdown_file(content: str) -> List[Dict[str, str]]:
    deps: List[Dict[str, str]] = []
    for m in re.finditer(r'\[.*?\]\((?!https?://|mailto:|ftp://)([^)\s#]+)', content):
        target = m.group(1)
        if target:
            deps.append({"target": target, "type": "links_to"})
    return deps


def analyze_yaml_file(content: str) -> List[Dict[str, str]]:
    # Versuche echtes YAML-Parsing; bei Fehlern fallback auf RegEx
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(content)
    except Exception:
        data = None

    deps: List[Dict[str, str]] = []

    def walk(x: Any):
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str):
            if re.search(r'\.(ya?ml|json|csv|tsv|txt|ini|conf|dockerfile|sql|stan|jags|r|rmd|sh|py)$', x, re.I):
                deps.append({"target": x, "type": "references"})

    if data is not None:
        walk(data)
        return deps

    # Fallback: einfache Schlüssel erkennen
    for m in re.finditer(r'^\s*(?:extends|file|build|dockerfile|config_file|path):\s*["\']?([^"\']+)["\']?', content, re.M):
        deps.append({"target": m.group(1), "type": "references"})
    return deps


def analyze_shell_script(content: str) -> List[Dict[str, str]]:
    deps: List[Dict[str, str]] = []
    patterns = [
        (r'^(?:source|\.\s+)\s+["\']?([^"\']+)["\']?', "executes"),
        (r'^\s*(?:bash|sh)\s+["\']?([^"\']+\.sh)["\']?', "executes"),
        (r'^\s*python(?:3)?\s+["\']?([^"\']+\.py)["\']?', "executes"),
        (r'^\s*Rscript\s+["\']?([^"\']+\.R)["\']?', "executes"),
    ]
    for pat, typ in patterns:
        for m in re.finditer(pat, content, re.M):
            deps.append({"target": m.group(1), "type": typ})
    return deps


def analyze_dockerfile(content: str) -> List[Dict[str, str]]:
    deps: List[Dict[str, str]] = []
    for line in content.splitlines():
        if re.match(r'^\s*FROM\s+', line):
            # Ganze FROM-Zeile (Image) als "inherits_from"
            parts = shlex.split(line, posix=True)
            # find token after FROM
            try:
                idx = next(i for i, t in enumerate(parts) if t.upper() == "FROM")
                if idx + 1 < len(parts):
                    deps.append({"target": parts[idx + 1], "type": "inherits_from"})
            except StopIteration:
                pass
        elif re.match(r'^\s*COPY\s+', line):
            tokens = shlex.split(line, posix=True)
            # DROP options like --chown=...
            toks = [t for t in tokens[1:] if not t.startswith("--")]
            if len(toks) >= 2:
                # all but last are sources
                for src in toks[:-1]:
                    deps.append({"target": src, "type": "copies"})
        elif re.match(r'^\s*include\s+', line, re.I):
            tokens = shlex.split(line, posix=True)
            if len(tokens) >= 2:
                deps.append({"target": tokens[1], "type": "includes"})
    return deps


def analyze_generic_config(content: str) -> List[Dict[str, str]]:
    # Heuristik für INI/CONF/etc.: greife einfache relative Pfade ab
    deps: List[Dict[str, str]] = []
    for m in re.finditer(r'(?<!https?://|mailto:|ftp://)(?:\./|\.\./|/)?[\w\-/\.]+\.(?:ya?ml|json|csv|tsv|txt|ini|conf|sql|stan|jags|r|rmd|sh|py)', content, re.I):
        deps.append({"target": m.group(0), "type": "references"})
    return deps


# ---------- Reporting: MASTER-DATEI-REFERENZ ----------

def write_master_reference(root: Path, files: List[Path], out_md_path: Path) -> None:
    # Sammle Datensätze
    records = []
    type_counter = Counter()
    total_size = 0

    for idx, p in enumerate(sorted(files, key=lambda x: posix(x.resolve().relative_to(root.resolve()))), start=1):
        rel = p.resolve().relative_to(root.resolve())
        ftype = file_type_for(p)
        size = p.stat().st_size if p.exists() else 0
        total_size += size
        type_counter[ftype] += 1
        records.append({
            "nr": idx,
            "name": p.name,
            "path": posix(rel),
            "type": ftype,
            "size": size,
        })

    # Markdown zusammenbauen
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append("# MASTER-DATEI-REFERENZ")
    lines.append("")
    lines.append(f"_Erzeugt am: {ts}_")
    lines.append("")
    lines.append("## Gesamtübersicht")
    lines.append("")
    lines.append("| Nr | Dateiname | Pfad | Typ | Größe |")
    lines.append("|---:|---|---|---|---:|")
    for r in records:
        lines.append(f"| {r['nr']} | {r['name']} | {r['path']} | {r['type']} | {bytes_to_human(r['size'])} |")
    lines.append("")
    lines.append(f"**Summe Dateien:** {len(records)}  \n**Gesamtgröße:** {bytes_to_human(total_size)}")
    lines.append("")
    lines.append("## Statistik nach Dateityp")
    lines.append("")
    lines.append("| Dateityp | Anzahl |")
    lines.append("|---|---:|")
    for t, c in sorted(type_counter.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| {t} | {c} |")
    content = "\n".join(lines)

    ensure_dir(out_md_path.parent if str(out_md_path.parent) != "" else Path("."))
    out_md_path.write_text(content, encoding="utf-8")
    print(f"📓 MASTER-Datei geschrieben: {posix(out_md_path.resolve())}")


# ---------- Graphviz ----------

def dot_quote(s: str) -> str:
    # sichere DOT-String-Quote via JSON
    return json.dumps(s)


def write_graphviz(root: Path, file_deps: Dict[str, List[Dict[str, Union[str, bool]]]], out_dot: Path, out_png: Optional[Path] = None) -> None:
    lines = []
    lines.append('digraph "deps" {')
    lines.append('  rankdir=LR;')
    lines.append('  node [shape=box, style="rounded,filled", fillcolor="#f8f8f8", fontname="Helvetica"];')
    lines.append('  edge [fontname="Helvetica"];')

    # Sammle alle Knoten
    nodes = set()
    for src, deps in file_deps.items():
        nodes.add(src)
        for d in deps:
            nodes.add(d["target"])

    # Knoten ausgeben
    for n in sorted(nodes):
        label = Path(n).name
        lines.append(f"  {dot_quote(n)} [label={dot_quote(label)}, tooltip={dot_quote(n)}];")

    # Kanten
    for src, deps in file_deps.items():
        for d in deps:
            typ = str(d["type"])
            exists = bool(d.get("exists", True))
            style = "solid" if exists else "dashed"
            lines.append(f"  {dot_quote(src)} -> {dot_quote(d['target'])} [label={dot_quote(typ)}, style={style}];")

    lines.append("}")
    dot_text = "\n".join(lines)

    ensure_dir(out_dot.parent if str(out_dot.parent) != "" else Path("."))
    out_dot.write_text(dot_text, encoding="utf-8")
    print(f"🗺️  DOT geschrieben: {posix(out_dot.resolve())}")

    if out_png is not None:
        if which("dot") is None:
            print("⚠️  'dot' (Graphviz) wurde nicht gefunden. PNG wird nicht erzeugt.")
        else:
            try:
                subprocess.run(["dot", "-Tpng", str(out_dot), "-o", str(out_png)], check=True)
                print(f"🖼️  PNG geschrieben: {posix(out_png.resolve())}")
            except subprocess.CalledProcessError as e:
                print(f"⚠️  Fehler beim Erzeugen der PNG mit graphviz: {e}")


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Multidisziplinäre Projekt-Abhängigkeitsanalyse")
    ap.add_argument("--root", default=".", help="Projektwurzel")
    ap.add_argument("--rscript", default="parser.R", help="Pfad zum R-Parser-Skript")
    ap.add_argument("--rscript-bin", default="Rscript", help="Pfad/Name der Rscript-Binary (z. B. 'Rscript' oder 'C:\\Program Files\\R\\R-4.5.1\\bin\\Rscript.exe')")
    ap.add_argument("--out-json", default=None, help="Optional: schreibe Ergebnis-JSON hierhin")
    ap.add_argument("--include-missing", action="store_true", help="Auch nicht vorhandene Ziele ausgeben")
    ap.add_argument("--exclude", action="append", default=[".git", "node_modules", "venv", "__pycache__"], help="Verzeichnisse ausschließen (mehrfach nutzbar)")
    ap.add_argument("--out-master", default="MASTER-DATEI-REFERENZ.md", help="Pfad für die Master-Datei-Referenz (Markdown)")
    ap.add_argument("--out-graph", default="deps_graph.dot", help="Pfad für Graphviz-DOT")
    ap.add_argument("--out-graph-png", default=None, help="Optionaler Pfad für Graph-PNG (benötigt 'dot')")
    # Folgeanalyse
    ap.add_argument("--postprocess", action="store_true", help="Nachanalyse (fehlende Abhängigkeiten, Statistiken) ausführen")
    ap.add_argument("--postprocess-outdir", default="analysis_reports", help="Zielordner für Nachanalyse-Reports")
    ap.add_argument("--pp-normalize-paths", choices=["posix", "windows", "none"], default="posix", help="Pfadnormalisierung in deps_normalized.json")
    ap.add_argument("--pp-dedupe", action="store_true", help="Kanten in deps_normalized.json deduplizieren")
    ap.add_argument("--pp-png", action="store_true", help="Missing-Graph zusätzlich als PNG rendern (Graphviz erforderlich)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    rscript_parser = Path(args.rscript).resolve()

    print(f"🚀 Starte Analyse\n   Root: {posix(root)}\n   R-Parser: {posix(rscript_parser)}")
    ensure_dir(Path(AST_OUTPUT_DIR))

    # Dateien einsammeln (inkl. Excludes)
    all_files: List[Path] = []
    for p in root.rglob("*"):
        if p.is_file() and not any(x in p.parts for x in args.exclude):
            all_files.append(p)

    # Schritt 1: R-ASTs generieren
    r_files = [p for p in all_files if p.suffix.lower() in (".r", ".rmd")]
    if r_files:
        print(f"⚙️  Generiere ASTs für {len(r_files)} R-Datei(en)...")
        generate_r_asts(root, r_files, rscript_parser, rscript_bin=args.rscript_bin)
    else:
        print("ℹ️  Keine R/Rmd-Dateien gefunden.")

    # Schritt 2: Analysieren
    file_dependencies: Dict[str, List[Dict[str, Union[str, bool]]]] = defaultdict(list)
    print(f"🔎 Analysiere {len(all_files)} Datei(en)...")

    for file_path in all_files:
        content = read_text_safe(file_path)
        if not content:
            continue

        deps: List[Dict[str, str]] = []
        ext = file_path.suffix.lower()
        name = file_path.name.lower()

        if ext in (".r", ".rmd"):
            deps.extend(analyze_r_file(root, file_path, content))
        elif ext == ".md":
            deps.extend(analyze_markdown_file(content))
        elif ext in (".yml", ".yaml"):
            deps.extend(analyze_yaml_file(content))
        elif ext in (".sh", ".bat"):
            deps.extend(analyze_shell_script(content))
        elif "dockerfile" in name or name == "makefile":
            deps.extend(analyze_dockerfile(content))
        elif ext in (".ini", ".conf"):
            deps.extend(analyze_generic_config(content))

        if not deps:
            continue

        # Quelle relativ zur Projektwurzel
        try:
            source_rel = file_path.resolve().relative_to(root.resolve())
        except ValueError:
            source_rel = file_path.resolve()

        for dep in deps:
            abs_target = resolve_path(file_path, dep["target"])
            # Ziel relativ zu Projektwurzel, sonst absolut
            try:
                rel_target = abs_target.relative_to(root)
            except ValueError:
                rel_target = abs_target
            exists = abs_target.exists()
            if args.include_missing or exists:
                file_dependencies[posix(source_rel)].append({
                    "target": posix(rel_target),
                    "type": dep["type"],
                    "exists": exists
                })

    # Schritt 3a: Konsolen-Ausgabe
    print("\n" + "=" * 64)
    print("✅ Analyseergebnis: Projekt-Abhängigkeiten")
    print("=" * 64)

    if not file_dependencies:
        print("\nKeine Abhängigkeiten gefunden.")
    else:
        for src in sorted(file_dependencies.keys()):
            print(f"\n📄 Datei: {posix(src)}")
            for dep in file_dependencies[src]:
                tag = "✔︎" if dep["exists"] else "✖"
                print(f"  --[{dep['type']:<15}]--> {dep['target']}  ({tag})")

    # Schritt 3b: JSON
    out_json_path: Optional[Path] = None
    if args.out_json:
        out_json_path = Path(args.out_json)
        ensure_dir(out_json_path.parent if str(out_json_path.parent) != "" else Path("."))
        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(file_dependencies, f, ensure_ascii=False, indent=2)
        print(f"\n📝 JSON geschrieben: {posix(out_json_path.resolve())}")

    # Schritt 3c: MASTER-DATEI-REFERENZ.md
    out_master_path = Path(args.out_master)
    if not out_master_path.is_absolute():
        out_master_path = Path(root) / out_master_path
    write_master_reference(root, all_files, out_master_path)

    # Schritt 3d: Graphviz (.dot / .png)
    out_dot_path = Path(args.out_graph)
    if not out_dot_path.is_absolute():
        out_dot_path = Path(root) / out_dot_path

    out_png_path = None
    if args.out_graph_png:
        op = Path(args.out_graph_png)
        out_png_path = op if op.is_absolute() else (Path(root) / op)

    write_graphviz(root, file_dependencies, out_dot_path, out_png_path)

    # Schritt 3e: Nachanalyse
    if args.postprocess:
        if not out_json_path:
            print("\n⚠️  Nachanalyse übersprungen: Bitte --out-json angeben.")
        else:
            pp_script = Path(__file__).parent / "deps_postprocess.py"
            if not pp_script.exists():
                print("\n⚠️  Nachanalyse-Skript deps_postprocess.py nicht gefunden (erwarte es neben project_analyzer.py).")
            else:
                cmd = [
                    sys.executable, str(pp_script),
                    "--in-json", str(out_json_path.resolve()),
                    "--out-dir", str((Path(root) / args.postprocess_outdir).resolve()),
                    "--normalize-paths", args.pp_normalize_paths
                ]
                if args.pp_dedupe:
                    cmd.append("--dedupe")
                if args.pp_png:
                    cmd.append("--png")
                try:
                    print(f"\n📦 Starte Folgeanalyse: {' '.join(cmd)}")
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError as e:
                    print(f"⚠️  Nachanalyse fehlgeschlagen: {e}")

if __name__ == "__main__":
    main()
