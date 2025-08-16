#!/usr/bin/env python3
# build_reverse_index.py
import json, argparse
from pathlib import Path
from collections import defaultdict

def norm(s, mode):
    if mode == "posix": return s.replace("\\", "/")
    if mode == "windows": return s.replace("/", "\\")
    return s

def load_deps(p: Path, mode: str):
    data = json.loads(p.read_text(encoding="utf-8"))
    out = {}
    for src, edges in data.items():
        srcn = norm(src, mode)
        out[srcn] = []
        for e in edges:
            out[srcn].append({
                "target": norm(e.get("target",""), mode),
                "type": e.get("type",""),
                "exists": bool(e.get("exists", True)),
            })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-json", help="deps_normalized.json oder deps.json")
    ap.add_argument("--out-json", default="analysis_reports/deps_reverse_index.json")
    ap.add_argument("--normalize-paths", choices=["posix","windows","none"], default="posix")
    ap.add_argument("--missing-only", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.in_json) if args.in_json else None
    if not in_path:
        for cand in [Path("analysis_reports/deps_normalized.json"), Path("deps.json")]:
            if cand.exists():
                in_path = cand
                break
    if not in_path or not in_path.exists():
        raise SystemExit("Eingabe-JSON nicht gefunden. --in-json angeben oder deps.json/analysis_reports/deps_normalized.json bereitstellen.")

    deps = load_deps(in_path, args.normalize_paths)
    rev = defaultdict(list)
    for src, edges in deps.items():
        for e in edges:
            if args.missing_only and e["exists"]:
                continue
            rev[e["target"]].append({"source": src, "type": e["type"], "exists": e["exists"]})

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rev, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Reverse-Index geschrieben: {out_path.resolve()} ({len(rev)} Ziele)")

if __name__ == "__main__":
    main()
