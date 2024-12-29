"""Generate the code reference page for a single py-module."""

# ruff: noqa: INP001, F821
from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

source_layout = False  # type: ignore[reportUndefinedVariable]
package_name = "redis-clone"


def mangled(name: str) -> str:
    """Mangle hyphens to underscores."""
    return name.replace("-", "_")


mangled_package = mangled(package_name)
mod_symbol = '<code class="doc-symbol doc-symbol-nav doc-symbol-module"></code>'

root = Path(__file__).parent.parent

if not source_layout:
    module_path = root / f"{mangled_package}.py"
    doc_path = Path(f"reference/{mangled_package}.md")

    if module_path.exists():
        with mkdocs_gen_files.open(doc_path, "w") as fd:
            fd.write(f"---\ntitle: {mangled_package}\n---\n\n::: {mangled_package}")

        mkdocs_gen_files.set_edit_path(doc_path, ".." / module_path.relative_to(root))

else:
    nav = mkdocs_gen_files.Nav()  # type: ignore[no-untyped-call]

    src = root / "src"

    for path in sorted(src.rglob("*.py")):
        module_path = path.relative_to(src).with_suffix("")
        doc_path = path.relative_to(src / mangled_package).with_suffix(".md")
        full_doc_path = Path("reference", doc_path)

        parts = tuple(module_path.parts)

        if parts[-1] == "__init__":
            parts = parts[:-1]
            doc_path = doc_path.with_name("index.md")
            full_doc_path = full_doc_path.with_name("index.md")
        elif parts[-1].startswith("_"):
            continue

        nav_parts = [f"{mod_symbol} {part}" for part in parts]
        nav[tuple(nav_parts)] = doc_path.as_posix()

        with mkdocs_gen_files.open(full_doc_path, "w") as fd:
            ident = ".".join(parts)
            fd.write(f"---\ntitle: {ident}\n---\n\n::: {ident}")

        mkdocs_gen_files.set_edit_path(full_doc_path, ".." / path.relative_to(root))

    with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
        nav_file.writelines(nav.build_literate_nav())
