#!/usr/bin/env python3
import re
import sys
from pathlib import Path


def table_cells(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    return [cell.strip() for cell in stripped[1:-1].split("|")]


def is_separator_row(cells: list[str] | None) -> bool:
    return cells is not None and all(
        re.fullmatch(r":?-+:?", cell) is not None for cell in cells
    )


def format_report(source: Path, destination: Path) -> None:
    lines = source.read_text(encoding="utf-8").splitlines()
    formatted: list[str] = []
    section_count = 0
    index = 0

    while index < len(lines):
        header = table_cells(lines[index])
        divider = table_cells(lines[index + 1]) if index + 1 < len(lines) else None
        title = table_cells(lines[index + 2]) if index + 2 < len(lines) else None

        if (
            header == ["secao"]
            and is_separator_row(divider)
            and title is not None
            and len(title) == 1
        ):
            if formatted and formatted[-1] != "":
                formatted.append("")
            formatted.extend([f"## {title[0]}", ""])
            section_count += 1
            index += 3
            continue

        formatted.append(lines[index])
        index += 1

    if section_count == 0:
        raise ValueError("Nenhum marcador de secao Trino foi encontrado.")

    destination.write_text("\n".join(formatted).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Uso: format_trino_markdown.py ENTRADA SAIDA")
    try:
        format_report(Path(sys.argv[1]), Path(sys.argv[2]))
    except (OSError, UnicodeError, ValueError) as error:
        print(f"Erro ao formatar relatorio: {error}", file=sys.stderr)
        raise SystemExit(1) from error
