from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.runtime import log_event


_LIST_ITEM_PATTERN = re.compile(r"^(\s*)([-*]|\d+\.)\s+(.*)$")
_HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.*)$")
_INLINE_MATH_PATTERN = re.compile(r"(?<!\\)\$(.+?)(?<!\\)\$")
_PAREN_MATH_PATTERN = re.compile(r"\\\((.+?)\\\)")
_BRACKET_MATH_PATTERN = re.compile(r"\\\[(.+?)\\\]", re.DOTALL)
_LATEX_WRAPPER_PATTERN = re.compile(
    r"\\(?:text|mathrm|operatorname|mathit|mathbf|mathsf|textbf|textit)\{([^{}]+)\}"
)
_LATEX_MATHBB_PATTERN = re.compile(r"\\mathbb\{([^{}]+)\}")
_LATEX_SQRT_PATTERN = re.compile(r"\\sqrt\{([^{}]+)\}")
_LATEX_FRAC_PATTERN = re.compile(r"\\frac\{([^{}]+)\}\{([^{}]+)\}")
_LATEX_COMMAND_PATTERN = re.compile(r"\\[A-Za-z]+")
_BOLD_PATTERN = re.compile(r"(?<!\*)\*\*(?=\S)(.+?)(?<=\S)\*\*(?!\*)")
_ITALIC_PATTERN = re.compile(r"(?<![\w\u4e00-\u9fff])\*(?=\S)(.+?)(?<=\S)\*(?![\w\u4e00-\u9fff])")
_SUPERSCRIPT_PATTERN = re.compile(r"\^\{([^{}]+)\}|\^([A-Za-z0-9+\-=]+)")
_SUBSCRIPT_PATTERN = re.compile(r"_\{([^{}]+)\}|_([0-9+\-=]+)")

_LATEX_REPLACEMENTS = {
    r"\argmax": "argmax",
    r"\argmin": "argmin",
    r"\varepsilon": "ε",
    r"\epsilon": "ε",
    r"\lambda": "λ",
    r"\theta": "θ",
    r"\sigma": "σ",
    r"\alpha": "α",
    r"\gamma": "γ",
    r"\delta": "δ",
    r"\beta": "β",
    r"\omega": "ω",
    r"\Omega": "Ω",
    r"\Theta": "Θ",
    r"\Lambda": "Λ",
    r"\Delta": "Δ",
    r"\Phi": "Φ",
    r"\Pi": "Π",
    r"\mu": "μ",
    r"\rho": "ρ",
    r"\pi": "π",
    r"\cdots": "⋯",
    r"\ldots": "…",
    r"\dots": "…",
    r"\cdot": "·",
    r"\times": "×",
    r"\pm": "±",
    r"\neq": "≠",
    r"\ne": "≠",
    r"\leq": "≤",
    r"\le": "≤",
    r"\geq": "≥",
    r"\ge": "≥",
    r"\approx": "≈",
    r"\infty": "∞",
    r"\forall": "∀",
    r"\exists": "∃",
    r"\subseteq": "⊆",
    r"\subset": "⊂",
    r"\supseteq": "⊇",
    r"\cup": "∪",
    r"\cap": "∩",
    r"\notin": "∉",
    r"\in": "∈",
    r"\mapsto": "↦",
    r"\rightarrow": "→",
    r"\to": "→",
    r"\langle": "<",
    r"\rangle": ">",
    r"\lVert": "||",
    r"\rVert": "||",
    r"\Vert": "||",
    r"\|": "||",
    r"\log": "log",
    r"\ln": "ln",
    r"\exp": "exp",
    r"\min": "min",
    r"\max": "max",
    r"\sum": "Σ",
    r"\prod": "Π",
    r"\quad": " ",
    r"\qquad": " ",
    r"\{": "{",
    r"\}": "}",
}

_UNICODE_SUPERSCRIPTS = str.maketrans(
    {
        "0": "⁰",
        "1": "¹",
        "2": "²",
        "3": "³",
        "4": "⁴",
        "5": "⁵",
        "6": "⁶",
        "7": "⁷",
        "8": "⁸",
        "9": "⁹",
        "+": "⁺",
        "-": "⁻",
        "=": "⁼",
        "(": "⁽",
        ")": "⁾",
        "n": "ⁿ",
        "i": "ⁱ",
    }
)

_UNICODE_SUBSCRIPTS = str.maketrans(
    {
        "0": "₀",
        "1": "₁",
        "2": "₂",
        "3": "₃",
        "4": "₄",
        "5": "₅",
        "6": "₆",
        "7": "₇",
        "8": "₈",
        "9": "₉",
        "+": "₊",
        "-": "₋",
        "=": "₌",
        "(": "₍",
        ")": "₎",
    }
)

_SUPPORTED_SUPERSCRIPT_CHARS = set("0123456789+-=()ni")
_SUPPORTED_SUBSCRIPT_CHARS = set("0123456789+-=()")

_PDF_FONT_SEARCH_LOCATIONS = (
    Path.home() / "Library/Fonts",
    Path("/Library/Fonts"),
    Path("/System/Library/Fonts"),
    Path("/System/Library/Fonts/Supplemental"),
)

_PDF_FONT_PATTERNS = (
    ("MicrosoftYaHei", ("*Microsoft*YaHei*.ttf", "*Microsoft*YaHei*.ttc", "*YaHei*.ttf", "*YaHei*.ttc", "*微软雅黑*.ttf", "*微软雅黑*.ttc", "*msyh*.ttf", "*msyh*.ttc")),
    ("DengXian", ("*DengXian*.ttf", "*DengXian*.ttc", "*等线*.ttf", "*等线*.ttc")),
    ("PingFangSC", ("*PingFang*.ttf", "*PingFang*.ttc")),
    ("HiraginoSansGB", ("*Hiragino Sans GB*.ttf", "*Hiragino Sans GB*.ttc", "*Hiragino*Sans*GB*.ttc")),
    ("STHeiti", ("*STHeiti*.ttf", "*STHeiti*.ttc")),
    ("ArialUnicodeMS", ("*Arial Unicode*.ttf",)),
    ("SongtiSC", ("*Songti*.ttf", "*Songti*.ttc")),
)

_HTML_BODY_FONT_STACK = (
    '"Microsoft YaHei", "微软雅黑", "DengXian", "等线", '
    '"PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", '
    '"Source Han Sans SC", "Helvetica Neue", Arial, sans-serif'
)


@dataclass(slots=True)
class HeadingBlock:
    level: int
    text: str
    anchor: str


@dataclass(slots=True)
class ParagraphBlock:
    text: str


@dataclass(slots=True)
class QuoteBlock:
    text: str


@dataclass(slots=True)
class CodeBlock:
    language: str
    text: str


@dataclass(slots=True)
class PageBreakBlock:
    pass


@dataclass(slots=True)
class ListItemNode:
    text: str
    ordered: bool
    children: list["ListItemNode"] = field(default_factory=list)


@dataclass(slots=True)
class ListBlock:
    items: list[ListItemNode]


@dataclass(slots=True)
class ReportDocument:
    title: str
    blocks: list[Any]
    headings: list[HeadingBlock]


@dataclass(frozen=True, slots=True)
class PdfFontSelection:
    font_name: str
    font_path: str | None
    strategy: str
    embedded: bool
    failures: tuple[str, ...] = ()


def build_report_document(markdown_text: str, title: str | None = None) -> ReportDocument:
    blocks = _parse_markdown(markdown_text)
    headings = [block for block in blocks if isinstance(block, HeadingBlock)]
    resolved_title = title or next((block.text for block in headings if block.level == 1), "Paper Analysis Report")
    return ReportDocument(title=resolved_title, blocks=blocks, headings=headings)


def export_html_report(
    document: ReportDocument,
    output_path: str | Path,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output = Path(output_path)
    diagnostics = _collect_document_export_diagnostics(document)
    log_event(
        "info",
        "Report HTML export normalization prepared",
        output_path=output,
        inline_math_expressions=diagnostics["inline_math_expressions"],
        latex_commands=diagnostics["latex_commands"],
    )
    html_text = _render_html_document(document, metadata=metadata)
    output.write_text(html_text, encoding="utf-8")
    return {
        "format": "html",
        "path": str(output),
        "bytes": output.stat().st_size,
        "generated_at": _iso_now(),
    }


def export_pdf_report(
    document: ReportDocument,
    output_path: str | Path,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer

    output = Path(output_path)
    diagnostics = _collect_document_export_diagnostics(document)
    font_selection = _select_pdf_font()
    cjk_font_name = font_selection.font_name
    log_event(
        "info",
        "Report PDF export font prepared",
        output_path=output,
        font_name=font_selection.font_name,
        font_path=font_selection.font_path,
        font_strategy=font_selection.strategy,
        embedded=font_selection.embedded,
        inline_math_expressions=diagnostics["inline_math_expressions"],
        latex_commands=diagnostics["latex_commands"],
        font_failures=" | ".join(font_selection.failures) if font_selection.failures else None,
    )

    styles = getSampleStyleSheet()
    base_style = ParagraphStyle(
        "ReportBody",
        parent=styles["BodyText"],
        fontName=cjk_font_name,
        fontSize=10.5,
        leading=17,
        textColor=colors.HexColor("#1f2937"),
        spaceAfter=8,
        alignment=TA_LEFT,
        wordWrap="CJK",
    )
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=base_style,
        fontSize=22,
        leading=28,
        spaceBefore=4,
        spaceAfter=16,
        textColor=colors.HexColor("#0f172a"),
    )
    heading_two_style = ParagraphStyle(
        "ReportHeadingTwo",
        parent=base_style,
        fontSize=16,
        leading=22,
        spaceBefore=12,
        spaceAfter=8,
        textColor=colors.HexColor("#0f172a"),
    )
    heading_three_style = ParagraphStyle(
        "ReportHeadingThree",
        parent=base_style,
        fontSize=13,
        leading=18,
        spaceBefore=10,
        spaceAfter=6,
        textColor=colors.HexColor("#1d4ed8"),
    )
    quote_style = ParagraphStyle(
        "ReportQuote",
        parent=base_style,
        leftIndent=10,
        borderColor=colors.HexColor("#cbd5e1"),
        borderPadding=8,
        borderWidth=0.5,
        borderLeft=True,
        textColor=colors.HexColor("#334155"),
        backColor=colors.HexColor("#f8fafc"),
    )
    code_style = ParagraphStyle(
        "ReportCode",
        parent=base_style,
        fontSize=8.8,
        leading=12,
        leftIndent=10,
        rightIndent=10,
        borderColor=colors.HexColor("#d1d5db"),
        borderPadding=8,
        borderWidth=0.5,
        backColor=colors.HexColor("#f8fafc"),
        textColor=colors.HexColor("#7c2d12"),
        wordWrap="CJK",
    )
    list_style = ParagraphStyle(
        "ReportList",
        parent=base_style,
        spaceAfter=2,
    )
    note_list_style = ParagraphStyle(
        "ReportNoteList",
        parent=list_style,
        leftIndent=18,
        rightIndent=8,
        borderColor=colors.HexColor("#99f6e4"),
        borderPadding=6,
        borderWidth=0.6,
        borderLeft=True,
        backColor=colors.HexColor("#f0fdfa"),
        textColor=colors.HexColor("#0f766e"),
    )
    meta_label_style = ParagraphStyle(
        "MetaLabel",
        parent=base_style,
        fontSize=8.8,
        leading=12,
        textColor=colors.HexColor("#64748b"),
    )

    flowables: list[Any] = []
    flowables.append(Paragraph(_format_inline_for_pdf(document.title), title_style))

    meta_lines = _collect_metadata_lines(metadata)
    if meta_lines:
        flowables.append(Paragraph(_format_inline_for_pdf(" | ".join(meta_lines)), meta_label_style))
        flowables.append(Spacer(1, 6))

    for block in document.blocks:
        if isinstance(block, HeadingBlock):
            style = title_style if block.level == 1 else heading_two_style if block.level == 2 else heading_three_style
            if block.level == 1:
                continue
            flowables.append(Paragraph(_format_inline_for_pdf(block.text), style))
        elif isinstance(block, ParagraphBlock):
            flowables.append(Paragraph(_format_inline_for_pdf(block.text), base_style))
        elif isinstance(block, QuoteBlock):
            flowables.append(Paragraph(_format_inline_for_pdf(block.text), quote_style))
        elif isinstance(block, CodeBlock):
            flowables.append(Paragraph(_format_code_block_for_pdf(block.text), code_style))
        elif isinstance(block, PageBreakBlock):
            flowables.append(PageBreak())
            continue
        elif isinstance(block, ListBlock):
            flowables.extend(_build_pdf_list_flowables(block.items, list_style, note_list_style))
        flowables.append(Spacer(1, 3))

    doc = SimpleDocTemplate(
        str(output),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=16 * mm,
        title=document.title,
        author="LongChain Paper Agent",
    )

    def _draw_footer(canvas, pdf_doc) -> None:
        canvas.saveState()
        canvas.setTitle(document.title)
        canvas.setAuthor("LongChain Paper Agent")
        canvas.setFont(cjk_font_name, 8.5)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(pdf_doc.leftMargin, 10 * mm, document.title[:48])
        canvas.drawRightString(A4[0] - pdf_doc.rightMargin, 10 * mm, str(canvas.getPageNumber()))
        canvas.restoreState()

    doc.build(flowables, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    return {
        "format": "pdf",
        "path": str(output),
        "bytes": output.stat().st_size,
        "generated_at": _iso_now(),
    }


def _parse_markdown(markdown_text: str) -> list[Any]:
    blocks: list[Any] = []
    lines = markdown_text.splitlines()
    heading_counts: dict[str, int] = {}
    index = 0

    while index < len(lines):
        line = lines[index]

        if not line.strip():
            index += 1
            continue

        if line.strip() == "<!--PAGE_BREAK-->":
            blocks.append(PageBreakBlock())
            index += 1
            continue

        if line.lstrip().startswith("```"):
            language = line.strip()[3:].strip()
            code_lines: list[str] = []
            index += 1
            while index < len(lines) and not lines[index].lstrip().startswith("```"):
                code_lines.append(lines[index])
                index += 1
            if index < len(lines):
                index += 1
            blocks.append(CodeBlock(language=language, text="\n".join(code_lines).rstrip()))
            continue

        heading_match = _HEADING_PATTERN.match(line)
        if heading_match:
            level = len(heading_match.group(1))
            text = heading_match.group(2).strip()
            anchor = _unique_anchor(text, heading_counts)
            blocks.append(HeadingBlock(level=level, text=text, anchor=anchor))
            index += 1
            continue

        if line.lstrip().startswith(">"):
            quote_lines = [line.lstrip()[1:].strip()]
            index += 1
            while index < len(lines) and lines[index].lstrip().startswith(">"):
                quote_lines.append(lines[index].lstrip()[1:].strip())
                index += 1
            blocks.append(QuoteBlock(text="\n".join(part for part in quote_lines if part)))
            continue

        if _is_list_item(line):
            list_lines = [line]
            index += 1
            while index < len(lines):
                candidate = lines[index]
                if not candidate.strip():
                    break
                if candidate.lstrip().startswith("```") or _HEADING_PATTERN.match(candidate):
                    break
                if _is_list_item(candidate) or (_has_indentation(candidate) and not candidate.lstrip().startswith(">")):
                    list_lines.append(candidate)
                    index += 1
                    continue
                break
            blocks.append(_parse_list_block(list_lines))
            continue

        paragraph_lines = [line.strip()]
        index += 1
        while index < len(lines):
            candidate = lines[index]
            if not candidate.strip():
                break
            if candidate.lstrip().startswith("```") or _HEADING_PATTERN.match(candidate) or _is_list_item(candidate):
                break
            if candidate.lstrip().startswith(">"):
                break
            paragraph_lines.append(candidate.strip())
            index += 1
        blocks.append(ParagraphBlock(text="\n".join(paragraph_lines).strip()))

    return blocks


def _parse_list_block(lines: list[str]) -> ListBlock:
    roots: list[ListItemNode] = []
    node_stack: list[ListItemNode] = []

    for raw_line in lines:
        match = _LIST_ITEM_PATTERN.match(raw_line)
        if match:
            indent, marker, text = match.groups()
            level = max(0, len(indent.expandtabs(2)) // 2)
            node = ListItemNode(text=text.strip(), ordered=marker.endswith("."))
            if level <= 0:
                roots.append(node)
                node_stack = [node]
                continue

            if not node_stack:
                roots.append(node)
                node_stack = [node]
                continue

            while len(node_stack) > level:
                node_stack.pop()
            parent = node_stack[-1] if node_stack else None
            if parent is None:
                roots.append(node)
                node_stack = [node]
                continue

            parent.children.append(node)
            if len(node_stack) == level:
                node_stack.append(node)
            else:
                node_stack = node_stack[:level]
                node_stack.append(node)
            continue

        if node_stack:
            continuation = raw_line.strip()
            if continuation:
                node_stack[-1].text = f"{node_stack[-1].text}\n{continuation}".strip()

    return ListBlock(items=roots)


def _render_html_document(document: ReportDocument, metadata: dict[str, Any] | None = None) -> str:
    toc_headings = [heading for heading in document.headings if heading.level in {2, 3}]
    body_parts = [
        _render_block_html(block)
        for block in document.blocks
        if not (isinstance(block, HeadingBlock) and block.level == 1)
    ]
    meta_lines = _collect_metadata_lines(metadata)
    meta_html = "".join(f"<span>{html.escape(line)}</span>" for line in meta_lines)
    toc_html = "".join(
        (
            f'<a class="toc-level-{heading.level}" href="#{heading.anchor}">'
            f"{html.escape(heading.text)}</a>"
        )
        for heading in toc_headings
    )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(document.title)}</title>
    <style>
      :root {{
        --bg: #f6f1e8;
        --panel: rgba(255, 255, 255, 0.88);
        --panel-strong: rgba(255, 255, 255, 0.95);
        --text: #172033;
        --muted: #52607a;
        --line: rgba(148, 163, 184, 0.32);
        --accent: #0f766e;
        --accent-soft: rgba(15, 118, 110, 0.08);
        --code-bg: #f8fafc;
        --shadow: 0 24px 80px rgba(15, 23, 42, 0.08);
        --radius: 24px;
      }}

      * {{
        box-sizing: border-box;
      }}

      html {{
        scroll-behavior: smooth;
      }}

      body {{
        margin: 0;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 28%),
          radial-gradient(circle at top right, rgba(30, 64, 175, 0.12), transparent 30%),
          linear-gradient(180deg, #fbf7f1 0%, var(--bg) 52%, #efe7dc 100%);
        font-family: {_HTML_BODY_FONT_STACK};
        line-height: 1.82;
      }}

      a {{
        color: #1d4ed8;
        text-decoration: none;
      }}

      a:hover {{
        text-decoration: underline;
      }}

      .page {{
        max-width: 1440px;
        margin: 0 auto;
        padding: 40px 24px 80px;
      }}

      .hero {{
        padding: 28px 30px;
        border: 1px solid var(--line);
        border-radius: var(--radius);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(255, 255, 255, 0.82));
        box-shadow: var(--shadow);
        backdrop-filter: blur(10px);
      }}

      .hero h1 {{
        margin: 0;
        font-size: clamp(2rem, 5vw, 3.3rem);
        line-height: 1.2;
        letter-spacing: -0.03em;
      }}

      .hero-meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 16px;
      }}

      .hero-meta span {{
        padding: 7px 12px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--muted);
        font-size: 0.92rem;
      }}

      .layout {{
        display: grid;
        grid-template-columns: minmax(0, 280px) minmax(0, 1fr);
        gap: 24px;
        margin-top: 26px;
      }}

      .toc {{
        position: sticky;
        top: 24px;
        align-self: start;
        padding: 22px 20px;
        border: 1px solid var(--line);
        border-radius: 20px;
        background: var(--panel);
        box-shadow: var(--shadow);
      }}

      .toc h2 {{
        margin: 0 0 14px;
        font-size: 1rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--muted);
      }}

      .toc nav {{
        display: flex;
        flex-direction: column;
        gap: 8px;
      }}

      .toc nav a {{
        color: var(--text);
        opacity: 0.88;
      }}

      .toc nav a.toc-level-3 {{
        padding-left: 14px;
        color: var(--muted);
        font-size: 0.96rem;
      }}

      .report {{
        padding: 34px clamp(18px, 3vw, 42px) 40px;
        border: 1px solid var(--line);
        border-radius: var(--radius);
        background: var(--panel-strong);
        box-shadow: var(--shadow);
      }}

      .report > :first-child {{
        margin-top: 0;
      }}

      .report h1,
      .report h2,
      .report h3 {{
        scroll-margin-top: 24px;
      }}

      .report h1 {{
        font-size: clamp(2rem, 4vw, 2.8rem);
        line-height: 1.22;
        letter-spacing: -0.03em;
        margin: 0 0 20px;
      }}

      .report h2 {{
        margin: 34px 0 14px;
        padding-top: 8px;
        font-size: 1.58rem;
        line-height: 1.35;
        border-top: 1px solid rgba(148, 163, 184, 0.2);
      }}

      .report h3 {{
        margin: 24px 0 10px;
        font-size: 1.18rem;
        color: #0f3d68;
      }}

      .report p,
      .report li,
      .report blockquote {{
        font-size: 1.02rem;
      }}

      .report ul,
      .report ol {{
        margin: 10px 0 14px 1.35rem;
        padding: 0;
      }}

      .report li {{
        margin: 0.32rem 0;
        padding-left: 0.22rem;
      }}

      .report li.note-item {{
        margin: 0.6rem 0;
        padding: 0.78rem 0.95rem;
        list-style: none;
        border-left: 4px solid rgba(15, 118, 110, 0.35);
        border-radius: 14px;
        background: rgba(240, 253, 250, 0.95);
      }}

      .page-break {{
        height: 56px;
        margin: 30px 0 12px;
        border-top: 1px dashed rgba(15, 23, 42, 0.18);
      }}

      .report code {{
        padding: 0.12rem 0.36rem;
        border-radius: 0.42rem;
        background: rgba(15, 23, 42, 0.06);
        font-family: "Iosevka", "JetBrains Mono", "SFMono-Regular", monospace;
        font-size: 0.92em;
      }}

      .report pre {{
        margin: 18px 0;
        padding: 18px 20px;
        overflow-x: auto;
        border: 1px solid rgba(148, 163, 184, 0.24);
        border-radius: 18px;
        background: var(--code-bg);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9);
      }}

      .report pre code {{
        display: block;
        padding: 0;
        background: transparent;
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 0.92rem;
      }}

      .report blockquote {{
        margin: 18px 0;
        padding: 8px 18px;
        border-left: 4px solid rgba(15, 118, 110, 0.35);
        background: rgba(248, 250, 252, 0.92);
        color: #334155;
      }}

      .report strong {{
        color: #0f172a;
      }}

      @media (max-width: 1080px) {{
        .layout {{
          grid-template-columns: 1fr;
        }}

        .toc {{
          position: static;
        }}
      }}

      @media print {{
        body {{
          background: white;
        }}

        .page {{
          max-width: none;
          padding: 0;
        }}

        .hero,
        .toc,
        .report {{
          box-shadow: none;
          background: white;
          border-color: #d6d6d6;
        }}

        .page-break {{
          height: 0;
          margin: 0;
          border: 0;
          break-before: page;
          page-break-before: always;
        }}
      }}
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <h1>{html.escape(document.title)}</h1>
        <div class="hero-meta">{meta_html}</div>
      </section>
      <section class="layout">
        <aside class="toc">
          <h2>目录</h2>
          <nav>{toc_html}</nav>
        </aside>
        <article class="report">
          {''.join(body_parts)}
        </article>
      </section>
    </main>
  </body>
</html>
"""


def _render_block_html(block: Any) -> str:
    if isinstance(block, HeadingBlock):
        return f'<h{block.level} id="{block.anchor}">{_format_inline_for_html(block.text)}</h{block.level}>'
    if isinstance(block, ParagraphBlock):
        return f"<p>{_format_inline_for_html(block.text)}</p>"
    if isinstance(block, QuoteBlock):
        return f"<blockquote>{_format_inline_for_html(block.text)}</blockquote>"
    if isinstance(block, CodeBlock):
        language_class = f' class="language-{html.escape(block.language)}"' if block.language else ""
        return f"<pre><code{language_class}>{html.escape(block.text)}</code></pre>"
    if isinstance(block, PageBreakBlock):
        return '<div class="page-break" aria-hidden="true"></div>'
    if isinstance(block, ListBlock):
        return _render_list_html(block.items)
    return ""


def _render_list_html(items: list[ListItemNode]) -> str:
    parts: list[str] = []
    for ordered, group in _group_list_items(items):
        tag = "ol" if ordered else "ul"
        parts.append(f"<{tag}>")
        for item in group:
            li_class = ' class="note-item"' if _is_annotation_item(item.text) else ""
            parts.append(f"<li{li_class}>")
            parts.append(_format_inline_for_html(item.text))
            if item.children:
                parts.append(_render_list_html(item.children))
            parts.append("</li>")
        parts.append(f"</{tag}>")
    return "".join(parts)


def _build_pdf_list_flowables(items: list[ListItemNode], style: Any, note_style: Any, depth: int = 0) -> list[Any]:
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph, Spacer

    flowables: list[Any] = []
    ordered_counters: dict[int, int] = {}

    for item in items:
        parent_style = note_style if _is_annotation_item(item.text) else style
        paragraph_style = ParagraphStyle(
            f"ReportListDepth{depth}",
            parent=parent_style,
            leftIndent=12 + depth * 16,
            firstLineIndent=0,
            spaceAfter=3,
        )
        if item.ordered:
            ordered_counters[depth] = ordered_counters.get(depth, 0) + 1
            marker = f"{ordered_counters[depth]}. "
        else:
            marker = "- "
        flowables.append(Paragraph(_format_inline_for_pdf(f"{marker}{item.text}"), paragraph_style))
        if item.children:
            flowables.extend(_build_pdf_list_flowables(item.children, style, note_style, depth=depth + 1))
        flowables.append(Spacer(1, 1))

    return flowables


def _group_list_items(items: list[ListItemNode]) -> list[tuple[bool, list[ListItemNode]]]:
    groups: list[tuple[bool, list[ListItemNode]]] = []
    current_ordered: bool | None = None
    current_group: list[ListItemNode] = []

    for item in items:
        if current_ordered is None or item.ordered == current_ordered:
            current_group.append(item)
            current_ordered = item.ordered
            continue
        groups.append((bool(current_ordered), current_group))
        current_ordered = item.ordered
        current_group = [item]

    if current_group:
        groups.append((bool(current_ordered), current_group))
    return groups


def _collect_metadata_lines(metadata: dict[str, Any] | None) -> list[str]:
    if not metadata:
        return []

    labels = {
        "document_model": "文档模型",
        "analysis_model": "分析模型",
        "sections": "深读章节",
        "web_search_enabled": "联网搜索",
        "paper_char_count": "文本字符数",
    }
    lines: list[str] = []
    for key in ("document_model", "analysis_model", "sections", "web_search_enabled", "paper_char_count"):
        value = metadata.get(key)
        if value is None:
            continue
        label = labels.get(key, key)
        rendered = "开启" if key == "web_search_enabled" and bool(value) else "关闭" if key == "web_search_enabled" else value
        lines.append(f"{label}: {rendered}")
    return lines


def _format_inline_for_html(text: str) -> str:
    return _format_inline(text, mode="html")


def _format_inline_for_pdf(text: str) -> str:
    return _format_inline(text, mode="pdf")


def _format_inline(text: str, mode: str) -> str:
    placeholders: dict[str, str] = {}
    normalized = _normalize_math_markup(text)
    escaped = html.escape(normalized, quote=False)

    def stash(rendered: str) -> str:
        token = f"__MARKUP_{len(placeholders)}__"
        placeholders[token] = rendered
        return token

    def render_link(label: str, url: str) -> str:
        safe_label = html.escape(label, quote=False)
        safe_url = html.escape(url, quote=True)
        if mode == "html":
            return f'<a href="{safe_url}" target="_blank" rel="noreferrer">{safe_label}</a>'
        return f'<link href="{safe_url}"><u>{safe_label}</u></link>'

    def render_code(code_text: str) -> str:
        safe_text = html.escape(code_text, quote=False)
        if mode == "html":
            return f"<code>{safe_text}</code>"
        return f'<font color="#7c2d12">{safe_text}</font>'

    escaped = re.sub(
        r"\[([^\]]+)\]\((https?://[^)\s]+)\)",
        lambda match: stash(render_link(match.group(1), match.group(2))),
        escaped,
    )
    escaped = re.sub(
        r"`([^`]+)`",
        lambda match: stash(render_code(match.group(1))),
        escaped,
    )
    escaped = _BOLD_PATTERN.sub(r"<strong>\1</strong>" if mode == "html" else r"<b>\1</b>", escaped)
    escaped = _ITALIC_PATTERN.sub(r"<em>\1</em>" if mode == "html" else r"<i>\1</i>", escaped)
    escaped = re.sub(
        r"(?<![\"=/])(https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+)",
        lambda match: stash(render_link(match.group(1), match.group(1))),
        escaped,
    )
    escaped = escaped.replace("\n", "<br/>")

    for token, rendered in placeholders.items():
        escaped = escaped.replace(token, rendered)
    return escaped


def _format_code_block_for_pdf(text: str) -> str:
    safe_text = html.escape(text, quote=False)
    safe_text = safe_text.replace(" ", "&nbsp;").replace("\t", "&nbsp;" * 4).replace("\n", "<br/>")
    return safe_text


def _collect_document_export_diagnostics(document: ReportDocument) -> dict[str, int]:
    parts: list[str] = [document.title]
    for block in document.blocks:
        if isinstance(block, (HeadingBlock, ParagraphBlock, QuoteBlock, CodeBlock)):
            parts.append(block.text)
        elif isinstance(block, ListBlock):
            parts.extend(_flatten_list_items(block.items))
    joined = "\n".join(parts)
    return {
        "inline_math_expressions": (
            len(_INLINE_MATH_PATTERN.findall(joined))
            + len(_PAREN_MATH_PATTERN.findall(joined))
            + len(_BRACKET_MATH_PATTERN.findall(joined))
        ),
        "latex_commands": len(_LATEX_COMMAND_PATTERN.findall(joined)),
    }


def _flatten_list_items(items: list[ListItemNode]) -> list[str]:
    values: list[str] = []
    for item in items:
        values.append(item.text)
        if item.children:
            values.extend(_flatten_list_items(item.children))
    return values


def _iter_pdf_font_candidates() -> list[tuple[str, Path, int]]:
    candidates: list[tuple[str, Path, int]] = []
    seen_paths: set[str] = set()

    for base_name, patterns in _PDF_FONT_PATTERNS:
        for directory in _PDF_FONT_SEARCH_LOCATIONS:
            if not directory.exists():
                continue
            for pattern in patterns:
                for path in sorted(directory.glob(pattern)):
                    if not path.is_file():
                        continue
                    resolved = str(path.resolve())
                    if resolved in seen_paths:
                        continue
                    seen_paths.add(resolved)
                    candidates.append((base_name, path, 0))
    return candidates


def _select_pdf_font() -> PdfFontSelection:
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    from reportlab.pdfbase.pdfmetrics import getRegisteredFontNames, registerFont
    from reportlab.pdfbase.ttfonts import TTFont

    registered_fonts = set(getRegisteredFontNames())
    failures: list[str] = []

    for base_name, font_path, subfont_index in _iter_pdf_font_candidates():
        font_name = f"{base_name}-{font_path.stem.replace(' ', '_')}"
        try:
            if font_name not in registered_fonts:
                registerFont(TTFont(font_name, str(font_path), subfontIndex=subfont_index))
            return PdfFontSelection(
                font_name=font_name,
                font_path=str(font_path),
                strategy="embedded_truetype",
                embedded=True,
                failures=tuple(failures),
            )
        except Exception as exc:  # pragma: no cover - depends on local fonts
            failures.append(f"{font_name}:{font_path}:{type(exc).__name__}:{exc}")

    fallback_name = "STSong-Light"
    if fallback_name not in registered_fonts:
        registerFont(UnicodeCIDFont(fallback_name))
    return PdfFontSelection(
        font_name=fallback_name,
        font_path=None,
        strategy="cid_fallback",
        embedded=False,
        failures=tuple(failures),
    )


def _normalize_math_markup(text: str) -> str:
    if not text or ("$" not in text and "\\" not in text):
        return text

    normalized = text
    for pattern in (_BRACKET_MATH_PATTERN, _PAREN_MATH_PATTERN, _INLINE_MATH_PATTERN):
        normalized = pattern.sub(lambda match: _normalize_math_expression(match.group(1)), normalized)
    normalized = _apply_latex_replacements(normalized)
    normalized = normalized.replace(r"\$", "$")
    return normalized


def _normalize_math_expression(expression: str) -> str:
    normalized = expression.strip().replace("\n", " ")
    normalized = re.sub(r"\\(?:left|right|bigl|bigr|Bigl|Bigr|big|Big)\b", "", normalized)

    while True:
        updated = normalized
        updated = _LATEX_WRAPPER_PATTERN.sub(lambda match: _normalize_math_expression(match.group(1)), updated)
        updated = _LATEX_MATHBB_PATTERN.sub(lambda match: _render_mathbb(match.group(1)), updated)
        updated = _LATEX_SQRT_PATTERN.sub(lambda match: _render_sqrt(match.group(1)), updated)
        updated = _LATEX_FRAC_PATTERN.sub(lambda match: _render_fraction(match.group(1), match.group(2)), updated)
        if updated == normalized:
            break
        normalized = updated

    normalized = _apply_latex_replacements(normalized)
    normalized = re.sub(r"\\([A-Za-z]+)", r"\1", normalized)
    normalized = _SUPERSCRIPT_PATTERN.sub(_replace_superscript, normalized)
    normalized = _SUBSCRIPT_PATTERN.sub(_replace_subscript, normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\s*([=≤≥≈→↦])\s*", r" \1 ", normalized)
    normalized = re.sub(r"\s*([,:;])\s*", r"\1 ", normalized)
    normalized = re.sub(r"<\s*([^<>]*?,[^<>]*?)\s*>", r"<\1>", normalized)
    normalized = re.sub(r"\s+([)\]⟩])", r"\1", normalized)
    normalized = re.sub(r"([(\[⟨])\s+", r"\1", normalized)
    return normalized.strip()


def _apply_latex_replacements(text: str) -> str:
    normalized = text.replace("~", " ").replace(r"\\", " ")
    for source in sorted(_LATEX_REPLACEMENTS, key=len, reverse=True):
        normalized = normalized.replace(source, _LATEX_REPLACEMENTS[source])
    normalized = re.sub(r"\\mathcal\{([^{}]+)\}", r"\1", normalized)
    normalized = re.sub(r"\\operatorname\{([^{}]+)\}", r"\1", normalized)
    return normalized


def _render_mathbb(token: str) -> str:
    known = {
        "R": "ℝ",
        "N": "ℕ",
        "Z": "ℤ",
        "Q": "ℚ",
        "C": "ℂ",
    }
    return known.get(token, token)


def _render_sqrt(token: str) -> str:
    inner = _normalize_math_expression(token)
    if _is_simple_math_atom(inner):
        return f"√{inner}"
    return f"√({inner})"


def _render_fraction(numerator: str, denominator: str) -> str:
    left = _normalize_math_expression(numerator)
    right = _normalize_math_expression(denominator)
    if _needs_parentheses(left):
        left = f"({left})"
    if _needs_parentheses(right):
        right = f"({right})"
    return f"{left}/{right}"


def _replace_superscript(match: re.Match[str]) -> str:
    token = match.group(1) or match.group(2) or ""
    rendered = _translate_script(token, script="superscript")
    if rendered is not None:
        return rendered
    if match.group(1) is not None:
        return f"^({token})"
    return f"^{token}"


def _replace_subscript(match: re.Match[str]) -> str:
    token = match.group(1) or match.group(2) or ""
    rendered = _translate_script(token, script="subscript")
    if rendered is not None:
        return rendered
    if match.group(1) is not None:
        return f"_({token})"
    return f"_{token}"


def _translate_script(token: str, script: str) -> str | None:
    translation = _UNICODE_SUPERSCRIPTS if script == "superscript" else _UNICODE_SUBSCRIPTS
    supported = _SUPPORTED_SUPERSCRIPT_CHARS if script == "superscript" else _SUPPORTED_SUBSCRIPT_CHARS
    if not token or any(character not in supported for character in token):
        return None
    return token.translate(translation)


def _needs_parentheses(text: str) -> bool:
    if text.startswith("(") and text.endswith(")"):
        return False
    return any(character in text for character in (" ", "+", "-", "=", "≤", "≥", "≈"))


def _is_simple_math_atom(text: str) -> bool:
    return bool(re.fullmatch(r"[\w\u0370-\u03ff∞πλμσθαβγδΩΘΦΠ₀-₉⁰-⁹.+\-]+", text))


def _unique_anchor(text: str, heading_counts: dict[str, int]) -> str:
    base = re.sub(r"[^\w\u4e00-\u9fff]+", "-", text.lower()).strip("-") or "section"
    count = heading_counts.get(base, 0)
    heading_counts[base] = count + 1
    if count == 0:
        return base
    return f"{base}-{count + 1}"


def _is_list_item(line: str) -> bool:
    return bool(_LIST_ITEM_PATTERN.match(line))


def _has_indentation(line: str) -> bool:
    return bool(line) and len(line) > len(line.lstrip(" \t"))


def _is_annotation_item(text: str) -> bool:
    normalized = text.strip()
    return normalized.startswith("**批注**") or normalized.startswith("**审稿批注**")


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")
