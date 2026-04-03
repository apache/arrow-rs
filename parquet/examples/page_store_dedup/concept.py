#!/usr/bin/env python3
"""
Generate the Parquet Page Store concept diagram.

Output: page_store_concept.svg  (open in any browser)
        page_store_concept.png  (requires drawsvg[raster])
"""

import os
import drawsvg as draw

HERE = os.path.dirname(__file__)

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

BG      = "#0f1117"
SURFACE = "#161b22"
BORDER  = "#2a2f3a"
TEXT_HI = "#f0f6fc"
TEXT_LO = "#6e7681"
BLUE    = "#4493f8"
GREEN   = "#3fb950"
PURPLE  = "#bc8cff"
ORANGE  = "#f0883e"
WHITE   = "#ffffff"

# ---------------------------------------------------------------------------
# Layout grid  (derive everything from these constants)
# ---------------------------------------------------------------------------

PAD       = 28          # outer margin
GAP       = 120         # gap between file panel right edge and store left edge

FILE_W    = 360
FILE_H    = 104
FILE_GAP  = 14          # vertical gap between file cards

N_FILES   = 4
FILES_H   = N_FILES * FILE_H + (N_FILES - 1) * FILE_GAP   # 502

STORE_Y_PAD = 38        # store header height (folder name + divider)
STORE_LEG_H = 82        # legend block at bottom of store
STORE_H   = FILES_H     # store and file panel share the same height

TITLE_H   = 82          # space taken by title block
TOP_Y     = TITLE_H + 12

CMP_H     = 82          # bottom comparison bar height
CMP_GAP   = 18

STORE_W   = 256         # fixed, intentionally compact
STORE_X   = PAD + FILE_W + GAP
CANVAS_W  = STORE_X + STORE_W + PAD
CANVAS_H  = TOP_Y + STORE_H + CMP_GAP + CMP_H + PAD


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate(total_mb: float = 3129.7, store_mb: float = 563.4) -> None:
    """Render the concept SVG (and optionally PNG) to the script directory."""

    d = draw.Drawing(CANVAS_W, CANVAS_H)

    # -----------------------------------------------------------------------
    # Drawing helpers  (closures over d)
    # -----------------------------------------------------------------------

    def bg_rect(x, y, w, h, fill=SURFACE, stroke=BORDER, rx=8, **kw):
        d.append(draw.Rectangle(x, y, w, h, fill=fill, stroke=stroke,
                                 stroke_width=1.5, rx=rx, ry=rx, **kw))

    def txt(s, x, y, size=13, fill=TEXT_HI, anchor="middle", weight="normal", **kw):
        d.append(draw.Text(s, size, x, y, text_anchor=anchor, fill=fill,
                           font_weight=weight,
                           font_family="ui-monospace,'SF Mono',monospace", **kw))

    def hline(x1, x2, y, color=BORDER, width=1, opacity=1.0, dash=None):
        kw = {"stroke_dasharray": dash} if dash else {}
        d.append(draw.Line(x1, y, x2, y, stroke=color, stroke_width=width,
                           stroke_opacity=opacity, **kw))

    arrowhead_ids: dict[str, str] = {}

    def _arrowhead(color: str) -> str:
        if color not in arrowhead_ids:
            aid = f"ah{len(arrowhead_ids)}"
            arrowhead_ids[color] = aid
            m = draw.Marker(-0.1, -3, 4, 3, orient="auto", id=aid)
            m.append(draw.Path(d="M0,-2.5 L3.5,0 L0,2.5 Z", fill=color))
            d.append_def(m)
        return f"url(#{arrowhead_ids[color]})"

    def arrow_line(x1, y, x2, color, label=None):
        path = draw.Path(stroke=color, stroke_width=1.8, stroke_opacity=0.55,
                         fill="none", marker_end=_arrowhead(color))
        path.M(x1, y)
        path.L(x2, y)
        d.append(path)
        if label:
            mx, lw = (x1 + x2) / 2, 72
            d.append(draw.Rectangle(mx - lw / 2, y - 10, lw, 16,
                                     fill="#1c2128", stroke=color, stroke_width=1,
                                     stroke_opacity=0.4, rx=8, ry=8))
            txt(label, mx, y + 2, size=9, fill=color, weight="bold")

    def page_tile(x, y, w, h, color, label):
        """Filled page block with glow halo + label."""
        d.append(draw.Rectangle(x - 3, y - 3, w + 6, h + 6,
                                 fill=color, fill_opacity=0.07, rx=6, ry=6))
        d.append(draw.Rectangle(x, y, w, h, fill=color, fill_opacity=0.18,
                                 stroke=color, stroke_width=1.5, stroke_opacity=0.7,
                                 rx=5, ry=5))
        txt(label, x + w / 2, y + h / 2 + 5, size=14, fill=color, weight="bold")

    def file_page(x, y, w, h, color, label):
        """Smaller page block used inside file cards."""
        d.append(draw.Rectangle(x, y, w, h, fill=color, fill_opacity=0.22,
                                 stroke=color, stroke_width=1, stroke_opacity=0.55,
                                 rx=3, ry=3))
        txt(label, x + w / 2, y + h / 2 + 4, size=9, fill=color, weight="bold")

    # -----------------------------------------------------------------------
    # Background + grid
    # -----------------------------------------------------------------------

    d.append(draw.Rectangle(0, 0, CANVAS_W, CANVAS_H, fill=BG))
    for gx in range(0, CANVAS_W, 40):
        d.append(draw.Line(gx, 0, gx, CANVAS_H, stroke=WHITE,
                           stroke_width=0.18, stroke_opacity=0.04))
    for gy in range(0, CANVAS_H, 40):
        d.append(draw.Line(0, gy, CANVAS_W, gy, stroke=WHITE,
                           stroke_width=0.18, stroke_opacity=0.04))

    # -----------------------------------------------------------------------
    # Title
    # -----------------------------------------------------------------------

    txt("Parquet Page Store", CANVAS_W / 2, 32, size=22, weight="bold")

    cx = CANVAS_W / 2
    d.append(draw.Raw(
        f'<text x="{cx}" y="53" text-anchor="middle"'
        f' font-family="ui-monospace,\'SF Mono\',monospace" font-size="11">'
        f'<tspan fill="{TEXT_LO}">Deduplication built into the Arrow Rust Parquet writer using </tspan>'
        f'<tspan fill="{WHITE}" font-weight="bold">Content-Defined Chunking</tspan>'
        f'</text>'
    ))

    hline(CANVAS_W / 2 - 230, CANVAS_W / 2 + 230, 63, color=BORDER)
    hline(CANVAS_W / 2 - 50,  CANVAS_W / 2 + 50,  63, color=BLUE, width=2, opacity=0.45)

    # -----------------------------------------------------------------------
    # Section labels  (centered above each panel)
    # -----------------------------------------------------------------------

    FILES_CX = PAD + FILE_W / 2
    STORE_CX = STORE_X + STORE_W / 2

    LABEL_Y = TOP_Y - 10
    txt("INPUT FILES", FILES_CX, LABEL_Y, size=9, fill=TEXT_LO, weight="bold")
    txt("PAGE STORE",  STORE_CX, LABEL_Y, size=9, fill=TEXT_LO, weight="bold")

    # -----------------------------------------------------------------------
    # Store card
    # -----------------------------------------------------------------------

    bg_rect(STORE_X, TOP_Y, STORE_W, STORE_H, fill="#0d1117", stroke=BORDER, rx=10)

    txt("pages/", STORE_X + 16, TOP_Y + 20, size=11, fill=TEXT_LO, anchor="start")
    hline(STORE_X + 12, STORE_X + STORE_W - 12, TOP_Y + 30, color=BORDER)

    # -----------------------------------------------------------------------
    # Unique pages grid  (centered inside the store card)
    # -----------------------------------------------------------------------

    UNIQUE_PAGES = [
        (BLUE,   "A"), (BLUE,   "B"), (BLUE,   "C"), (BLUE,   "D"),
        (BLUE,   "E"), (BLUE,   "F"), (PURPLE, "G"), (PURPLE, "H"),
        (GREEN,  "I"), (GREEN,  "J"), (ORANGE, "K"), (ORANGE, "L"),
    ]

    SP_COLS   = 3
    SPW, SPH  = 56, 40
    SP_GAP_X  = 14
    SP_GAP_Y  = 10

    grid_w = SP_COLS * SPW + (SP_COLS - 1) * SP_GAP_X
    SP_START_X = STORE_X + (STORE_W - grid_w) // 2
    SP_START_Y = TOP_Y + STORE_Y_PAD + 10

    page_centers: dict[str, tuple[float, float]] = {}

    for i, (color, label) in enumerate(UNIQUE_PAGES):
        col, row = i % SP_COLS, i // SP_COLS
        px = SP_START_X + col * (SPW + SP_GAP_X)
        py = SP_START_Y + row * (SPH + SP_GAP_Y)
        page_tile(px, py, SPW, SPH, color, label)
        page_centers[label] = (px + SPW / 2, py + SPH / 2)
        txt(f"#{label.lower()}3f9a…", px + SPW / 2, py + SPH + 10,
            size=7, fill=TEXT_LO)

    N_PAGE_ROWS = (len(UNIQUE_PAGES) + SP_COLS - 1) // SP_COLS
    last_row_py   = SP_START_Y + (N_PAGE_ROWS - 1) * (SPH + SP_GAP_Y)
    hash_label_bottom = last_row_py + SPH + 14

    LIST_MARGIN_X = 14
    LIST_INNER_X  = 10
    LINE_H        = 13
    LIST_INNER_PY = 7

    LISTING = [
        ("158k", "a3f9b2e1c04d7f28"),
        ("201k", "ff22e9640578db3c"),
        ("167k", "bc8cff3ad19f673d"),
        ("148k", "4493f8c9b28705f3"),
        ("160k", "3fb950efa4891422"),
    ]

    list_x = STORE_X + LIST_MARGIN_X
    list_w = STORE_W - 2 * LIST_MARGIN_X
    list_y = hash_label_bottom + 8
    list_h = len(LISTING) * LINE_H + 2 * LIST_INNER_PY

    d.append(draw.Rectangle(list_x, list_y, list_w, list_h,
                             fill="#0a0d12", rx=4, ry=4))

    for i, (size, hash_prefix) in enumerate(LISTING):
        baseline = list_y + LIST_INNER_PY + i * LINE_H + LINE_H - 3
        line_txt = f".rw-r--r--  {size:>4}  {hash_prefix}….page"
        txt(line_txt, list_x + LIST_INNER_X, baseline,
            size=7.5, fill="#3d4450", anchor="start")

    # -----------------------------------------------------------------------
    # Legend  (centered, pinned to bottom of store card)
    # -----------------------------------------------------------------------

    LEG_ITEMS = [
        (BLUE,   "shared by all"),
        (PURPLE, "filter boundary"),
        (GREEN,  "new column"),
        (ORANGE, "new rows"),
    ]
    LEG_COL_W = STORE_W / 2 - 4
    LEG_Y0    = TOP_Y + STORE_H - STORE_LEG_H + 20

    hline(STORE_X + 12, STORE_X + STORE_W - 12,
          TOP_Y + STORE_H - STORE_LEG_H, color=BORDER)

    for i, (color, label) in enumerate(LEG_ITEMS):
        col, row = i % 2, i // 2
        lx = STORE_X + 20 + col * LEG_COL_W
        ly = LEG_Y0 + row * 22
        d.append(draw.Rectangle(lx, ly - 7, 11, 11, fill=color,
                                 fill_opacity=0.85, rx=2, ry=2))
        txt(label, lx + 16, ly + 2, size=10, fill=TEXT_LO, anchor="start")

    # -----------------------------------------------------------------------
    # File cards
    # -----------------------------------------------------------------------

    PW, PH, PGAP = 34, 26, 4

    FILES = [
        ("original.parquet",  "baseline · 996k rows",
         [(BLUE,"A"),(BLUE,"B"),(BLUE,"C"),(BLUE,"D"),(BLUE,"E"),(BLUE,"F")],
         BLUE,    "baseline"),
        ("filtered.parquet",  "keep num_turns < 3",
         [(BLUE,"A"),(PURPLE,"G"),(BLUE,"C"),(BLUE,"D"),(BLUE,"E"),(PURPLE,"H")],
         PURPLE,  "92% reused"),
        ("augmented.parquet", "add num_turns column",
         [(BLUE,"A"),(GREEN,"I"),(BLUE,"B"),(BLUE,"C"),(GREEN,"J"),(BLUE,"D"),(BLUE,"E"),(BLUE,"F")],
         GREEN,   "98% reused"),
        ("appended.parquet",  "append 5 000 rows",
         [(BLUE,"A"),(BLUE,"B"),(BLUE,"C"),(BLUE,"D"),(BLUE,"E"),(BLUE,"F"),(ORANGE,"K"),(ORANGE,"L")],
         ORANGE,  "98% reused"),
    ]

    for fi, (fname, subtitle, pages, accent, reuse_lbl) in enumerate(FILES):
        fy = TOP_Y + fi * (FILE_H + FILE_GAP)
        card_mid_y = fy + FILE_H / 2

        bg_rect(PAD, fy, FILE_W, FILE_H, fill=SURFACE, stroke=BORDER, rx=8)
        d.append(draw.Rectangle(PAD, fy + 10, 3, FILE_H - 20,
                                 fill=accent, fill_opacity=0.85, rx=1, ry=1))
        txt(fname,    PAD + 16, fy + 26, size=12, fill=TEXT_HI, weight="bold", anchor="start")
        txt(subtitle, PAD + 16, fy + 43, size=10, fill=TEXT_LO, anchor="start")

        strip_x = PAD + 16
        strip_y = fy + FILE_H - PH - 12
        for pi, (pcolor, plabel) in enumerate(pages):
            file_page(strip_x + pi * (PW + PGAP), strip_y, PW, PH, pcolor, plabel)

        arrow_line(PAD + FILE_W + 4, card_mid_y, STORE_X - 4, accent, label=reuse_lbl)

    # -----------------------------------------------------------------------
    # Bottom: storage comparison bars
    # -----------------------------------------------------------------------

    CMP_Y = TOP_Y + STORE_H + CMP_GAP
    CMP_X = PAD
    CMP_W = CANVAS_W - PAD * 2

    bg_rect(CMP_X, CMP_Y, CMP_W, CMP_H, fill="#0d1117", stroke=BORDER, rx=8)
    txt("STORAGE COMPARISON", CMP_X + CMP_W / 2, CMP_Y + 13,
        size=9, fill=TEXT_LO, weight="bold")

    LABEL_COL_W = 132
    RIGHT_PAD   = 12
    TRACK_X = CMP_X + LABEL_COL_W
    TRACK_W = CMP_W - LABEL_COL_W - RIGHT_PAD - 220

    BAR_H = 20
    savings_pct = round((1 - store_mb / total_mb) * 100)
    ratio = total_mb / store_mb

    R1_Y = CMP_Y + 22
    txt("Vanilla Parquet", TRACK_X - 8, R1_Y + BAR_H / 2 + 4,
        size=10, fill=TEXT_LO, anchor="end")
    d.append(draw.Rectangle(TRACK_X, R1_Y, TRACK_W, BAR_H,
                             fill="#ef5350", fill_opacity=0.22,
                             stroke="#ef5350", stroke_width=1.2, stroke_opacity=0.45,
                             rx=4, ry=4))
    txt(f"{total_mb:,.0f} MB  (4 independent files)",
        TRACK_X + TRACK_W + 10, R1_Y + BAR_H / 2 + 4,
        size=10, fill="#ef9a9a", anchor="start")

    R2_Y  = R1_Y + BAR_H + 8
    WITH_W = round(TRACK_W * store_mb / total_mb)
    txt("Page Store via CDC", TRACK_X - 8, R2_Y + BAR_H / 2 + 4,
        size=10, fill=TEXT_LO, anchor="end")
    d.append(draw.Rectangle(TRACK_X, R2_Y, WITH_W, BAR_H,
                             fill="#66bb6a", fill_opacity=0.22,
                             stroke="#66bb6a", stroke_width=1.2, stroke_opacity=0.45,
                             rx=4, ry=4))
    txt(f"{store_mb:,.0f} MB  —  {savings_pct}% less  ·  {ratio:.1f}× ratio",
        TRACK_X + WITH_W + 10, R2_Y + BAR_H / 2 + 4,
        size=10, fill="#a5d6a7", anchor="start")

    # -----------------------------------------------------------------------
    # Save
    # -----------------------------------------------------------------------

    out_svg = os.path.join(HERE, "page_store_concept.svg")
    out_png = os.path.join(HERE, "page_store_concept.png")
    d.save_svg(out_svg)
    print(f"  Saved {out_svg}")

    try:
        d.save_png(out_png)
        print(f"  Saved {out_png}")
    except Exception as e:
        print(f"  PNG skipped ({e}) — open the SVG in a browser")


if __name__ == "__main__":
    generate()
