"""
Styled PDF class for environmental analysis reports.
Provides cover page, styled sections, stat cards, and modern tables.

Importable helper - use: from f.infrared.pdf_styles import StyledReportPDF
"""

#extra_requirements:
#fpdf2==2.8.3

import io
import base64
from datetime import datetime
from fpdf import FPDF
from fpdf.enums import MethodReturnValue


ACCENT = (76, 201, 240)     # #4cc9f0
ACCENT_DARK = (56, 163, 194)
DARK = (26, 26, 46)         # #1a1a2e
GRAY = (100, 100, 100)
LIGHT_BG = (245, 247, 250)  # #f5f7fa
WHITE = (255, 255, 255)
TEXT = (50, 50, 50)
TEXT_LIGHT = (120, 120, 120)

DIRS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

# Unicode -> Latin-1 safe replacements for fpdf2 built-in fonts
_UNICODE_MAP = {
    '\u2014': '--',   # em dash
    '\u2013': '-',    # en dash
    '\u2018': "'",    # left single quote
    '\u2019': "'",    # right single quote
    '\u201c': '"',    # left double quote
    '\u201d': '"',    # right double quote
    '\u2022': '-',    # bullet
    '\u2026': '...',  # ellipsis
    '\u2032': "'",    # prime
    '\u2033': '"',    # double prime
}


def _safe(text: str) -> str:
    """Replace non-Latin-1 Unicode chars with safe ASCII equivalents."""
    for uc, repl in _UNICODE_MAP.items():
        text = text.replace(uc, repl)
    # Drop any remaining non-latin-1 chars
    return text.encode('latin-1', errors='replace').decode('latin-1')


def compass(deg: float) -> str:
    return DIRS[round(deg / 45) % 8]


class StyledReportPDF(FPDF):
    """Enhanced PDF with modern styling for environmental reports."""

    def __init__(self, location_name: str):
        super().__init__()
        self.location_name = location_name
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() == 1:
            return  # Cover page has no header
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        self.cell(0, 8, _safe(f'Environmental Analysis Report - {self.location_name}'),
                  align='R')
        self.ln(10)

    def footer(self):
        if self.page_no() == 1:
            return
        self.set_y(-15)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

    # ── Cover page ──────────────────────────────────────────
    def cover_page(self, subtitle: str = "", sim_count: int = 0,
                   pin_count: int = 0):
        """Full cover page with accent bar and metadata."""
        # Accent bar at top
        self.set_fill_color(*ACCENT)
        self.rect(0, 0, 210, 6, 'F')

        # Title
        self.set_y(60)
        self.set_font('Helvetica', 'B', 28)
        self.set_text_color(*DARK)
        self.cell(0, 14, 'Environmental', align='C')
        self.ln(14)
        self.cell(0, 14, 'Analysis Report', align='C')
        self.ln(18)

        # Accent line
        self.set_draw_color(*ACCENT)
        self.set_line_width(1.2)
        self.line(60, self.get_y(), 150, self.get_y())
        self.ln(12)

        # Location
        self.set_font('Helvetica', '', 16)
        self.set_text_color(*TEXT)
        self.cell(0, 10, _safe(self.location_name), align='C')
        self.ln(8)

        # Subtitle (e.g. date)
        if subtitle:
            self.set_font('Helvetica', '', 11)
            self.set_text_color(*TEXT_LIGHT)
            self.cell(0, 8, subtitle, align='C')
            self.ln(8)

        # Meta badges
        if sim_count or pin_count:
            self.ln(6)
            self.set_font('Helvetica', '', 10)
            self.set_text_color(*GRAY)
            parts = []
            if sim_count:
                parts.append(f'{sim_count} Simulation{"s" if sim_count != 1 else ""}')
            if pin_count:
                parts.append(f'{pin_count} Sample Pin{"s" if pin_count != 1 else ""}')
            self.cell(0, 8, '  |  '.join(parts), align='C')

        # Bottom accent bar
        self.set_fill_color(*ACCENT)
        self.rect(0, 291, 210, 6, 'F')

    # ── Section title ───────────────────────────────────────
    def section_title(self, title: str):
        """Section header with accent background pill."""
        title = _safe(title)
        self.ln(2)
        self.set_fill_color(*ACCENT)
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(*WHITE)
        w = self.get_string_width(title) + 16
        self.cell(w, 8, f'  {title}', fill=True)
        self.ln(10)

    # ── Body text ───────────────────────────────────────────
    def body_text(self, text: str):
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*TEXT)
        self.multi_cell(0, 5.5, _safe(text))
        self.ln(2)

    # ── Key-value pair ──────────────────────────────────────
    def key_value(self, key: str, value: str):
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*DARK)
        self.cell(50, 6, _safe(key) + ':')
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*TEXT)
        self.cell(0, 6, _safe(value))
        self.ln(6)

    # ── Stat cards (Min / Max / Mean) ───────────────────────
    def stat_cards(self, stats: dict, unit: str):
        """Render 3 inline stat cards for min/max/mean."""
        card_w = 56
        gap = 6
        start_x = (self.w - (card_w * 3 + gap * 2)) / 2
        y = self.get_y()
        card_h = 22

        items = [
            ('Min', stats.get('min', 0), (52, 152, 219)),   # Blue
            ('Mean', stats.get('mean', 0), (76, 201, 240)),  # Accent
            ('Max', stats.get('max', 0), (231, 76, 60)),     # Red
        ]

        for i, (label, val, color) in enumerate(items):
            x = start_x + i * (card_w + gap)
            # Card background
            self.set_fill_color(*LIGHT_BG)
            self.rect(x, y, card_w, card_h, 'F')
            # Left accent stripe
            self.set_fill_color(*color)
            self.rect(x, y, 3, card_h, 'F')

            # Value
            self.set_xy(x + 6, y + 2)
            self.set_font('Helvetica', 'B', 14)
            self.set_text_color(*DARK)
            self.cell(card_w - 8, 10, f'{val:.1f} {unit}')

            # Label
            self.set_xy(x + 6, y + 12)
            self.set_font('Helvetica', '', 8)
            self.set_text_color(*TEXT_LIGHT)
            self.cell(card_w - 8, 6, label)

        self.set_y(y + card_h + 6)

    # ── Styled table ────────────────────────────────────────
    def styled_table(self, headers: list, rows: list,
                     col_widths: list = None):
        """Modern table with accent header and alternating rows."""
        n = len(headers)
        if not col_widths:
            col_widths = [(self.w - 20) / n] * n

        # Header row
        self.set_fill_color(*ACCENT_DARK)
        self.set_font('Helvetica', 'B', 9)
        self.set_text_color(*WHITE)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 8, h, align='C', fill=True)
        self.ln()

        # Data rows
        self.set_font('Helvetica', '', 9)
        for r_idx, row in enumerate(rows):
            if r_idx % 2 == 0:
                self.set_fill_color(*LIGHT_BG)
            else:
                self.set_fill_color(*WHITE)
            self.set_text_color(*TEXT)
            for i, val in enumerate(row):
                self.cell(col_widths[i], 7, str(val), align='C', fill=True)
            self.ln()
        self.ln(4)

    # ── Callout box (for AI blurbs) ─────────────────────────
    def callout_box(self, text: str):
        """Light background callout for AI-generated text."""
        text = _safe(text)
        y = self.get_y()
        self.set_fill_color(*LIGHT_BG)
        # Calculate height needed
        self.set_font('Helvetica', 'I', 9)
        # Use multi_cell dry run to get height
        h = self.multi_cell(self.w - 30, 5, text, dry_run=True,
                            output=MethodReturnValue.HEIGHT)
        self.rect(12, y, self.w - 24, h + 6, 'F')
        # Left accent bar
        self.set_fill_color(*ACCENT)
        self.rect(12, y, 2.5, h + 6, 'F')

        self.set_xy(18, y + 3)
        self.set_text_color(*GRAY)
        self.multi_cell(self.w - 36, 5, text)
        self.ln(4)

    # ── Methodology box ─────────────────────────────────────
    def methodology_box(self, lines: list):
        """Gray background box for methodology section."""
        self.section_title('Methodology')
        y = self.get_y()
        total_h = len(lines) * 5.5 + 10
        self.set_fill_color(*LIGHT_BG)
        self.rect(10, y, self.w - 20, total_h, 'F')
        self.set_xy(14, y + 4)
        self.set_font('Helvetica', '', 9)
        self.set_text_color(*TEXT)
        for line in lines:
            self.cell(0, 5.5, _safe(line))
            self.ln(5.5)
            self.set_x(14)
        self.ln(6)

    # ── Image with border ───────────────────────────────────
    def add_image_b64(self, img_b64: str, max_w: int = 170):
        if not img_b64:
            return
        raw = (img_b64.split(',', 1)[-1]
               if img_b64.startswith('data:') else img_b64)
        buf = io.BytesIO(base64.b64decode(raw))
        x = (self.w - max_w) / 2
        y = self.get_y()
        # Subtle border
        self.set_draw_color(220, 220, 220)
        self.set_line_width(0.5)
        self.rect(x - 1, y - 1, max_w + 2, max_w * 0.6 + 2)
        self.image(buf, x=x, w=max_w)
        self.ln(6)

    # ── Weather bar ─────────────────────────────────────────
    def weather_bar(self, weather: dict):
        """Compact horizontal weather info bar."""
        if not weather:
            return
        t = weather.get('temperature', 20)
        rh = weather.get('humidity', 50)
        ws = weather.get('windSpeed', 3)
        wd = compass(weather.get('windDirection', 0))
        text = f'{t}°C  ·  {rh}% RH  ·  {ws} m/s from {wd}'
        y = self.get_y()
        self.set_fill_color(*LIGHT_BG)
        self.rect(10, y, self.w - 20, 8, 'F')
        self.set_xy(14, y + 1)
        self.set_font('Helvetica', '', 9)
        self.set_text_color(*GRAY)
        self.cell(0, 6, text)
        self.set_y(y + 10)

    # ── Attribution footer ──────────────────────────────────
    def attribution(self):
        self.ln(6)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*TEXT_LIGHT)
        self.cell(0, 4, 'Generated by Environmental Analysis MCP Server',
                  align='C')
        self.ln(4)
        self.cell(0, 4,
                  'Data: OpenStreetMap, Overture Maps, TUM LOD1, Infrared.city',
                  align='C')


def main() -> dict:
    """No-op entry point - this module is imported by report_pdf."""
    return {"status": "ok", "message": "pdf_styles is an importable helper"}
