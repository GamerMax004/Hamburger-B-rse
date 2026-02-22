# ═══════════════════════════════════════════════════════════════════
# ─── IMPORTS ──────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

import discord
from discord import app_commands, ui
from discord.ext import tasks

import json
import asyncio
import random
import math
import io
import time
import logging
import os
import warnings
from datetime import datetime, timezone, timedelta
from typing import Optional
from copy import deepcopy
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib import font_manager
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Matplotlib – Emoji-Warnungen unterdrücken (kein Emoji-Font auf Servern)
matplotlib.use("Agg")
warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")


# Render-Kompatibilität: sicherstellen dass ein geeigneter Font verfügbar ist
for _f in font_manager.findSystemFonts():
    if "dejavu" in _f.lower() or "liberation" in _f.lower():
        break

# HTTP-Server für Render Health-Check (Render erwartet einen offenen Port)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("BörsenBot")

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT A: KONFIGURATION ───────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

BOT_TOKEN             = os.environ.get("BOT_TOKEN")

# Rollen-IDs
KUNDE_ROLLE_ID        = 1411746722022952960
MITARBEITER_ROLLE_ID  = 1467310132387119330
LEITUNG_ROLLE_ID      = 1411748129933496391

# Kanal-IDs
AUSZAHLUNGS_KANAL_ID  = 1474862101020545186
GRAPH_KANAL_ID        = 1474800938907865088
DB_BACKUP_KANAL_ID    = 1474874014680879316

# Rolle die bei Auszahlungsanfragen gepingt wird
AUSZAHLUNGS_ROLLE_ID  = 1474862101020545186

# Handelszeiten (deutsche Zeit)
HANDELS_START = 8    # 08:00 Uhr
HANDELS_ENDE  = 22   # 22:00 Uhr

# Zeitzone für Deutschland
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

BERLIN_TZ = zoneinfo.ZoneInfo("Europe/Berlin")

def get_now():
    """Gibt die aktuelle Zeit in der deutschen Zeitzone zurück (mit Sommerzeit)."""
    return datetime.now(BERLIN_TZ)

# Abgeltungssteuer – fällt beim VERKAUF an (auf realisierten Gewinn)
STEUER_FREIBETRAG = 1000.0
STEUER_SATZ       = 0.25

# Datenbankdatei  (auf Render: /tmp ist persistent per Disk, sonst ephemeral)
DB_DATEI = os.environ.get("DB_PFAD", "boerse_db.json")

GRAPH_UPDATE_DEFAULT = 60

MARKT_DEFAULTS = {
    "handelsgebühr":   0.0025,
    "spread":           0.001,
    "volatilitaet":     0.015,
    "trend":            0.0,
    "min_ordergroesse": 1,
    "max_ordergroesse": 10000,
    "graph_intervall":  GRAPH_UPDATE_DEFAULT,
    "markt_offen":      True,
}

# ─── Kurseinfluss durch Käufe/Verkäufe ───────────────────────────
# Wie stark eine Order den Kurs bewegt, abhängig vom Umlaufanteil.
# Formel: Einfluss = HANDELS_IMPACT_FAKTOR * (menge / max_stueckzahl)
# Beispiel: Kauf von 1% der Aktien -> +0.5% Kursanstieg (bei Faktor 0.5)
HANDELS_IMPACT_FAKTOR = 0.5   # max. Kursveränderung bei 100%-Kauf (50%)
HANDELS_IMPACT_MAX    = 0.05  # Einzelner Trade kann max. 5% bewegen

# ═══════════════════════════════════════════════════════════════════
# ─── RENDER HEALTH-CHECK SERVER ───────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass  # kein Spam im Log

def _starte_health_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        srv = HTTPServer(("0.0.0.0", port), _HealthHandler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        log.info(f"Health-Check Server gestartet auf Port {port}")
    except Exception as e:
        log.warning(f"Health-Server konnte nicht starten: {e}")

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT B: JSON-DATENBANK ──────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

_db: dict = {}
_db_lock = asyncio.Lock()

def _leer_db() -> dict:
    return {
        "markt": dict(MARKT_DEFAULTS),
        "aktien": {},
        "portfolios": {}
    }

def _lade_db() -> dict:
    if os.path.exists(DB_DATEI):
        try:
            with open(DB_DATEI, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in MARKT_DEFAULTS.items():
                data["markt"].setdefault(k, v)
            data["markt"].setdefault("graph_nachricht_id", 0)
            return data
        except Exception as e:
            log.error(f"DB-Ladefehler: {e} – starte mit leerer DB")
    return _leer_db()

def _speichere_db():
    tmp = DB_DATEI + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_DATEI)

async def save_db():
    async with _db_lock:
        await asyncio.get_event_loop().run_in_executor(None, _speichere_db)

def m() -> dict:
    return _db["markt"]

def aktien() -> dict:
    return _db["aktien"]

def portfolios() -> dict:
    return _db["portfolios"]

def get_aktie(sym: str) -> Optional[dict]:
    return aktien().get(sym.upper())

def get_portfolio(uid: str) -> Optional[dict]:
    return portfolios().get(str(uid))

def hat_portfolio(uid) -> bool:
    return str(uid) in portfolios()

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT C: MARKT-ENGINE ────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

def _berechne_neuen_kurs(sym: str, info: dict) -> float:
    """
    Realistische Kurssimulation (gedämpfte GBM + Mean-Reversion).
    - Tages-Vol auf Minuten-Intervall skaliert (480 Handelsminuten/Tag)
    - Maximale Einzelbewegung 0,5% pro Minute (kein Flash-Crash)
    - Sehr seltene kleine Newssprünge (~1x/Tag, max ±2%)
    - Schwache Mean-Reversion zum Tagesstartkurs
    """
    g_vol   = m()["volatilitaet"]
    g_trnd  = m()["trend"]
    a_vol   = info.get("volatilitaet", g_vol)
    eff_vol = max(a_vol, g_vol)

    sigma_min = eff_vol / math.sqrt(480)
    mu_min    = g_trnd * 0.0001

    rand = random.gauss(0, sigma_min)

    start_preis  = info.get("tagesstart_preis", info["preis"])
    abweichung   = (info["preis"] - start_preis) / start_preis if start_preis else 0
    mean_rev     = -0.001 * abweichung

    raw = mu_min + rand + mean_rev
    raw = max(-0.005, min(0.005, raw))  # max ±0,5% pro Minute

    if random.random() < 0.0008:
        news = random.gauss(0, eff_vol * 0.5)
        raw += max(-0.02, min(0.02, news))

    return max(0.01, round(info["preis"] * (1 + raw), 4))


def _wende_handelseinfluss_an(sym: str, info: dict, menge: int, ist_kauf: bool) -> float:
    """
    Berechnet und wendet den Kurseinfluss einer Order an.

    Logik:
    - Kauf  -> Nachfrage steigt -> Kurs steigt
    - Verkauf -> Angebot steigt -> Kurs fällt
    - Stärke abhängig vom Anteil der gehandelten Menge an der Gesamtmenge (max_stueckzahl)
    - Ohne max_stueckzahl: fester Ersatzwert (10.000) als Referenz

    Gibt den neuen Kurs zurück.
    """
    max_st = info.get("max_stueckzahl", 10_000)
    # Anteil der Order an der Gesamtmenge (0.0 – 1.0)
    anteil = menge / max_st if max_st > 0 else 0.0

    # Roheinfluss: proportional zum Anteil, skaliert mit Faktor
    raw_einfluss = HANDELS_IMPACT_FAKTOR * anteil

    # Richtung: Kauf = positiv, Verkauf = negativ
    if not ist_kauf:
        raw_einfluss = -raw_einfluss

    # Kappen auf ±HANDELS_IMPACT_MAX pro Trade
    einfluss = max(-HANDELS_IMPACT_MAX, min(HANDELS_IMPACT_MAX, raw_einfluss))

    neuer_kurs = max(0.01, round(info["preis"] * (1 + einfluss), 4))

    richtung = "↑" if einfluss > 0 else "↓"
    log.info(
        f"[Handelseinfluss] {sym}: {'Kauf' if ist_kauf else 'Verkauf'} {menge} Stück "
        f"({anteil*100:.2f}% von {max_st}) -> {einfluss*100:+.3f}% {richtung} "
        f"| {info['preis']:.4f} -> {neuer_kurs:.4f} EUR"
    )

    return neuer_kurs


async def update_kurse():
    if not m().get("markt_offen", True):
        return
    jetzt = get_now()
    ts    = int(time.time())

    for sym, info in aktien().items():
        if jetzt.hour == HANDELS_START and jetzt.minute == 0:
            info["tagesstart_preis"] = info["preis"]
        info["preis"] = _berechne_neuen_kurs(sym, info)
        info.setdefault("kurshistorie", []).append([ts, info["preis"]])
        if len(info["kurshistorie"]) > 2880:
            info["kurshistorie"] = info["kurshistorie"][-2880:]

    await save_db()

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT D: STEUERSYSTEM ────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

def steuer_auf_gewinn(portfolio: dict, gewinn: float) -> dict:
    """Abgeltungssteuer beim Verkauf auf realisierten Gewinn."""
    jahr = str(get_now().year)
    portfolio.setdefault("steuer_verbraucht", {})
    bereits = portfolio["steuer_verbraucht"].get(jahr, 0.0)

    if gewinn <= 0:
        return {
            "steuerpflichtig": 0.0, "steuer": 0.0,
            "netto_gewinn": round(gewinn, 2),
            "freibetrag_rest": round(max(0.0, STEUER_FREIBETRAG - bereits), 2),
            "neuer_verbrauch": round(bereits, 2),
        }

    frei        = max(0.0, STEUER_FREIBETRAG - bereits)
    steuerpfl   = max(0.0, gewinn - frei)
    steuer      = round(steuerpfl * STEUER_SATZ, 2)
    verbraucht  = round(bereits + min(gewinn, frei), 2)

    return {
        "steuerpflichtig": round(steuerpfl, 2),
        "steuer":          steuer,
        "netto_gewinn":    round(gewinn - steuer, 2),
        "freibetrag_rest": round(frei, 2),
        "neuer_verbrauch": verbraucht,
    }

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT E: BERECHTIGUNGSSYSTEM ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════

def _hat_rolle(interaction: discord.Interaction, *rolle_ids: int) -> bool:
    if not interaction.guild:
        return False
    user_rollen = {r.id for r in interaction.user.roles}
    return bool(user_rollen & set(rolle_ids))

def ist_kunde(i):       return _hat_rolle(i, KUNDE_ROLLE_ID, MITARBEITER_ROLLE_ID, LEITUNG_ROLLE_ID)
def ist_mitarbeiter(i): return _hat_rolle(i, MITARBEITER_ROLLE_ID, LEITUNG_ROLLE_ID)
def ist_leitung(i):     return _hat_rolle(i, LEITUNG_ROLLE_ID)

def keine_rechte_embed(stufe: str) -> discord.Embed:
    return discord.Embed(title="Keine Berechtigung",
                          description=f"Du benötigst die Rolle **{stufe}**.",
                          color=0xE74C3C)

def ist_handelszeit() -> bool:
    jetzt = get_now()
    return HANDELS_START <= jetzt.hour < HANDELS_ENDE

def handelszeit_embed() -> discord.Embed:
    return discord.Embed(
        title="Börse geschlossen!",
        description=f"Sehr geehrter Kunde,\n> der Handel an unserer Börse ist ausschließlich täglich zwischen **{HANDELS_START:02d}:00** und **{HANDELS_ENDE:02d}:00 Uhr** möglich. Außerhalb dieser Zeiten stehen keine Kauf- oder Verkaufsfunktionen zur Verfügung. Wir bitten um Ihr Verständnis und freuen uns, Sie während der offiziellen Handelszeiten wieder begrüßen zu dürfen.\n**Mit freundlichen Grüßen**\n~ Hamburger Börse",
        color=0x0f0f17
    )

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT F: GRAPH-SYSTEM ────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

DARK = "#0f0f17"
PLOT_STYLE = {
    "figure.facecolor": DARK,
    "axes.facecolor":   DARK,
    "axes.edgecolor":   "#2a2a3f",
    "axes.labelcolor":  "#c8c8e0",
    "xtick.color":      "#555577",
    "ytick.color":      "#555577",
    "text.color":       "#c8c8e0",
    "grid.color":       "#1a1a2e",
    "grid.linestyle":   "-",
    "grid.alpha":       1.0,
}

FARBEN = [
    "#89b4fa","#a6e3a1","#f38ba8","#fab387",
    "#cba6f7","#f9e2af","#94e2d5","#89dceb",
    "#b4befe","#eba0ac","#74c7ec","#d9e0ee",
]


def _get_heutigen_tagesbereich():
    jetzt = get_now()
    start = jetzt.replace(hour=HANDELS_START, minute=0, second=0, microsecond=0)
    ende  = jetzt.replace(hour=HANDELS_ENDE,  minute=0, second=0, microsecond=0)
    return start.replace(tzinfo=None), ende.replace(tzinfo=None)


def _filtere_heutige_history(kurshistorie: list) -> list:
    jetzt      = get_now()
    ts_start   = jetzt.replace(hour=HANDELS_START, minute=0, second=0, microsecond=0).timestamp()
    ts_ende    = jetzt.replace(hour=HANDELS_ENDE,  minute=0, second=0, microsecond=0).timestamp()
    return [r for r in kurshistorie if ts_start <= r[0] <= ts_ende]


def _x_achse_konfigurieren(ax, heute_start: datetime, heute_ende: datetime):
    ticks = []
    h = HANDELS_START
    while h <= HANDELS_ENDE:
        ticks.append(heute_start.replace(hour=h, minute=0, second=0, microsecond=0))
        h += 2

    ax.set_xlim(heute_start, heute_ende)
    ax.set_xticks(ticks)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.set_xticklabels([t.strftime("%H:%M") for t in ticks])


def _chart_single(sym: str, info: dict) -> discord.File:
    heute_start, heute_ende = _get_heutigen_tagesbereich()
    hist = _filtere_heutige_history(info.get("kurshistorie", []))

    jetzt_berlin = get_now()
    handelszeit_begonnen = (
        jetzt_berlin.hour > HANDELS_START
        or (jetzt_berlin.hour == HANDELS_START and jetzt_berlin.minute >= 0)
    )

    if len(hist) < 2 and handelszeit_begonnen and ist_handelszeit():
        ts_now = int(time.time())
        ts_start_heute = int(
            jetzt_berlin.replace(hour=HANDELS_START, minute=0, second=0, microsecond=0).timestamp()
        )
        hist = [[ts_start_heute, info["preis"]], [ts_now, info["preis"]]]

    timestamps = [datetime.fromtimestamp(r[0], BERLIN_TZ).replace(tzinfo=None) for r in hist]
    prices     = [r[1] for r in hist]

    kurshistorie_gesamt = info.get("kurshistorie", [])
    if len(kurshistorie_gesamt) >= 2:
        change = (info["preis"] - kurshistorie_gesamt[-2][1]) / kurshistorie_gesamt[-2][1] * 100
    else:
        change = 0.0

    sign    = "+" if change >= 0 else ""
    chg_col = "#a6e3a1" if change >= 0 else "#f38ba8"
    col     = "#89b4fa"

    with matplotlib.rc_context(PLOT_STYLE):
        fig, ax = plt.subplots(figsize=(12, 5), dpi=120)

        if len(prices) >= 2:
            price_min = min(prices)
            ax.fill_between(timestamps, prices, price_min * 0.997, color=col, alpha=0.14)
            ax.plot(timestamps, prices, color=col, linewidth=2.0)
            ax.scatter([timestamps[-1]], [prices[-1]], color=col, s=55, zorder=5)
            ax.annotate(
                f"  {prices[-1]:,.4f}   {sign}{change:.2f}%",
                xy=(timestamps[-1], prices[-1]),
                color=chg_col, fontsize=10, fontweight="bold"
            )
        else:
            ax.text(
                0.5, 0.5,
                f"Handel beginnt um {HANDELS_START:02d}:00 Uhr\nAktueller Kurs: {info['preis']:,.4f} EUR",
                transform=ax.transAxes,
                ha="center", va="center",
                color="#555577", fontsize=11
            )

        fig.suptitle(f"{sym} - {info['name']}", fontsize=14,
                      fontweight="bold", color="#c8c8e0")

    _x_achse_konfigurieren(ax, heute_start, heute_ende)
    plt.setp(ax.get_xticklabels(), rotation=25, ha="right")

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.2f}"))
    ax.grid(True, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout(rect=[0, 0, 1, 0.94])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=DARK)
    plt.close(fig)
    buf.seek(0)

    return discord.File(buf, filename="chart.png")


def _chart_multi() -> Optional[discord.File]:
    alle = list(aktien().items())
    if not alle:
        return None

    heute_start, heute_ende = _get_heutigen_tagesbereich()
    jetzt_berlin = get_now()
    handelszeit_begonnen = (
        jetzt_berlin.hour > HANDELS_START
        or (jetzt_berlin.hour == HANDELS_START and jetzt_berlin.minute >= 0)
    )

    n    = len(alle)
    cols = min(n, 3)
    rows = math.ceil(n / cols)

    with matplotlib.rc_context(PLOT_STYLE):
        fig, axes = plt.subplots(
            rows, cols,
            figsize=(cols * 5.5, rows * 3.8),
            dpi=110, squeeze=False
        )

        for idx, (sym, info) in enumerate(alle):
            r, c = divmod(idx, cols)
            ax   = axes[r][c]

            hist = _filtere_heutige_history(info.get("kurshistorie", []))

            if len(hist) < 2 and handelszeit_begonnen and ist_handelszeit():
                ts_now = int(time.time())
                ts_start_heute = int(
                    jetzt_berlin.replace(hour=HANDELS_START, minute=0, second=0, microsecond=0).timestamp()
                )
                hist = [[ts_start_heute, info["preis"]], [ts_now, info["preis"]]]

            col = FARBEN[idx % len(FARBEN)]

            if len(hist) >= 2:
                prices = [x[1] for x in hist]
                dates  = [datetime.fromtimestamp(x[0], BERLIN_TZ).replace(tzinfo=None) for x in hist]

                p_start = prices[0]
                change  = (prices[-1] - p_start) / p_start * 100 if p_start else 0
                sign    = "+" if change >= 0 else ""
                chg_c   = "#a6e3a1" if change >= 0 else "#f38ba8"

                ax.fill_between(dates, prices, min(prices) * 0.997, color=col, alpha=0.14)
                ax.plot(dates, prices, color=col, linewidth=1.8)

                ax.set_title(
                    f"{sym}  {prices[-1]:,.2f}  {sign}{change:.2f}%",
                    fontsize=9, color=chg_c, pad=3
                )
            else:
                ax.text(
                    0.5, 0.5,
                    f"ab {HANDELS_START:02d}:00 Uhr",
                    transform=ax.transAxes,
                    ha="center", va="center",
                    color="#555577", fontsize=9
                )
                ax.set_title(
                    f"{sym}  {info['preis']:,.2f}",
                    fontsize=9, color="#c8c8e0", pad=3
                )

            _x_achse_konfigurieren(ax, heute_start, heute_ende)
            ax.tick_params(labelsize=7)
            ax.grid(True, alpha=0.5)
            ax.spines[["top", "right"]].set_visible(False)

        for idx in range(n, rows * cols):
            r, c = divmod(idx, cols)
            axes[r][c].set_visible(False)

        fig.suptitle("Börsen Live-Board", fontsize=13,
                      fontweight="bold", color="#c8c8e0")
        plt.tight_layout(rect=[0, 0, 1, 0.95])

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", facecolor=DARK)
        plt.close(fig)
        buf.seek(0)

    return discord.File(buf, filename="chart.png")

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT G: BOT + COMMAND TREE ──────────────────────────────
# ═══════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
client  = discord.Client(intents=intents)
tree    = app_commands.CommandTree(client)

async def _reply(interaction: discord.Interaction, **kwargs):
    try:
        await interaction.response.send_message(**kwargs)
    except discord.InteractionResponded:
        await interaction.followup.send(**kwargs)

def fmt(p: float) -> str:
    return f"{p:,.2f} EUR"

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT H: DROPDOWN-VIEWS ──────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

class AktienAuswahlView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self._baue_select()

    def _baue_select(self):
        self.clear_items()
        alle = list(aktien().items())
        if not alle:
            return

        optionen = [discord.SelectOption(
            label="Gesamtmarkt (alle Aktien)",
            value="__ALL__",
            description="Multi-Chart aller Aktien",
            default=True,
        )]
        for sym, info in alle[:24]:
            hist   = info.get("kurshistorie", [])
            change = (info["preis"] - hist[-2][1]) / hist[-2][1] * 100 if len(hist) >= 2 else 0.0
            sign   = "+" if change >= 0 else ""
            optionen.append(discord.SelectOption(
                label=f"{sym} - {info['name'][:28]}",
                value=sym,
                description=f"{info['preis']:,.2f}   {sign}{change:.2f}%",
            ))

        sel = ui.Select(
            placeholder="Aktie auswählen...",
            options=optionen,
            custom_id="aktien_select_persistent",
        )
        sel.callback = self._select_callback
        self.add_item(sel)

    async def _select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        sym = interaction.data["values"][0]

        if sym == "__ALL__":
            graph = _chart_multi()
            embed = await _baue_markt_embed()
        else:
            info = get_aktie(sym)
            if not info:
                return await interaction.followup.send("Aktie nicht mehr vorhanden.", ephemeral=True)
            graph = _chart_single(sym, info)
            embed = _baue_aktie_embed(sym, info)

        if graph:
            embed.set_image(url="attachment://chart.png")
            await interaction.followup.send(embed=embed, file=graph, ephemeral=True)
        else:
            await interaction.followup.send(embed=embed, ephemeral=True)


class KaufenMengeModal(ui.Modal, title="Aktien kaufen"):
    menge = ui.TextInput(label="Anzahl Aktien", placeholder="z.B. 10", min_length=1, max_length=8)
    def __init__(self, sym: str):
        super().__init__()
        self.sym = sym
    async def on_submit(self, interaction: discord.Interaction):
        try:
            m_val = int(self.menge.value)
        except ValueError:
            return await interaction.response.send_message("Ungültige Menge.", ephemeral=True)
        await _do_kaufen(interaction, self.sym, m_val)


class VerkaufenMengeModal(ui.Modal, title="Aktien verkaufen"):
    menge = ui.TextInput(label="Anzahl Aktien", placeholder="z.B. 5", min_length=1, max_length=8)
    def __init__(self, sym: str):
        super().__init__()
        self.sym = sym
    async def on_submit(self, interaction: discord.Interaction):
        try:
            m_val = int(self.menge.value)
        except ValueError:
            return await interaction.response.send_message("Ungültige Menge.", ephemeral=True)
        await _do_verkaufen(interaction, self.sym, m_val)


def _aktien_select_optionen(nur_mit_bestand: Optional[str] = None) -> list:
    alle = list(aktien().items())
    if nur_mit_bestand:
        pf = get_portfolio(nur_mit_bestand)
        pos = pf.get("positionen", {}) if pf else {}
        alle = [(s, i) for s, i in alle if pos.get(s, {}).get("menge", 0) > 0]
    optionen = []
    for sym, info in alle[:25]:
        hist   = info.get("kurshistorie", [])
        change = (info["preis"] - hist[-2][1]) / hist[-2][1] * 100 if len(hist) >= 2 else 0.0
        sign   = "+" if change >= 0 else ""
        optionen.append(discord.SelectOption(
            label=f"{sym} - {info['name'][:28]}",
            value=sym,
            description=f"{info['preis']:,.4f}   {sign}{change:.2f}%",
        ))
    return optionen


class KaufenSelectView(ui.View):
    def __init__(self, uid: str):
        super().__init__(timeout=60)
        self._uid = uid
        opts = _aktien_select_optionen()
        if not opts:
            return
        sel = ui.Select(placeholder="Aktie zum Kaufen wählen...", options=opts)
        sel.callback = self._cb
        self.add_item(sel)

    async def _cb(self, interaction: discord.Interaction):
        sym = interaction.data["values"][0]
        await interaction.response.send_modal(KaufenMengeModal(sym))


class VerkaufenSelectView(ui.View):
    def __init__(self, uid: str):
        super().__init__(timeout=60)
        self._uid = uid
        opts = _aktien_select_optionen(nur_mit_bestand=uid)
        if not opts:
            return
        sel = ui.Select(placeholder="Aktie zum Verkaufen wählen...", options=opts)
        sel.callback = self._cb
        self.add_item(sel)

    async def _cb(self, interaction: discord.Interaction):
        sym = interaction.data["values"][0]
        await interaction.response.send_modal(VerkaufenMengeModal(sym))


# ─── Auszahlungs-Flow (mit Pflicht-Beleg-Link) ────────────────────

class AuszahlungLinkModal(ui.Modal, title="Auszahlung bestätigen"):
    nachricht_link = ui.TextInput(
        label="Link zur Zahlungsnachricht / Buchungsbeleg",
        placeholder="https://... (Discord-Nachricht, PayPal-Beleg, etc.)",
        style=discord.TextStyle.short,
        min_length=10, max_length=500, required=True,
    )

    def __init__(self, view_ref: "AuszahlungBestätigenView"):
        super().__init__()
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        link = self.nachricht_link.value.strip()
        pf   = get_portfolio(self.view_ref.user_id)
        if not pf:
            return await interaction.response.send_message("Portfolio nicht mehr vorhanden.", ephemeral=True)
        if pf["guthaben"] < self.view_ref.betrag:
            return await interaction.response.send_message("Guthaben reicht nicht mehr aus.", ephemeral=True)

        pf["guthaben"] -= self.view_ref.betrag
        pf.setdefault("transaktionen", []).append({
            "typ":            "AUSZAHLUNG",
            "brutto":         self.view_ref.betrag,
            "steuer":         0.0,
            "netto":          self.view_ref.betrag,
            "beleg_link":     link,
            "bestätigt_von": str(interaction.user.id),
            "timestamp":      int(time.time()),
        })
        await save_db()

        embed = discord.Embed(title="Auszahlung bestätigt", color=0x2ECC71)
        embed.add_field(name="Nutzer",         value=f"<@{self.view_ref.user_id}>", inline=True)
        embed.add_field(name="Betrag",         value=fmt(self.view_ref.betrag),      inline=True)
        embed.add_field(name="Bestätigt von", value=interaction.user.mention,       inline=True)
        embed.add_field(name="Beleg / Link",   value=link,                           inline=False)

        try:
            await interaction.message.edit(embed=embed, view=None)
        except Exception:
            pass

        try:
            member = await interaction.guild.fetch_member(int(self.view_ref.user_id))
            dm = discord.Embed(title="Auszahlung bestätigt", color=0x2ECC71)
            dm.add_field(name="Betrag",      value=fmt(self.view_ref.betrag))
            dm.add_field(name="Beleg / Link", value=link, inline=False)
            await member.send(embed=dm)
        except Exception:
            pass

        await interaction.response.send_message("Auszahlung erfolgreich bestätigt.", ephemeral=True)


class AuszahlungBestätigenView(ui.View):
    def __init__(self, user_id: str, betrag: float, netto: float, steuer: float):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.betrag  = betrag
        self.netto   = netto
        self.steuer  = steuer

    @ui.button(label="Auszahlung bestätigen", style=discord.ButtonStyle.green, custom_id="auszahlung_bestätigen")
    async def bestätigen(self, interaction: discord.Interaction, button: ui.Button):
        if not ist_mitarbeiter(interaction):
            return await interaction.response.send_message("Keine Berechtigung.", ephemeral=True)
        await interaction.response.send_modal(AuszahlungLinkModal(view_ref=self))

    @ui.button(label="Ablehnen", style=discord.ButtonStyle.red, custom_id="auszahlung_ablehnen")
    async def ablehnen(self, interaction: discord.Interaction, button: ui.Button):
        if not ist_mitarbeiter(interaction):
            return await interaction.response.send_message("Keine Berechtigung.", ephemeral=True)
        embed = discord.Embed(title="Auszahlung abgelehnt", color=0xE74C3C)
        embed.add_field(name="Nutzer",    value=f"<@{self.user_id}>")
        embed.add_field(name="Betrag",    value=fmt(self.betrag))
        embed.add_field(name="Abgelehnt", value=interaction.user.mention)
        await interaction.response.edit_message(embed=embed, view=None)

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT I: EMBED-BUILDER ───────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

async def _baue_markt_embed() -> discord.Embed:
    alle = list(aktien().items())
    embed = discord.Embed(title="Börsen Live-Board", color=0x5865F2,
                           timestamp=datetime.now(timezone.utc))
    if not alle:
        embed.description = "*(Noch keine Aktien im Markt)*"
    else:
        lines = []
        for sym, info in alle:
            hist   = info.get("kurshistorie", [])
            change = (info["preis"] - hist[-2][1]) / hist[-2][1] * 100 if len(hist) >= 2 else 0.0
            sign   = "+" if change >= 0 else ""
            # Umlauf-Info anzeigen wenn vorhanden
            umlauf_str = ""
            max_st = info.get("max_stueckzahl")
            if max_st:
                umlauf = info.get("umlauf", 0)
                umlauf_str = f"  `{umlauf:,}/{max_st:,}`"
            lines.append(f"**`{sym:<5}`**  `{info['preis']:>10,.4f} EUR`  `{sign}{change:>7.2f}%`{umlauf_str}")
        embed.description = "\n".join(lines)

    m_val = m()
    offen  = m_val.get("markt_offen", True)
    jetzt  = get_now()
    zeit_ok = HANDELS_START <= jetzt.hour < HANDELS_ENDE
    status = "Geöffnet" if (offen and zeit_ok) else "Geschlossen"
    embed.add_field(name="Status",       value=status,                               inline=True)
    embed.add_field(name="Gebühr",      value=f"{m_val['handelsgebühr']*100:.2f}%",   inline=True)
    embed.add_field(name="Volatilität", value=f"{m_val['volatilitaet']*100:.2f}%",     inline=True)
    embed.add_field(name="Handelszeiten",
                     value=f"{HANDELS_START:02d}:00 - {HANDELS_ENDE:02d}:00 Uhr",   inline=True)
    embed.set_footer(text=f"Aktualisiert alle {m_val.get('graph_intervall', 60)}s  |  /aktien_kaufen  /aktien_verkaufen  /portfolio")
    embed.set_image(url="attachment://chart.png")
    return embed


def _baue_aktie_embed(sym: str, info: dict) -> discord.Embed:
    hist   = info.get("kurshistorie", [])
    change = (info["preis"] - hist[-2][1]) / hist[-2][1] * 100 if len(hist) >= 2 else 0.0
    sign   = "+" if change >= 0 else ""
    color  = 0x2ECC71 if change >= 0 else 0xE74C3C
    embed  = discord.Embed(title=f"{sym} - {info['name']}", color=color,
                            timestamp=get_now())
    embed.add_field(name="Kurs",        value=f"**{info['preis']:,.4f} EUR**",              inline=True)
    embed.add_field(name="Änderung",   value=f"{sign}{change:.2f}%",                   inline=True)
    embed.add_field(name="Volatilität",value=f"{info.get('volatilitaet', 0)*100:.2f}%",inline=True)

    # Umlauf / Verfügbarkeit anzeigen
    max_st = info.get("max_stueckzahl")
    if max_st:
        umlauf    = info.get("umlauf", 0)
        verfügbar = max_st - umlauf
        embed.add_field(
            name="Umlauf / Verfügbar",
            value=f"`{umlauf:,}` verkauft  |  `{verfügbar:,}` verfügbar  |  `{max_st:,}` gesamt",
            inline=False
        )

    heute_hist = _filtere_heutige_history(hist)
    if len(heute_hist) >= 2:
        embed.add_field(name="Tages-Hoch", value=f"{max(h[1] for h in heute_hist):,.4f} EUR", inline=True)
        embed.add_field(name="Tages-Tief", value=f"{min(h[1] for h in heute_hist):,.4f} EUR", inline=True)
    embed.set_image(url="attachment://chart.png")
    return embed

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT J: PERSISTENTER GRAPH ──────────────────────────────
# ═══════════════════════════════════════════════════════════════════

_graph_view_instance = None

async def graph_aktualisieren():
    global _graph_view_instance
    kanal = client.get_channel(GRAPH_KANAL_ID)
    if not kanal:
        log.warning("GRAPH_KANAL_ID nicht gefunden.")
        return

    graph = _chart_multi()
    embed = await _baue_markt_embed()
    _graph_view_instance = AktienAuswahlView()

    alte_id = m().get("graph_nachricht_id", 0)
    if alte_id:
        try:
            alte = await kanal.fetch_message(alte_id)
            await alte.delete()
        except (discord.NotFound, discord.Forbidden):
            pass

    try:
        if graph:
            neue = await kanal.send(embed=embed, file=graph, view=_graph_view_instance)
        else:
            neue = await kanal.send(embed=embed, view=_graph_view_instance)
        m()["graph_nachricht_id"] = neue.id
        await save_db()
        log.info(f"Graph aktualisiert (msg_id={neue.id})")
    except Exception as e:
        log.error(f"Graph-Nachricht Fehler: {e}")

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT K: DB-BACKUP SYSTEM ────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

async def db_backup_senden():
    kanal = client.get_channel(DB_BACKUP_KANAL_ID)
    if not kanal:
        log.warning("DB_BACKUP_KANAL_ID nicht gefunden.")
        return

    try:
        db_json = json.dumps(_db, ensure_ascii=False, indent=2)
        buf     = io.BytesIO(db_json.encode("utf-8"))
        ts_str  = get_now().strftime("%Y-%m-%d_%H-%M")
        datei   = discord.File(buf, filename=f"börse_db_{ts_str}.json")

        n_aktien  = len(aktien())
        n_pf      = len(portfolios())
        embed = discord.Embed(
            title="Automatisches DB-Backup",
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Aktien",     value=str(n_aktien), inline=True)
        embed.add_field(name="Portfolios", value=str(n_pf),     inline=True)
        embed.add_field(name="Datei",      value=f"`börse_db_{ts_str}.json`", inline=False)
        embed.set_footer(text="Wiederherstellen: /db_laden (Leitungsebene)")

        await kanal.send(embed=embed, file=datei)
        log.info(f"DB-Backup gesendet ({n_aktien} Aktien, {n_pf} Portfolios)")
    except Exception as e:
        log.error(f"DB-Backup Fehler: {e}")

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT K2: HANDELSFUNKTIONEN ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════

async def _do_kaufen(interaction: discord.Interaction, sym: str, menge: int):
    uid = str(interaction.user.id)
    if not ist_handelszeit():
        return await _reply(interaction, embed=handelszeit_embed(), ephemeral=True)
    if not m().get("markt_offen", True):
        return await _reply(interaction, content="Der Markt ist manuell geschlossen.", ephemeral=True)
    if not hat_portfolio(uid):
        return await _reply(interaction, content="Du hast kein Portfolio. Bitte einen Mitarbeiter.", ephemeral=True)

    mp = m()
    if not (mp["min_ordergroesse"] <= menge <= mp["max_ordergroesse"]):
        return await _reply(interaction,
            content=f"Menge muss zwischen {mp['min_ordergroesse']} und {mp['max_ordergroesse']:,} liegen.",
            ephemeral=True)

    info = get_aktie(sym)
    if not info:
        return await _reply(interaction, content=f"Aktie `{sym}` nicht gefunden.", ephemeral=True)

    # ── Stückzahl-Limit prüfen ────────────────────────────────────
    max_st = info.get("max_stueckzahl")
    if max_st is not None:
        umlauf    = info.get("umlauf", 0)
        verfügbar = max_st - umlauf
        if menge > verfügbar:
            return await _reply(
                interaction,
                content=(
                    f"Nicht genug Aktien verfügbar!\n"
                    f"Du möchtest **{menge:,}** kaufen, aber nur **{verfügbar:,}** von **{max_st:,}** sind noch verfügbar."
                ),
                ephemeral=True
            )

    kaufkurs = info["preis"] * (1 + mp["spread"] / 2)
    gebühr  = kaufkurs * menge * mp["handelsgebühr"]
    gesamt   = kaufkurs * menge + gebühr

    pf = get_portfolio(uid)
    if pf["guthaben"] < gesamt:
        return await _reply(interaction,
            content=f"Guthaben reicht nicht.\nBenötigt: **{fmt(gesamt)}** | Vorhanden: **{fmt(pf['guthaben'])}**",
            ephemeral=True)

    pf["guthaben"] -= gesamt
    pos = pf.setdefault("positionen", {}).setdefault(sym, {"menge": 0, "kaufkurs_avg": 0.0})
    alter_wert   = pos["menge"] * pos["kaufkurs_avg"]
    pos["menge"] += menge
    pos["kaufkurs_avg"] = (alter_wert + kaufkurs * menge) / pos["menge"]

    # ── Umlauf erhöhen ────────────────────────────────────────────
    if max_st is not None:
        info["umlauf"] = info.get("umlauf", 0) + menge

    # ── Kurseinfluss durch Kauf anwenden (Kurs steigt) ────────────
    alter_preis    = info["preis"]
    info["preis"]  = _wende_handelseinfluss_an(sym, info, menge, ist_kauf=True)
    ts = int(time.time())
    info.setdefault("kurshistorie", []).append([ts, info["preis"]])
    if len(info["kurshistorie"]) > 2880:
        info["kurshistorie"] = info["kurshistorie"][-2880:]

    pf.setdefault("transaktionen", []).append({
        "typ": "KAUF", "symbol": sym, "menge": menge,
        "kurs": round(kaufkurs, 4), "gebühr": round(gebühr, 4),
        "gesamt": round(gesamt, 4), "timestamp": ts,
        "kurseinfluss": round(info["preis"] - alter_preis, 4),
    })
    await save_db()

    kursänderung = info["preis"] - alter_preis
    sign_k = "+" if kursänderung >= 0 else ""

    embed = discord.Embed(title="Kauf erfolgreich", color=0x2ECC71)
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    embed.add_field(name="Aktie",        value=f"**{sym}** - {info['name']}", inline=False)
    embed.add_field(name="Menge",        value=f"{menge:,}",                  inline=True)
    embed.add_field(name="Kaufkurs",     value=f"{kaufkurs:,.4f} EUR",        inline=True)
    embed.add_field(name="Gebühr",      value=fmt(gebühr),                   inline=True)
    embed.add_field(name="Gesamt",       value=f"**{fmt(gesamt)}**",           inline=True)
    embed.add_field(name="Guthaben",     value=fmt(pf["guthaben"]),            inline=True)
    embed.add_field(
        name="Kurseinfluss",
        value=f"`{sign_k}{kursänderung:+.4f} EUR` → neuer Kurs: `{info['preis']:,.4f} EUR`",
        inline=False
    )
    if max_st is not None:
        embed.add_field(
            name="Umlauf",
            value=f"`{info.get('umlauf', 0):,}` / `{max_st:,}` Aktien im Umlauf",
            inline=False
        )
    await _reply(interaction, embed=embed, ephemeral=True)

    asyncio.create_task(graph_aktualisieren())


async def _do_verkaufen(interaction: discord.Interaction, sym: str, menge: int):
    """Steuer wird hier beim Verkauf auf den realisierten Gewinn berechnet."""
    uid = str(interaction.user.id)
    if not ist_handelszeit():
        return await _reply(interaction, embed=handelszeit_embed(), ephemeral=True)
    if not m().get("markt_offen", True):
        return await _reply(interaction, content="Der Markt ist manuell geschlossen.", ephemeral=True)
    if not hat_portfolio(uid):
        return await _reply(interaction, content="Du hast kein Portfolio.", ephemeral=True)

    mp  = m()
    pf  = get_portfolio(uid)
    pos = pf.get("positionen", {}).get(sym, {})
    if pos.get("menge", 0) < menge:
        return await _reply(interaction,
            content=f"Du besitzt nur **{pos.get('menge', 0):,}** Aktien von {sym}.",
            ephemeral=True)

    info = get_aktie(sym)
    if not info:
        return await _reply(interaction, content=f"Aktie `{sym}` nicht gefunden.", ephemeral=True)

    verkaufkurs   = info["preis"] * (1 - mp["spread"] / 2)
    gebühr       = verkaufkurs * menge * mp["handelsgebühr"]
    brutto_erlös = verkaufkurs * menge - gebühr
    gewinn_brutto = (verkaufkurs - pos["kaufkurs_avg"]) * menge - gebühr

    steuer_info   = steuer_auf_gewinn(pf, gewinn_brutto)
    steuer_betrag = steuer_info["steuer"]
    netto_erlös  = brutto_erlös - steuer_betrag

    jahr = str(get_now().year)
    pf.setdefault("steuer_verbraucht", {})[jahr] = steuer_info["neuer_verbrauch"]
    pf["guthaben"] += netto_erlös
    pos["menge"]   -= menge
    if pos["menge"] == 0:
        del pf["positionen"][sym]

    # ── Umlauf verringern ─────────────────────────────────────────
    max_st = info.get("max_stueckzahl")
    if max_st is not None:
        info["umlauf"] = max(0, info.get("umlauf", 0) - menge)

    # ── Kurseinfluss durch Verkauf anwenden (Kurs fällt) ──────────
    alter_preis   = info["preis"]
    info["preis"] = _wende_handelseinfluss_an(sym, info, menge, ist_kauf=False)
    ts = int(time.time())
    info.setdefault("kurshistorie", []).append([ts, info["preis"]])
    if len(info["kurshistorie"]) > 2880:
        info["kurshistorie"] = info["kurshistorie"][-2880:]

    pf.setdefault("transaktionen", []).append({
        "typ": "VERKAUF", "symbol": sym, "menge": menge,
        "kurs": round(verkaufkurs, 4), "gebühr": round(gebühr, 4),
        "brutto_erlös": round(brutto_erlös, 4),
        "gewinn_brutto": round(gewinn_brutto, 4),
        "steuer": round(steuer_betrag, 4),
        "netto_erlös": round(netto_erlös, 4),
        "kurseinfluss": round(info["preis"] - alter_preis, 4),
        "timestamp": ts,
    })
    await save_db()

    kursänderung = info["preis"] - alter_preis
    color = 0x2ECC71 if gewinn_brutto >= 0 else 0xE74C3C
    sign  = "+" if gewinn_brutto >= 0 else ""
    embed = discord.Embed(title="Verkauf erfolgreich", color=color)
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    embed.add_field(name="Aktie",         value=f"**{sym}** - {info['name']}", inline=False)
    embed.add_field(name="Menge",         value=f"{menge:,}",                  inline=True)
    embed.add_field(name="Verkaufskurs",  value=f"{verkaufkurs:,.4f} EUR",      inline=True)
    embed.add_field(name="Gebühr",       value=fmt(gebühr),                   inline=True)
    embed.add_field(name="Brutto-Erlös", value=fmt(brutto_erlös),             inline=True)
    embed.add_field(name="Gewinn/Verlust",value=f"{sign}{fmt(gewinn_brutto)}",  inline=True)

    if steuer_betrag > 0:
        frei_danach = max(0.0, STEUER_FREIBETRAG - steuer_info["neuer_verbrauch"])
        embed.add_field(
            name="Abgeltungssteuer (25%)",
            value=f"-{fmt(steuer_betrag)}\n(Freibetrag danach: {fmt(frei_danach)})",
            inline=False
        )
    else:
        frei_rest = steuer_info["freibetrag_rest"] - max(0.0, gewinn_brutto)
        embed.add_field(
            name="Steuer",
            value=f"Keine (Freibetrag verbleibend: {fmt(max(0.0, frei_rest))})",
            inline=False
        )

    embed.add_field(name="Netto-Erlös", value=f"**{fmt(netto_erlös)}**", inline=True)
    embed.add_field(name="Guthaben",     value=fmt(pf["guthaben"]),        inline=True)
    embed.add_field(
        name="Kurseinfluss",
        value=f"`{kursänderung:+.4f} EUR` → neuer Kurs: `{info['preis']:,.4f} EUR`",
        inline=False
    )
    if max_st is not None:
        embed.add_field(
            name="Umlauf",
            value=f"`{info.get('umlauf', 0):,}` / `{max_st:,}` Aktien im Umlauf",
            inline=False
        )
    await _reply(interaction, embed=embed, ephemeral=True)

    asyncio.create_task(graph_aktualisieren())

# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT L: SLASH COMMANDS ──────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

@tree.command(name="portfolio", description="Portfolio anzeigen")
@app_commands.describe(nutzer="[Nur Mitarbeiter] Portfolio eines anderen Nutzers")
async def cmd_portfolio(interaction: discord.Interaction, nutzer: Optional[discord.Member] = None):
    await interaction.response.defer(ephemeral=True)
    if nutzer and not ist_mitarbeiter(interaction):
        return await interaction.followup.send(embed=keine_rechte_embed("Mitarbeiter"), ephemeral=True)

    ziel_uid  = str(nutzer.id if nutzer else interaction.user.id)
    ziel_name = (nutzer or interaction.user).display_name
    pf        = get_portfolio(ziel_uid)
    if not pf:
        return await interaction.followup.send("Kein Portfolio vorhanden.", ephemeral=True)

    embed = discord.Embed(title=f"Portfolio - {ziel_name}", color=0x5865F2,
                           timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Guthaben", value=fmt(pf["guthaben"]), inline=True)

    depot_wert = 0.0
    positionen = pf.get("positionen", {})
    if positionen:
        lines = []
        for sym, pos in positionen.items():
            info = get_aktie(sym)
            if not info:
                continue
            wert  = info["preis"] * pos["menge"]
            pnl   = (info["preis"] - pos["kaufkurs_avg"]) * pos["menge"]
            pnl_p = (info["preis"] - pos["kaufkurs_avg"]) / pos["kaufkurs_avg"] * 100 if pos["kaufkurs_avg"] else 0
            depot_wert += wert
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"**{sym}** x{pos['menge']:,}  |  Kauf-Ø {pos['kaufkurs_avg']:,.4f} -> {info['preis']:,.4f} EUR"
                f"\n  Wert: `{fmt(wert)}`  P&L: `{sign}{fmt(pnl)} ({sign}{pnl_p:.1f}%)`"
            )
        embed.add_field(name="Positionen", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="Positionen", value="Keine offenen Positionen.", inline=False)

    embed.add_field(name="Depotwert",      value=fmt(depot_wert),                  inline=True)
    embed.add_field(name="Gesamtvermögen",value=fmt(pf["guthaben"] + depot_wert), inline=True)

    jahr = str(get_now().year)
    verbraucht = pf.get("steuer_verbraucht", {}).get(jahr, 0.0)
    embed.add_field(
        name=f"Abgeltungssteuer-Freibetrag {jahr}",
        value=f"Verbraucht (Verkaufe): {fmt(verbraucht)} / {fmt(STEUER_FREIBETRAG)}\n"
              f"Verbleibend: **{fmt(max(0.0, STEUER_FREIBETRAG - verbraucht))}**",
        inline=False
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="aktien_kaufen", description="Aktien kaufen (nur während Handelszeiten)")
async def cmd_aktien_kaufen(interaction: discord.Interaction):
    if not ist_kunde(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Kunde"), ephemeral=True)
    if not ist_handelszeit():
        return await interaction.response.send_message(embed=handelszeit_embed(), ephemeral=True)
    if not hat_portfolio(str(interaction.user.id)):
        return await interaction.response.send_message("Du hast kein Portfolio. Bitte wende dich an einen Mitarbeiter.", ephemeral=True)
    if not aktien():
        return await interaction.response.send_message("Keine Aktien verfügbar.", ephemeral=True)
    opts = _aktien_select_optionen()
    if not opts:
        return await interaction.response.send_message("Keine Aktien vorhanden.", ephemeral=True)
    await interaction.response.send_message(
        "<:490209buybutton:1474867799393435648> Welche Aktie möchtest du kaufen?",
        view=KaufenSelectView(str(interaction.user.id)), ephemeral=True
    )


@tree.command(name="aktien_verkaufen", description="Aktien verkaufen (nur während Handelszeiten)")
async def cmd_aktien_verkaufen(interaction: discord.Interaction):
    if not ist_kunde(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Kunde"), ephemeral=True)
    if not ist_handelszeit():
        return await interaction.response.send_message(embed=handelszeit_embed(), ephemeral=True)
    uid = str(interaction.user.id)
    if not hat_portfolio(uid):
        return await interaction.response.send_message("Du hast kein Portfolio.", ephemeral=True)
    pf = get_portfolio(uid)
    if not pf.get("positionen"):
        return await interaction.response.send_message("Du hast keine Aktien im Depot.", ephemeral=True)
    await interaction.response.send_message(
        "<:730336sellbutton:1474867813452746822> Welche Aktie möchtest du verkaufen?",
        view=VerkaufenSelectView(uid), ephemeral=True
    )


@tree.command(name="portfolio_erstellen", description="[Mitarbeiter] Portfolio für einen Kunden erstellen")
@app_commands.describe(kunde="Der Kunde", startguthaben="Anfangsguthaben in EUR")
async def cmd_portfolio_erstellen(interaction: discord.Interaction,
                                   kunde: discord.Member, startguthaben: float = 0.0):
    if not ist_mitarbeiter(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Mitarbeiter"), ephemeral=True)
    uid = str(kunde.id)
    if hat_portfolio(uid):
        return await interaction.response.send_message(f"{kunde.mention} hat bereits ein Portfolio.", ephemeral=True)
    if startguthaben < 0:
        return await interaction.response.send_message("Startguthaben muss >= 0 sein.", ephemeral=True)

    portfolios()[uid] = {
        "erstellt_von": str(interaction.user.id), "erstellt_am": int(time.time()),
        "guthaben": startguthaben, "positionen": {}, "transaktionen": [],
        "steuer_verbraucht": {},
    }
    await save_db()

    embed = discord.Embed(title="Portfolio erstellt", color=0x2ECC71)
    embed.add_field(name="Kunde",         value=kunde.mention,           inline=True)
    embed.add_field(name="Startguthaben", value=fmt(startguthaben),       inline=True)
    embed.add_field(name="Erstellt von",  value=interaction.user.mention, inline=True)
    await interaction.response.send_message(embed=embed)


@tree.command(name="guthaben_aufladen", description="[Mitarbeiter] Guthaben einzahlen")
@app_commands.describe(kunde="Der Kunde", betrag="Betrag in EUR")
async def cmd_guthaben_aufladen(interaction: discord.Interaction,
                                 kunde: discord.Member, betrag: float):
    if not ist_mitarbeiter(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Mitarbeiter"), ephemeral=True)
    uid = str(kunde.id)
    pf  = get_portfolio(uid)
    if not pf:
        return await interaction.response.send_message(f"{kunde.mention} hat kein Portfolio.", ephemeral=True)
    if betrag <= 0:
        return await interaction.response.send_message("Betrag muss > 0 sein.", ephemeral=True)

    pf["guthaben"] += betrag
    pf.setdefault("transaktionen", []).append({
        "typ": "EINZAHLUNG", "betrag": betrag,
        "von": str(interaction.user.id), "timestamp": int(time.time()),
    })
    await save_db()

    embed = discord.Embed(title="Guthaben aufgeladen", color=0x2ECC71)
    embed.add_field(name="Kunde",         value=kunde.mention,           inline=True)
    embed.add_field(name="Einzahlung",    value=fmt(betrag),              inline=True)
    embed.add_field(name="Neues Guthaben",value=fmt(pf["guthaben"]),     inline=True)
    embed.add_field(name="Mitarbeiter",   value=interaction.user.mention, inline=True)
    await interaction.response.send_message(embed=embed)


@tree.command(name="guthaben_auszahlen", description="[Mitarbeiter] Auszahlungsanfrage stellen")
@app_commands.describe(kunde="Der Kunde", betrag="Betrag in EUR")
async def cmd_guthaben_auszahlen(interaction: discord.Interaction,
                                  kunde: discord.Member, betrag: float):
    if not ist_mitarbeiter(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Mitarbeiter"), ephemeral=True)
    uid = str(kunde.id)
    pf  = get_portfolio(uid)
    if not pf:
        return await interaction.response.send_message(f"{kunde.mention} hat kein Portfolio.", ephemeral=True)
    if betrag <= 0:
        return await interaction.response.send_message("Betrag muss > 0 sein.", ephemeral=True)
    if pf["guthaben"] < betrag:
        return await interaction.response.send_message(
            f"Guthaben reicht nicht.\nVorhanden: **{fmt(pf['guthaben'])}**  |  Angefragt: **{fmt(betrag)}**",
            ephemeral=True)

    auszahlungs_kanal = client.get_channel(AUSZAHLUNGS_KANAL_ID)
    if not auszahlungs_kanal:
        return await interaction.response.send_message("Auszahlungskanal nicht gefunden.", ephemeral=True)

    embed = discord.Embed(title="Auszahlungsanfrage", color=0xE67E22,
                           timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Kunde",         value=kunde.mention,            inline=True)
    embed.add_field(name="Angefragt von", value=interaction.user.mention, inline=True)
    embed.add_field(name="Betrag",        value=fmt(betrag),               inline=True)
    embed.add_field(name="Steuerhinweis",
                     value="Steuer wurde bereits beim Aktienverkauf abgezogen.", inline=False)
    embed.set_footer(text=f"Aktuelles Guthaben: {fmt(pf['guthaben'])}")

    view = AuszahlungBestätigenView(user_id=uid, betrag=betrag, netto=betrag, steuer=0.0)
    await auszahlungs_kanal.send(
        content=f"<@&{AUSZAHLUNGS_ROLLE_ID}> - neue Auszahlungsanfrage für {kunde.mention}",
        embed=embed, view=view
    )
    await interaction.response.send_message(
        f"Auszahlungsanfrage ueber **{fmt(betrag)}** wurde gestellt.", ephemeral=True
    )


@tree.command(name="portfolio_loeschen", description="[Leitungsebene] Portfolio eines Kunden löschen")
@app_commands.describe(kunde="Kunde")
async def cmd_portfolio_löschen(interaction: discord.Interaction, kunde: discord.Member):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    uid = str(kunde.id)
    if not hat_portfolio(uid):
        return await interaction.response.send_message(f"{kunde.mention} hat kein Portfolio.", ephemeral=True)
    pf = portfolios().pop(uid)
    await save_db()
    embed = discord.Embed(title="Portfolio gelöscht", color=0xE74C3C)
    embed.add_field(name="Kunde",       value=kunde.mention,                 inline=True)
    embed.add_field(name="Guthaben",    value=fmt(pf.get("guthaben", 0)),    inline=True)
    embed.add_field(name="Gelöscht",   value=interaction.user.mention,      inline=True)
    await interaction.response.send_message(embed=embed)


@tree.command(name="aktie_hinzufuegen", description="[Leitungsebene] Neue Aktie hinzufügen")
@app_commands.describe(
    symbol="Kürzel (max. 6 Zeichen)",
    name="Vollständiger Name",
    startpreis="Startkurs in EUR",
    volatilitaet="Tägl. Volatilität in % (Standard: 1.5)",
    max_stueckzahl="Maximale Gesamtanzahl an Aktien (0 = unbegrenzt)"
)
async def cmd_aktie_hinzufügen(interaction: discord.Interaction,
                                 symbol: str, name: str,
                                 startpreis: float, volatilitaet: float = 1.5,
                                 max_stueckzahl: int = 0):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    sym = symbol.upper()[:6].strip()
    if not sym.isalpha():
        return await interaction.response.send_message("Symbol darf nur Buchstaben enthalten.", ephemeral=True)
    if get_aktie(sym):
        return await interaction.response.send_message(f"Aktie `{sym}` existiert bereits.", ephemeral=True)
    if startpreis <= 0:
        return await interaction.response.send_message("Startpreis muss > 0 sein.", ephemeral=True)
    if max_stueckzahl < 0:
        return await interaction.response.send_message("Maximale Stückzahl muss >= 0 sein.", ephemeral=True)

    vol = max(0.001, min(1.0, volatilitaet / 100))

    aktien_eintrag = {
        "name": name, "preis": round(startpreis, 4),
        "volatilitaet": vol, "tagesstart_preis": round(startpreis, 4),
        "kurshistorie": [[int(time.time()), round(startpreis, 4)]],
        "umlauf": 0,  # Anzahl der aktuell im Umlauf befindlichen Aktien
    }

    # Nur setzen wenn ein Limit gewünscht ist
    if max_stueckzahl > 0:
        aktien_eintrag["max_stueckzahl"] = max_stueckzahl

    aktien()[sym] = aktien_eintrag
    await save_db()

    embed = discord.Embed(title="Aktie hinzugefügt", color=0x2ECC71)
    embed.add_field(name="Symbol",         value=sym,                      inline=True)
    embed.add_field(name="Name",           value=name,                     inline=True)
    embed.add_field(name="Startpreis",     value=f"{startpreis:,.4f} EUR", inline=True)
    embed.add_field(name="Volatilität",   value=f"{vol*100:.2f}%",        inline=True)
    embed.add_field(
        name="Max. Stückzahl",
        value=f"`{max_stueckzahl:,}`" if max_stueckzahl > 0 else "`Unbegrenzt`",
        inline=True
    )
    embed.add_field(name="Kurseinfluss",
                     value=f"Käufe/Verkäufe beeinflussen den Kurs (Faktor: {HANDELS_IMPACT_FAKTOR})",
                     inline=False)
    await interaction.response.send_message(embed=embed)


@tree.command(name="aktie_loeschen", description="[Leitungsebene] Aktie löschen")
@app_commands.describe(symbol="Symbol der Aktie")
async def cmd_aktie_löschen(interaction: discord.Interaction, symbol: str):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    sym = symbol.upper()
    if not get_aktie(sym):
        return await interaction.response.send_message(f"Aktie `{sym}` nicht gefunden.", ephemeral=True)
    info = aktien().pop(sym)
    for uid, pf in portfolios().items():
        pos = pf.get("positionen", {}).pop(sym, None)
        if pos and pos["menge"] > 0:
            ausz = info["preis"] * pos["menge"]
            pf["guthaben"] += ausz
            pf.setdefault("transaktionen", []).append({
                "typ": "ZWANGSLIQUIDATION", "symbol": sym,
                "menge": pos["menge"], "kurs": info["preis"],
                "einnahmen": ausz, "timestamp": int(time.time()),
            })
    await save_db()
    embed = discord.Embed(title="Aktie gelöscht", color=0xE74C3C)
    embed.add_field(name="Symbol",       value=sym,                       inline=True)
    embed.add_field(name="Letzter Kurs", value=f"{info['preis']:,.4f} EUR",inline=True)
    embed.set_footer(text="Bestände wurden liquidiert und gutgeschrieben.")
    await interaction.response.send_message(embed=embed)


@tree.command(name="markt_parameter", description="[Leitungsebene] Marktparameter anzeigen")
async def cmd_markt_parameter(interaction: discord.Interaction):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    mp = m()
    embed = discord.Embed(title="Aktuelle Marktparameter", color=0x5865F2,
                           timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Handelsgebühr",   value=f"`{mp['handelsgebühr']*100:.4f}%`",  inline=True)
    embed.add_field(name="Spread",           value=f"`{mp['spread']*100:.4f}%`",          inline=True)
    embed.add_field(name="Tages-Volatilität",value=f"`{mp['volatilitaet']*100:.4f}%`",  inline=True)
    embed.add_field(name="Trend/Drift",      value=f"`{mp['trend']:+.4f}`",               inline=True)
    embed.add_field(name="Min. Order",       value=f"`{mp['min_ordergroesse']} Stück`",  inline=True)
    embed.add_field(name="Max. Order",       value=f"`{mp['max_ordergroesse']:,} Stück`",inline=True)
    embed.add_field(name="Graph-Intervall",  value=f"`{mp['graph_intervall']}s`",         inline=True)
    embed.add_field(name="Markt",            value="`Offen`" if mp.get("markt_offen") else "`Geschlossen`", inline=True)
    embed.add_field(name="Steuer",
                     value=f"Freibetrag: `{fmt(STEUER_FREIBETRAG)}/Jahr`\nSatz: `{STEUER_SATZ*100:.0f}%` auf Gewinne bei Verkauf",
                     inline=True)
    embed.add_field(
        name="Kurseinfluss (Trades)",
        value=f"Faktor: `{HANDELS_IMPACT_FAKTOR}`  |  Max pro Trade: `{HANDELS_IMPACT_MAX*100:.1f}%`",
        inline=False
    )
    embed.set_footer(text="Ändern: /markt_parameter_setzen")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="markt_parameter_setzen", description="[Leitungsebene] Marktparameter ändern")
@app_commands.describe(parameter="Welcher Parameter?", wert="Neuer Wert")
@app_commands.choices(parameter=[
    app_commands.Choice(name="Handelsgebühr (in %, z.B. 0.25)",    value="handelsgebuehr"),
    app_commands.Choice(name="Spread Geld-Brief (in %, z.B. 0.10)", value="spread"),
    app_commands.Choice(name="Tages-Volatilität (in %, z.B. 1.5)", value="volatilitaet"),
    app_commands.Choice(name="Trend/Drift (-5 bis +5)",              value="trend"),
    app_commands.Choice(name="Min. Ordergröße (Stück)",           value="min_ordergroesse"),
    app_commands.Choice(name="Max. Ordergröße (Stück)",           value="max_ordergroesse"),
    app_commands.Choice(name="Graph-Intervall (Sekunden, min. 30)",  value="graph_intervall"),
    app_commands.Choice(name="Markt öffnen (1) / schließen (0)",   value="markt_offen"),
])
async def cmd_markt_parameter_setzen(interaction: discord.Interaction, parameter: str, wert: float):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    mp  = m()
    alt = mp.get(parameter)
    if parameter in ("handelsgebuehr", "spread", "volatilitaet"):
        neuer_wert = max(0.0, wert / 100)
    elif parameter == "trend":
        neuer_wert = max(-1.0, min(1.0, wert / 100))
    elif parameter in ("min_ordergroesse", "max_ordergroesse"):
        neuer_wert = max(1, int(wert))
    elif parameter == "graph_intervall":
        neuer_wert = max(30, int(wert))
        graph_post_task.change_interval(seconds=neuer_wert)
    elif parameter == "markt_offen":
        neuer_wert = bool(int(wert))
    else:
        return await interaction.response.send_message("Unbekannter Parameter.", ephemeral=True)
    mp[parameter] = neuer_wert
    await save_db()
    embed = discord.Embed(title="Marktparameter geändert", color=0x2ECC71)
    embed.add_field(name="Parameter", value=parameter,       inline=True)
    embed.add_field(name="Alt",       value=str(alt),        inline=True)
    embed.add_field(name="Neu",       value=str(neuer_wert), inline=True)
    await interaction.response.send_message(embed=embed)


@tree.command(name="aktie_volatilitaet", description="[Leitungsebene] Volatilität einer Aktie anpassen")
@app_commands.describe(symbol="Symbol", volatilitaet="Tägl. Volatilität in % (z.B. 2.5)")
async def cmd_aktie_vol(interaction: discord.Interaction, symbol: str, volatilitaet: float):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    info = get_aktie(symbol.upper())
    if not info:
        return await interaction.response.send_message(f"Aktie `{symbol.upper()}` nicht gefunden.", ephemeral=True)
    vol = max(0.001, min(1.0, volatilitaet / 100))
    info["volatilitaet"] = vol
    await save_db()
    await interaction.response.send_message(f"Volatilität von **{symbol.upper()}** auf **{vol*100:.2f}%** gesetzt.")


@tree.command(name="markt_oeffnen", description="[Leitungsebene] Markt öffnen oder schließen")
@app_commands.describe(offen="True = geöffnet, False = geschlossen")
async def cmd_markt_öffnen(interaction: discord.Interaction, offen: bool):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    m()["markt_offen"] = offen
    await save_db()
    status = "geöffnet" if offen else "geschlossen"
    await interaction.response.send_message(f"Markt ist jetzt **{status}**.")


@tree.command(name="graph_jetzt", description="[Leitungsebene] Graph sofort aktualisieren")
async def cmd_graph_jetzt(interaction: discord.Interaction):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    await interaction.response.defer()
    await graph_aktualisieren()
    await interaction.followup.send("Graph wurde aktualisiert.")


@tree.command(name="kurs", description="Aktuellen Kurs einer Aktie anzeigen")
@app_commands.describe(symbol="Aktien-Symbol (z.B. AAPL)")
async def cmd_kurs(interaction: discord.Interaction, symbol: str):
    await interaction.response.defer(ephemeral=True)
    sym  = symbol.upper()
    info = get_aktie(sym)
    if not info:
        return await interaction.followup.send(f"Aktie `{sym}` nicht gefunden.", ephemeral=True)
    graph = _chart_single(sym, info)
    embed = _baue_aktie_embed(sym, info)
    embed.set_image(url="attachment://chart.png")
    await interaction.followup.send(embed=embed, file=graph, ephemeral=True)


@tree.command(name="markt", description="Gesamtübersicht aller Aktien")
async def cmd_markt(interaction: discord.Interaction):
    await interaction.response.defer()
    graph = _chart_multi()
    embed = await _baue_markt_embed()
    if graph:
        embed.set_image(url="attachment://chart.png")
        await interaction.followup.send(embed=embed, file=graph)
    else:
        await interaction.followup.send(embed=embed)


# ─── DB-Backup & Wiederherstellung ────────────────────────────────

@tree.command(name="db_backup", description="[Leitungsebene] DB-Backup jetzt senden")
async def cmd_db_backup(interaction: discord.Interaction):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await db_backup_senden()
    await interaction.followup.send("DB-Backup wurde gesendet.", ephemeral=True)


@tree.command(name="db_laden", description="[Leitungsebene] Datenbank aus Backup wiederherstellen")
@app_commands.describe(datei="Die börse_db_*.json Datei aus dem Backup-Kanal")
async def cmd_db_laden(interaction: discord.Interaction, datei: discord.Attachment):
    if not ist_leitung(interaction):
        return await interaction.response.send_message(embed=keine_rechte_embed("Leitungsebene"), ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    if not datei.filename.endswith(".json"):
        return await interaction.followup.send("Nur .json-Dateien erlaubt.", ephemeral=True)

    try:
        raw  = await datei.read()
        data = json.loads(raw.decode("utf-8"))

        if not all(k in data for k in ("markt", "aktien", "portfolios")):
            return await interaction.followup.send("Ungültige DB-Struktur.", ephemeral=True)

        global _db
        _db = data

        for k, v in MARKT_DEFAULTS.items():
            _db["markt"].setdefault(k, v)
        _db["markt"].setdefault("graph_nachricht_id", 0)

        for sym, info in aktien().items():
            info.setdefault("tagesstart_preis", info["preis"])
            info.setdefault("umlauf", 0)  # umlauf-Feld für ältere Backups ergänzen

        await save_db()

        embed = discord.Embed(title="Datenbank wiederhergestellt", color=0x2ECC71,
                               timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Aktien",       value=str(len(aktien())),     inline=True)
        embed.add_field(name="Portfolios",   value=str(len(portfolios())), inline=True)
        embed.add_field(name="Wiederhergestellt von", value=interaction.user.mention, inline=True)
        embed.add_field(name="Quelldatei",   value=datei.filename,         inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
        log.info(f"DB wiederhergestellt aus {datei.filename} von {interaction.user}")

    except json.JSONDecodeError:
        await interaction.followup.send("Datei ist kein gültiges JSON.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Fehler beim Laden: {e}", ephemeral=True)


# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT M: BACKGROUND TASKS ────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

_markt_schluss_gemeldet = False


@tasks.loop(seconds=60)
async def kurs_update_task():
    global _markt_schluss_gemeldet
    jetzt = get_now()

    if jetzt.hour == HANDELS_START and jetzt.minute == 0:
        if not m().get("markt_offen", True):
            m()["markt_offen"] = True
            _markt_schluss_gemeldet = False
            await save_db()
            log.info("Markt automatisch geöffnet (08:00 Uhr Berliner Zeit)")

            for info in aktien().values():
                info["tagesstart_preis"] = info["preis"]

            kanal = client.get_channel(GRAPH_KANAL_ID)
            if kanal:
                try:
                    embed = discord.Embed(
                        title="Börse geöffnet!",
                        description=f"> Der Handel ist täglich zwischen **{HANDELS_START:02d}:00** und **{HANDELS_ENDE:02d}:00 Uhr** möglich. Sie können innerhalb dieser Zeiten uneingeschränkt Kauf- und Verkaufsaufträge tätigen. Wir wünschen Ihnen erfolgreiche Handelsgeschäfte.\n**Mit freundlichen Grüßen**\n~ Hamburger Börse",
                        color=0x0f0f17,
                        timestamp=datetime.now(timezone.utc)
                    )
                    await kanal.send(embed=embed)
                except Exception as e:
                    log.error(f"Öffnungs-Nachricht Fehler: {e}")

    if jetzt.hour == HANDELS_ENDE and jetzt.minute == 0 and not _markt_schluss_gemeldet:
        m()["markt_offen"] = False
        _markt_schluss_gemeldet = True
        await save_db()
        log.info("Markt automatisch geschlossen (22:00 Uhr Berliner Zeit)")

        kanal = client.get_channel(GRAPH_KANAL_ID)
        if kanal:
            try:
                lines = []
                for sym, info in aktien().items():
                    hist   = _filtere_heutige_history(info.get("kurshistorie", []))
                    if len(hist) >= 2:
                        start  = hist[0][1]
                        ende   = hist[-1][1]
                        change = (ende - start) / start * 100 if start else 0
                        sign   = "+" if change >= 0 else ""
                        lines.append(f"**`{sym:<5}`**  `{ende:>10,.4f} EUR`  `{sign}{change:>7.2f}%`")
                    else:
                        lines.append(f"**`{sym:<5}`**  `{info['preis']:>10,.4f} EUR`  `n/a`")

                embed = discord.Embed(
                    title="Börsenschluss - Tagesübersicht",
                    description="\n".join(lines) if lines else "Keine Aktien.",
                    color=0xE74C3C,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text=f"Nächste Öffnung: {HANDELS_START:02d}:00 Uhr")
                await kanal.send(embed=embed)
            except Exception as e:
                log.error(f"Schlussbericht Fehler: {e}")

        await graph_aktualisieren()
        return

    if not ist_handelszeit():
        return

    try:
        await update_kurse()
    except Exception as e:
        log.error(f"Kurs-Update Fehler: {e}")


@tasks.loop(seconds=GRAPH_UPDATE_DEFAULT)
async def graph_post_task():
    if not ist_handelszeit():
        return
    try:
        await graph_aktualisieren()
    except Exception as e:
        log.error(f"Graph-Update Fehler: {e}")


@tasks.loop(hours=3)
async def db_backup_task():
    try:
        await db_backup_senden()
    except Exception as e:
        log.error(f"DB-Backup Task Fehler: {e}")


# ═══════════════════════════════════════════════════════════════════
# ─── ABSCHNITT N: BOT START ───────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════

@client.event
async def on_ready():
    global _db
    _db = _lade_db()
    log.info(f"Eingeloggt als {client.user}  (ID: {client.user.id})")

    await tree.sync()
    log.info("Slash-Commands synchronisiert.")

    client.add_view(AuszahlungBestätigenView("0", 0, 0, 0))

    for sym, info in aktien().items():
        info.setdefault("tagesstart_preis", info["preis"])
        info.setdefault("umlauf", 0)  # Rückwärtskompatibilität

    intervall = int(m().get("graph_intervall", GRAPH_UPDATE_DEFAULT))
    graph_post_task.change_interval(seconds=intervall)

    kurs_update_task.start()
    graph_post_task.start()
    db_backup_task.start()

    await asyncio.sleep(3)
    await graph_aktualisieren()
    log.info(f"Bereit. Graph-Intervall: {intervall}s | DB-Backup alle 3h")


_starte_health_server()

client.run(BOT_TOKEN)
