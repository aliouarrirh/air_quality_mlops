import logging
import sys
import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")   # pas d'affichage interactif
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ── Logging et Encodage ──────────────────────────────────────────────────────
sys.stdout.reconfigure(encoding='utf-8')

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "eda.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("eda")

# ── Chemins ──────────────────────────────────────────────────────────────────
PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
EDA_DIR       = Path(__file__).resolve().parents[2] / "reports"
EDA_DIR.mkdir(parents=True, exist_ok=True)

# ── Style global matplotlib ───────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi":       120,
    "figure.facecolor": "white",
    "axes.facecolor":   "#f8f9fa",
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "font.size":        11,
    "axes.titlesize":   13,
    "axes.labelsize":   11,
})

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def save_fig(fig: plt.Figure, name: str) -> Path:
    path = EDA_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  Figure sauvegardée : {path.name}")
    return path

# ─────────────────────────────────────────────────────────────────────────────
# 1. Statistiques descriptives
# ─────────────────────────────────────────────────────────────────────────────

def stats_summary(df: pd.DataFrame) -> dict:
    logger.info("─── Stats descriptives ───")
    target_stats = df["pm2p5"].describe()
    logger.info(f"\n{target_stats.to_string()}")

    nan_report = df.isna().mean().sort_values(ascending=False)
    nan_cols = nan_report[nan_report > 0].head(20)

    return {
        "n_rows": len(df),
        "n_features": df.shape[1],
        "pm2p5_describe": target_stats.to_dict(),
        "top_nan": nan_cols.to_dict(),
    }

# ─────────────────────────────────────────────────────────────────────────────
# 2. Série temporelle PM2.5
# ─────────────────────────────────────────────────────────────────────────────

def plot_pm25_timeseries(df: pd.DataFrame) -> Path:
    logger.info("─── Plot série temporelle ───")
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=False)

    daily = df.set_index("datetime")["pm2p5"].resample("D").mean()
    axes[0].plot(daily.index, daily.values, linewidth=0.8, color="#2196f3", alpha=0.8)
    axes[0].fill_between(daily.index, daily.values, alpha=0.15, color="#2196f3")
    axes[0].axhline(y=15, color="orange", linestyle="--", linewidth=1, label="OMS 24h (15 µg/m³)")
    axes[0].set_title("PM2.5 journalier moyen — Casablanca")
    axes[0].set_ylabel("PM2.5 (µg/m³)")
    axes[0].legend(fontsize=9)

    last_90 = df[df["datetime"] >= df["datetime"].max() - pd.Timedelta(days=90)]
    axes[1].plot(last_90["datetime"], last_90["pm2p5"], linewidth=0.6, color="#9c27b0", alpha=0.9)
    axes[1].set_title("Zoom — 90 derniers jours (horaire)")
    axes[1].set_ylabel("PM2.5 (µg/m³)")
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))

    axes[2].hist(df["pm2p5"].dropna(), bins=60, color="#4caf50", edgecolor="white", alpha=0.8)
    axes[2].set_title("Distribution PM2.5")
    axes[2].set_xlabel("PM2.5 (µg/m³)")
    axes[2].set_ylabel("Fréquence")
    for threshold, label, color in [(15, "OMS 15", "orange"), (25, "OMS 25", "red")]:
        axes[2].axvline(x=threshold, color=color, linestyle="--", linewidth=1.5, label=label)
    axes[2].legend(fontsize=9)

    fig.tight_layout()
    return save_fig(fig, "01_pm25_timeseries")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Patterns temporels (heure/jour/mois)
# ─────────────────────────────────────────────────────────────────────────────

def plot_temporal_patterns(df: pd.DataFrame) -> Path:
    logger.info("─── Plot patterns temporels ───")
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    hourly = df.groupby("heure")["pm2p5"].agg(["mean", "std"])
    axes[0].bar(hourly.index, hourly["mean"], color="#42a5f5", alpha=0.8, label="Moyenne")
    axes[0].fill_between(hourly.index,
                         hourly["mean"] - hourly["std"],
                         hourly["mean"] + hourly["std"],
                         alpha=0.2, color="#42a5f5", label="±1 std")
    axes[0].set_title("PM2.5 moyen par heure")
    axes[0].set_xlabel("Heure")
    axes[0].set_ylabel("PM2.5 (µg/m³)")
    axes[0].legend()

    dow_labels = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
    dow = df.groupby("jour_semaine")["pm2p5"].mean()
    colors = ["#ef5350" if i >= 5 else "#42a5f5" for i in dow.index]
    axes[1].bar(dow.index, dow.values, color=colors, alpha=0.85)
    axes[1].set_xticks(range(7))
    axes[1].set_xticklabels(dow_labels)
    axes[1].set_title("PM2.5 moyen par jour")
    axes[1].set_ylabel("PM2.5 (µg/m³)")

    month_labels = ["Jan","Fév","Mar","Avr","Mai","Jun","Jul","Aoû","Sep","Oct","Nov","Déc"]
    monthly = df.groupby("mois")["pm2p5"].mean()
    axes[2].bar(monthly.index, monthly.values, color="#66bb6a", alpha=0.85)
    axes[2].set_xticks(range(1, 13))
    axes[2].set_xticklabels(month_labels, rotation=45)
    axes[2].set_title("PM2.5 moyen par mois")
    axes[2].set_ylabel("PM2.5 (µg/m³)")

    fig.tight_layout()
    return save_fig(fig, "02_temporal_patterns")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Corrélations météo × PM2.5
# ─────────────────────────────────────────────────────────────────────────────

def plot_weather_correlations(df: pd.DataFrame) -> Path:
    logger.info("─── Plot corrélations météo ───")
    weather_cols = [c for c in ["temp_c", "vent_kmh", "blh_m", "no2"] if c in df.columns]

    if not weather_cols:
        return None

    corr_data = df[["pm2p5"] + weather_cols].corr()["pm2p5"].drop("pm2p5").sort_values()

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#ef5350" if v > 0 else "#42a5f5" for v in corr_data.values]
    bars = ax.barh(corr_data.index, corr_data.values, color=colors, alpha=0.8)
    ax.axvline(x=0, color="black", linewidth=0.8)
    ax.set_title("Corrélation Pearson — Variables météo × PM2.5")
    ax.set_xlabel("Corrélation de Pearson")

    for bar, val in zip(bars, corr_data.values):
        ax.text(val + 0.005 * np.sign(val), bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}", va="center", fontsize=9)

    fig.tight_layout()
    return save_fig(fig, "03_weather_correlations")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Matrice de corrélation (heatmap top features)
# ─────────────────────────────────────────────────────────────────────────────

def plot_correlation_heatmap(df: pd.DataFrame) -> Path:
    logger.info("─── Heatmap corrélations ───")
    import seaborn as sns

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    corr_full = df[numeric_cols].corr()
    top_cols  = corr_full["pm2p5"].abs().sort_values(ascending=False).head(15).index.tolist()

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(
        df[top_cols].corr(),
        annot=True, fmt=".2f", cmap="RdBu_r", center=0,
        square=True, linewidths=0.5, ax=ax, cbar_kws={"shrink": 0.8},
        annot_kws={"size": 9},
    )
    ax.set_title("Matrice de corrélation — Top Features")
    fig.tight_layout()
    return save_fig(fig, "04_correlation_heatmap")

# ─────────────────────────────────────────────────────────────────────────────
# 6. Autocorrélation PM2.5
# ─────────────────────────────────────────────────────────────────────────────

def plot_autocorrelation(df: pd.DataFrame) -> Path:
    logger.info("─── Autocorrélation PM2.5 ───")
    from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

    pm2p5_clean = df["pm2p5"].dropna()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))
    plot_acf(pm2p5_clean,  lags=72, ax=ax1, alpha=0.05)
    plot_pacf(pm2p5_clean, lags=72, ax=ax2, alpha=0.05, method="yw")
    ax1.set_title("ACF — PM2.5 (72 lags horaires)")
    ax2.set_title("PACF — PM2.5 (72 lags horaires)")
    ax1.set_xlabel("Lag (heures)")
    ax2.set_xlabel("Lag (heures)")
    fig.tight_layout()
    return save_fig(fig, "05_autocorrelation")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Rapport HTML
# ─────────────────────────────────────────────────────────────────────────────

def generate_html_report(stats: dict, figures: list[Path], data_preview_html: str) -> Path:
    """Génère un rapport HTML avec les stats, les données brutes et les figures."""
    import base64

    figures_html = ""
    for fig_path in figures:
        if fig_path is None or not fig_path.exists():
            continue
        with open(fig_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        figures_html += f"""
        <div class="figure">
            <img src="data:image/png;base64,{b64}" alt="{fig_path.stem}"/>
            <p class="caption">{fig_path.stem.replace('_', ' ').title()}</p>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>EDA — Qualité de l'air PM2.5 Casablanca</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 40px auto; max-width: 1200px; color: #333; }}
  h1 {{ color: #1565c0; border-bottom: 2px solid #1565c0; padding-bottom: 8px; }}
  h2 {{ color: #37474f; margin-top: 40px; border-bottom: 1px solid #eee; padding-bottom: 10px; }}
  
  /* Style des cartes de statistiques */
  .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 16px; margin: 20px 0; }}
  .stat-card {{ background: #f0f4ff; border-radius: 8px; padding: 16px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
  .stat-card .value {{ font-size: 2em; font-weight: bold; color: #1565c0; }}
  .stat-card .label {{ font-size: 0.85em; color: #555; margin-top: 4px; }}
  
  /* Style pour le tableau DataFrame */
  .table-container {{ overflow-x: auto; margin-top: 15px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
  .dataframe {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
  .dataframe th {{ background-color: #1565c0; color: white; padding: 12px 15px; text-align: left; }}
  .dataframe td {{ padding: 10px 15px; border-bottom: 1px solid #ddd; }}
  .dataframe tbody tr:nth-of-type(even) {{ background-color: #f9f9f9; }}
  .dataframe tbody tr:hover {{ background-color: #f1f1f1; }}
  
  /* Style des graphiques */
  .figure {{ margin: 30px 0; text-align: center; }}
  .figure img {{ max-width: 100%; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
  .caption {{ color: #777; font-size: 0.9em; margin-top: 10px; font-weight: bold; }}
  
  footer {{ margin-top: 60px; color: #aaa; font-size: 0.85em; text-align: center; border-top: 1px solid #eee; padding-top: 20px; }}
</style>
</head>
<body>
<h1>EDA — Prédiction Qualité de l'air PM2.5 Casablanca</h1>
<p>Généré le {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M UTC")}</p>

<h2>1. Statistiques Globales</h2>
<div class="stats-grid">
  <div class="stat-card"><div class="value">{stats['n_rows']:,}</div><div class="label">Observations</div></div>
  <div class="stat-card"><div class="value">{stats['n_features']}</div><div class="label">Features (Colonnes)</div></div>
  <div class="stat-card"><div class="value">{stats['pm2p5_describe'].get('mean', 0):.1f}</div><div class="label">PM2.5 moyen (µg/m³)</div></div>
  <div class="stat-card"><div class="value">{stats['pm2p5_describe'].get('max', 0):.1f}</div><div class="label">PM2.5 max (µg/m³)</div></div>
  <div class="stat-card"><div class="value">{stats['pm2p5_describe'].get('50%', 0):.1f}</div><div class="label">PM2.5 médiane</div></div>
  <div class="stat-card"><div class="value">{stats['pm2p5_describe'].get('std', 0):.1f}</div><div class="label">Écart-type</div></div>
</div>

<h2>2. Aperçu du Dataset (Les 5 premières lignes)</h2>
<div class="table-container">
  {data_preview_html}
</div>

<h2>3. Visualisations Analytiques</h2>
{figures_html}

<footer>PFM — Machine Learning | Master AI & Big Data | Air Quality Casablanca</footer>
</body>
</html>"""

    report_path = EDA_DIR / "eda_report.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"✅ Rapport HTML généré avec succès → {report_path}")
    return report_path

# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def run_eda(input_path: Path = None) -> None:
    """Pipeline EDA complet."""
    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║        DÉBUT EDA                             ║")
    logger.info("╚══════════════════════════════════════════════╝")

    input_path = input_path or PROCESSED_DIR / "casablanca_master.parquet"

    try:
        logger.info(f"Lecture : {input_path}")
        df = pd.read_parquet(input_path)
        df = df.reset_index() 
        logger.info(f"  {len(df)} lignes chargées")

        # 1. Stats
        stats = stats_summary(df)

        # 2. Aperçu HTML des données (df.head())
        logger.info("Création de l'aperçu du dataset...")
        # float_format permet d'arrondir les chiffres à 2 virgules pour que ça soit propre
        data_preview_html = df.head(5).to_html(classes="dataframe", index=False, float_format=lambda x: f"{x:.2f}")

        # 3. Figures
        figures = []
        figures.append(plot_pm25_timeseries(df))
        figures.append(plot_temporal_patterns(df))
        figures.append(plot_weather_correlations(df))
        figures.append(plot_correlation_heatmap(df))
        figures.append(plot_autocorrelation(df))

        # 4. Génération finale du rapport HTML en lui passant la preview
        generate_html_report(stats, [f for f in figures if f is not None], data_preview_html)

        logger.info(f"🚀 EDA terminé. Ouvre le fichier {EDA_DIR}/eda_report.html dans ton navigateur !")

    except Exception as e:
        logger.error(f"Erreur EDA : {e}")
        raise

if __name__ == "__main__":
    run_eda()