import pandas as pd
import logging

def create_features(df_clean):
    logging.info("Début de l'étape FEATURE ENGINEERING...")

    try:
        df = df_clean.copy()
        logging.info("Création des variables de temps...")
        df['heure'] = df.index.hour
        df['jour_semaine'] = df.index.dayofweek
        df['mois'] = df.index.month
        
        df['est_weekend'] = (df['jour_semaine'] >= 5).astype(int)

        logging.info("Création des variables de décalage (Lags)...")
        
        df['pm2p5_lag_1h'] = df['pm2p5'].shift(1)
        df['pm2p5_lag_2h'] = df['pm2p5'].shift(2)
        
        df['pm2p5_lag_24h'] = df['pm2p5'].shift(24)

        logging.info("Création des moyennes mobiles (Rolling Means)...")
        
        df['pm2p5_rolling_3h'] = df['pm2p5'].rolling(window=3).mean()
        df['pm2p5_rolling_24h'] = df['pm2p5'].rolling(window=24).mean()

        taille_avant = len(df)
        df = df.dropna()
        lignes_purges = taille_avant - len(df)
        
        logging.info(f"Feature Engineering terminé ! {lignes_purges} lignes de démarrage purgées (à cause du Lag 24h).")
        logging.info(f"Taille finale pour le Machine Learning : {len(df)} lignes et {len(df.columns)} variables (colonnes).")
        
        return df

    except Exception as e:
        logging.error(f"Erreur fatale lors du Feature Engineering : {e}")
        raise