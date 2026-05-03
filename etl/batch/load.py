import os
import logging

def save_to_processed(df, filepath):
    logging.info("Début de l'étape LOAD...")

    try:
        dossier_destination = os.path.dirname(filepath)
        
        if not os.path.exists(dossier_destination):
            os.makedirs(dossier_destination)
            logging.info(f"Création du dossier manquant : {dossier_destination}")

        logging.info(f"Écriture du fichier Parquet en cours : {filepath}...")
        df.to_parquet(filepath, index=True) 
        
        taille_ko = os.path.getsize(filepath) / 1024
        logging.info(f"Sauvegarde réussie ! Taille du fichier : {taille_ko:.2f} Ko.")

    except Exception as e:
        logging.error(f"Erreur fatale lors de la sauvegarde : {e}")
        raise