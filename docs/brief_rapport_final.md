# Brief — Rédaction du rapport final « Delhi Air Quality MLOps »

> **À lire par l'assistant chargé de rédiger le rapport.** Ce document est autonome : toutes les données factuelles nécessaires y figurent. **N'invente aucun chiffre, aucun outil, aucun résultat** qui ne soit pas listé ici. Si une information te manque, signale-la explicitement par `[À COMPLÉTER : ...]` plutôt que de la combler.

---

## 1. Ce que tu dois produire

Un **rapport final de projet universitaire** en français, structuré selon le plan imposé en section 6, couvrant les 11 livrables du module.

**Format** : Markdown, titres hiérarchisés, tableaux quand ils clarifient, blocs de code pour les extraits techniques, **schémas en Mermaid** selon les spécifications de la section 7.

**Longueur visée** : 25 à 35 pages équivalent, soit environ 8 000 à 12 000 mots.

**Registre** : rapport technique d'étudiants de master. Précis, sobre, assumant les choix et les limites. Ni promotionnel, ni scolaire.

### Consignes de rédaction importantes

- **Ancre chaque affirmation dans un fait de la section 3.** Ce rapport doit se lire comme écrit par l'équipe qui a vécu le projet, pas comme une synthèse générique sur le MLOps.
- **Évite le remplissage théorique.** Pas de paragraphe générique sur « l'importance du MLOps dans l'entreprise moderne ». Le lecteur connaît le domaine.
- **Varie la structure.** Toutes les sections ne doivent pas suivre le même gabarit. Certaines gagnent à être narratives, d'autres tabulaires.
- **Assume les faiblesses** (section 5). Un rapport qui reconnaît ses limites est plus crédible qu'un rapport parfait. C'est aussi ce qui est attendu d'une analyse critique.
- **Utilise les incidents réels** (section 4). C'est la matière la plus distinctive du projet : elle prouve un déploiement réel et non une exécution sur poste de développement.

---

## 2. Contexte du projet

**Module** : MLOps & DataOps — Pr. Mohammed Ait Daoud, FSBM
**Sujet** : Prédiction de la qualité de l'air à Delhi (Inde)
**Soutenance** : 18 septembre 2026
**Dépôt** : `aliouarrirh/air_quality_mlops`

**Équipe** — `[À COMPLÉTER : noms des membres]`, avec la répartition de rôles suivante :

| Rôle | Membre |
|---|---|
| Product Owner | `[À COMPLÉTER]` |
| Scrum Master | `[À COMPLÉTER]` |
| Data Engineer | `[À COMPLÉTER]` |
| ML Engineer | `[À COMPLÉTER]` |
| Data Analyst | `[À COMPLÉTER]` |

**Problématique métier** : Delhi subit l'une des pires pollutions atmosphériques au monde. Les dispositifs existants publient des mesures brutes, constatées. Ils ne permettent pas d'anticiper. Un pic de PM2.5 connu quelques heures à l'avance permettrait de déclencher des alertes sanitaires, d'adapter la circulation ou de fermer des écoles.

**Objectif** : construire une chaîne MLOps complète qui prédit la concentration horaire de PM2.5, expose cette prédiction par une API, et surveille en continu la santé du service comme la qualité du modèle.

---

## 3. Données factuelles du projet

> Tout ce qui suit est vérifié. C'est le socle du rapport.

### 3.1 Source de données

- **35 stations** de mesure à Delhi et sa région
- Origine : **OpenAQ v3**, ingérée sous forme d'**instantanés CSV** versionnés dans le dépôt (et non par appel direct à l'API)
- **6 polluants** : PM2.5, PM10, NO2, SO2, CO, O3
- Trois jeux : métadonnées des stations, mesures historiques, instantané temps réel
- Volumétrie après transformation : **4 332 lignes** dans la table d'agrégats horaires

**Justification du choix CSV** (à développer, c'est un point que le jury peut soulever) : l'intégration continue reconstruit l'intégralité de la chaîne de données à chaque commit. Elle ne peut dépendre ni de la disponibilité d'une API externe, ni d'une clé secrète en environnement d'exécution. Le connecteur dlt reste identique ; seule la source d'alimentation change.

### 3.2 Pile technique

| Couche | Outil | Détail |
|---|---|---|
| Ingestion | **dlt** | 3 ressources, écriture idempotente |
| Stockage analytique | **DuckDB** | schéma `raw` |
| Transformations | **dbt** | 3 modèles, 7 tests |
| Orchestration | **Dagster** | 4 assets, 2 jobs, 2 planifications |
| Modélisation | **XGBoost** | régression |
| Suivi & registre | **MLflow** | backend SQLite, artefacts sur MinIO |
| Stockage objet | **MinIO** | compatible S3, mode proxy |
| Service | **FastAPI** | 3 endpoints |
| Conteneurisation | **Docker Compose** | 9 services |
| Intégration continue | **GitHub Actions** | 3 jobs |
| Supervision | **Grafana + PostgreSQL** | 14 panneaux |
| Déploiement | **Komodo** | serveur `exp.s3.fsbm.ma` |

### 3.3 Modélisation dbt

Trois couches :

1. `stg_delhi_measurements` *(vue)* — nettoyage, indicateur de validité par polluant (bornes spécifiques à chaque polluant), variables temporelles en heure locale indienne, dérivation des **saisons indiennes** (`winter`, `summer`, `monsoon`, `post_monsoon`)
2. `int_delhi_cleaned` *(vue)* — déduplication
3. `mart_delhi_hourly` *(table)* — pivot des 6 polluants en colonnes, agrégation horaire par station, calcul de l'**AQI selon le standard indien NAQI**, catégorisation (`Good` → `Satisfactory` → `Moderate` → `Poor` → `Very Poor` → `Severe`)

### 3.4 Qualité des données

**Contrat de données** : `data_contracts/air_quality_contract.yaml` — schéma, types, champs obligatoires, valeurs autorisées, seuils de qualité.

**9 tests automatisés** (`tests/test_data_quality.py`) couvrant les 5 dimensions exigées :

| Dimension | Tests |
|---|---|
| Complétude | `datetime_utc` jamais nul ; au moins 90 % de valeurs renseignées |
| Validité | aucune valeur négative ; PM2.5 borné 0–999 ; catégories AQI conformes |
| Cohérence | **PM2.5 hivernal > PM2.5 de mousson** — invariant climatique connu de Delhi |
| Intégrité | au moins 10 stations ; présence de PM2.5, PM10, NO2, CO |
| Fraîcheur | instantané de moins de 3 h |

**7 tests dbt** complémentaires : `not_null`, `accepted_values`.

> Le test de cohérence mérite d'être mis en avant : il encode une connaissance métier. Si les données le contredisent, c'est que la source est corrompue, même si chaque valeur prise isolément paraît plausible.

### 3.5 Modèle

- **Algorithme** : XGBoost Regressor
- **Cible** : concentration de PM2.5
- **11 variables** : pm10, no2, so2, co, o3, hour_local, day_of_week, month, season_winter, season_summer, season_monsoon
- **Hyperparamètres** : `n_estimators=200`, `max_depth=6`, `learning_rate=0.1`, `subsample=0.8`, `random_state=42`
- **Découpage** : temporel 80/20, **sans mélange aléatoire** — pour ne pas faire fuiter de l'information du futur vers le passé

**Résultats observés sur trois entraînements successifs :**

| Entraînement | RMSE | MAE | R² |
|---|---|---|---|
| 1 | 38,32 | 29,48 | 0,192 |
| 2 | 37,81 | 28,85 | 0,160 |
| 3 | 36,86 | 28,17 | 0,176 |

*(unités : µg/m³ pour RMSE et MAE)*

### 3.6 Service ML

**FastAPI**, trois endpoints :

| Endpoint | Rôle |
|---|---|
| `GET /health` | état du service, version du modèle servie, mode de résolution |
| `POST /predict` | AQI prédit, catégorie NAQI, version réelle du modèle |
| `POST /reload` | recharge le modèle depuis le registre, sans redémarrage |

**Résolution du modèle**, par ordre de priorité :
1. Variable `MLFLOW_MODEL_URI` si définie *(échappatoire explicite)*
2. Alias **`@champion`** du registre MLflow *(mode nominal)*
3. Version la plus récente *(repli, évite un service indisponible tant qu'aucun champion n'est promu)*

Le modèle est chargé **une seule fois au démarrage** et mis en cache. Une version antérieure le rechargeait à chaque requête.

### 3.7 Cycle de vie du modèle

Point central du projet, à développer en détail :

- Chaque entraînement crée une **nouvelle version** au registre
- **La promotion n'est pas automatique.** Le premier modèle est promu pour amorcer le service ; ensuite l'alias `champion` ne bouge que sur décision explicite
- La mise en service se fait par `POST /reload`, **sans redéploiement ni interruption**

**Justification** : les écarts de RMSE entre versions sont de l'ordre de 1 à 2 µg/m³ sur des valeurs avoisinant 150. Une règle de promotion automatique déclencherait sur du bruit statistique plutôt que sur une amélioration réelle. La promotion conditionnelle — ne promouvoir qu'au-delà d'un seuil de gain — est identifiée au backlog.

### 3.8 Orchestration

**4 assets Dagster** : `ingest_delhi_data → run_dbt → train_model`, plus `collect_monitoring` indépendant.

**2 jobs** : `delhi_daily_pipeline` (quotidien à 1 h), `monitoring_job` (horaire).

Durée d'exécution observée du pipeline complet : **≈ 44 secondes**.

### 3.9 Supervision

Tableau de bord Grafana provisionné automatiquement, **14 panneaux**, alimenté par PostgreSQL. Collecte assurée par un job Dagster horaire qui sonde l'API, interroge MLflow et compare les distributions.

| Axe exigé | Mise en œuvre |
|---|---|
| Disponibilité | taux de disponibilité par endpoint sur 72 h, sondes sur `/health` et `/predict` |
| Temps de réponse | latence médiane (p50) par endpoint, série temporelle |
| Métriques ML | RMSE, MAE, R² relevés depuis MLflow, évolution dans le temps |
| Dérive | score par polluant face à une distribution de référence, seuil de déclenchement, tableau récapitulatif |

Cinq tables PostgreSQL : `service_health`, `ml_metrics`, `drift_metrics`, `predictions`, `pipeline_runs`.

### 3.10 Intégration continue

`.github/workflows/ci.yml`, déclenché sur chaque push et pull request :

| Job | Contenu |
|---|---|
| `test` | reconstruction de la base DuckDB depuis les CSV → `dbt run` → `pytest` avec couverture → `dbt test` |
| `build-docker` | construction de l'image, démarrage du conteneur, vérification du endpoint `/health` |
| `lint` | analyse statique avec ruff |

> **Les trois jobs passent au vert.** La qualité de style a fait l'objet d'une passe dédiée : 32 avertissements corrigés, sans modification de comportement, validés en rejouant la séquence complète en local.

### 3.11 Déploiement

9 services orchestrés par Docker Compose, déployés sur Komodo :

| Port | Service |
|---|---|
| 4901 | PostgreSQL |
| 4902 | MLflow |
| 4903 | Dagster |
| 4904 | API FastAPI |
| 4905 | Grafana |
| 4906 | Console MinIO |

Le déploiement est déclenché manuellement : **livraison continue**, et non déploiement continu. Ce choix est délibéré — voir section 5.

### 3.12 Collaboration

- Plus de **80 commits**, messages conventionnels (`feat:`, `fix:`)
- Travaux menés sur branches, **2 pull requests fusionnées**
- Gestion agile : 6 épics, 19 user stories, 6 sprints

---

## 4. Incidents réels rencontrés

> **Matière la plus distinctive du rapport.** À exploiter dans une section dédiée (« Difficultés rencontrées ») et dans les rétrospectives de sprint. Chacun a été diagnostiqué puis corrigé.

| # | Incident | Cause racine | Résolution |
|---|---|---|---|
| 1 | Le backend de suivi MLflow refuse les écritures | Le stockage sur système de fichiers est passé en mode maintenance dans les versions récentes | Migration vers un backend base de données |
| 2 | Interface MLflow inaccessible derrière le proxy | Contrôles d'origine et d'en-tête hôte | Configuration des origines autorisées |
| 3 | Un second job d'orchestration n'apparaît jamais | **Le code déployé n'était pas celui du dépôt** : la plateforme de déploiement ne reconstruisait jamais l'image conteneur | Montage du code en volume, découplant code et image |
| 4 | L'orchestrateur ne démarre plus | Conflit de migration de schéma dans sa base interne | Version de l'outil figée |
| 5 | Les exécutions restent bloquées en file d'attente | L'ordonnanceur et l'interface ne partageaient pas le même répertoire d'état | Volume partagé |
| 6 | Les transformations échouent brutalement | Incompatibilité entre la bibliothèque de transformation et sa dépendance de sérialisation | Correctif de compatibilité appliqué au démarrage |
| 7 | L'entraînement écrit dans un stockage local au lieu du serveur | Une variable d'environnement héritée écrasait la configuration | Transmission explicite au sous-processus |
| 8 | **Artefacts du modèle introuvables depuis l'API** | Avec un stockage fichier, chaque conteneur résout les chemins sur **son propre** système de fichiers : l'entraînement écrivait à un emplacement invisible pour l'API | Passage au stockage objet MinIO |
| 9 | Les tableaux de bord n'affichent pas les mesures réelles | Les jeux de démonstration étaient réinjectés à chaque déploiement et noyaient les données réelles | Insertion conditionnée |
| 10 | La CI échoue faute de données | L'environnement d'intégration ne disposait pas de la base analytique | Reconstruction de la chaîne depuis les sources versionnées |
| 11 | Le déploiement échoue au téléchargement des images | Les images MinIO ne sont plus distribuées sur Docker Hub | Bascule vers le registre officiel, tags épinglés |
| 12 | Le stockage objet reste inutilisé malgré sa configuration | L'emplacement des artefacts d'une expérience est **figé à sa création** et ne peut être modifié ensuite | Archivage de l'ancienne expérience, recréation |
| 13 | La promotion d'un modèle reste sans effet | Une variable figée réinjectée par la plateforme de déploiement neutralisait l'alias | Nettoyage de la configuration d'environnement |

**Enseignement transversal à formuler** : la majorité de ces incidents proviennent de l'**écart entre l'environnement de développement et l'environnement déployé**, et non d'erreurs de logique métier. Plusieurs échouaient silencieusement — sans message d'erreur — ce qui justifie a posteriori l'investissement dans la supervision et l'intégration continue.

---

## 5. Analyse critique et limites

> Section attendue dans un rapport de ce niveau. À traiter franchement.

**Performance du modèle.** Un R² d'environ 0,18 est faible et doit être assumé. Prédire le PM2.5 horaire à partir des seuls autres polluants et de variables calendaires est intrinsèquement difficile : **les variables météorologiques manquent** — vent, température, humidité, inversion thermique — alors qu'elles dominent la dispersion des particules à Delhi. C'est la première piste d'amélioration.

**Source de données.** Instantanés CSV plutôt qu'appel direct à l'API OpenAQ. Choix motivé par la reproductibilité de l'intégration continue, au prix de l'actualité des données.

**Promotion manuelle.** Défendable, mais elle introduit une dépendance humaine dans la chaîne. La promotion conditionnelle sur seuil de gain est identifiée au backlog.

**Livraison continue et non déploiement continu.** Le déploiement reste un geste explicite. Pour un modèle de ML dont la sortie a un impact sanitaire, cette validation humaine est un choix défendable — mais c'est bien un choix, et il doit être présenté comme tel.

**Sécurité.** Les identifiants du stockage objet sont ceux par défaut. En production, ils seraient injectés par un gestionnaire de secrets. `[À COMPLÉTER : préciser si corrigé avant la soutenance]`

**Dérive : détection sans réaction.** Le système détecte la dérive mais ne déclenche aucun réentraînement automatique. La boucle est ouverte.

---

## 6. Plan du rapport à produire

Structure attendue, alignée sur les 11 livrables du module.

1. **Introduction** — problématique, enjeu sanitaire à Delhi, objectifs, périmètre
2. **Vision du projet** *(L1)* — utilisateurs cibles, valeur métier, stratégie de données, indicateurs de succès
3. **Gestion de projet agile** *(L2)* — organisation, rôles, backlog produit, découpage en sprints, déroulement, rétrospectives *(nourrir avec les incidents de la section 4)*
4. **Architecture générale** *(L3)* — vue d'ensemble, choix technologiques et **justification de chacun**
   → **Schéma 1** *(spécifié en section 7)*
5. **Pipeline DataOps** *(L4)* — ingestion dlt, stockage DuckDB, transformations dbt en trois couches, orchestration Dagster
   → **Schéma 2** et **Schéma 3** *(spécifiés en section 7)*
6. **Qualité et gouvernance des données** *(L5)* — contrat, tests des 5 dimensions, lignage, documentation
7. **Composant ML** *(L6)* — préparation, ingénierie des variables, entraînement, évaluation, **résultats chiffrés**
8. **Suivi et registre MLflow** *(L7)* — expériences, paramètres, métriques, artefacts sur stockage objet, registre
   → `[CAPTURE : interface MLflow avec les runs et métriques]`
9. **Cycle de vie du modèle** — versionnement, comparaison, promotion par alias, mise en service à chaud
   → **Schéma 4** *(spécifié en section 7)*
10. **Déploiement et mise en service** *(L8)* — API, conteneurisation, architecture de déploiement
    → `[CAPTURE : documentation Swagger et exemple de réponse]`
11. **Intégration continue** *(L9)* — les trois jobs, ce qu'ils protègent
12. **Supervision et observabilité** *(L10)* — les 4 axes, collecte, seuils
    → `[CAPTURE : tableau de bord Grafana]`
13. **Difficultés rencontrées et solutions** — section 4 du présent brief, rédigée et analysée
14. **Analyse critique et limites** — section 5 du présent brief
15. **Perspectives** — variables météorologiques, API temps réel, promotion conditionnelle, réentraînement sur dérive, alertes, extension à d'autres villes
16. **Conclusion** — bilan par rapport aux objectifs, compétences acquises
17. **Annexes** — arborescence du dépôt, extraits de configuration, ports des services

---

## 7. Schémas à générer

Produis les quatre schémas ci-dessous **en Mermaid**, directement intégrables au rapport. N'ajoute aucun composant qui ne figure pas dans les spécifications : elles décrivent le système réel.

Conseils communs : oriente de haut en bas ou de gauche à droite selon la lisibilité, regroupe par `subgraph` quand c'est indiqué, et garde les libellés courts.

---

### Schéma 1 — Architecture globale

Type suggéré : `flowchart LR` avec regroupements.

**Composants et regroupements :**

| Regroupement | Éléments |
|---|---|
| Sources | 3 fichiers CSV (stations, mesures, instantané) |
| Traitement des données | dlt → DuckDB → dbt |
| Orchestration | Dagster *(pilote ingestion, dbt, entraînement, supervision)* |
| Apprentissage | `train.py` (XGBoost) |
| Suivi & artefacts | MLflow `:4902` → MinIO `:4906` |
| Service | FastAPI `:4904` *(3 endpoints + console web servie à la racine)* |
| Supervision | `monitoring.py` → PostgreSQL `:4901` → Grafana `:4905` |
| Industrialisation | GitHub Actions → Komodo |

**Liaisons à représenter :**
- CSV → dlt → DuckDB → dbt → DuckDB *(couche mart)*
- Dagster pilote : dlt, dbt, `train.py`, `monitoring.py` *(traits pointillés, pour distinguer l'orchestration du flux de données)*
- `train.py` → MLflow *(métriques et paramètres)* ; MLflow → MinIO *(artefacts)*
- MLflow *(registre)* → FastAPI *(chargement du modèle)*
- `monitoring.py` sonde FastAPI, lit MLflow, lit DuckDB → écrit dans PostgreSQL → lu par Grafana
- GitHub Actions valide le dépôt ; Komodo déploie sur le serveur

Fais apparaître **Dagster comme chef d'orchestre**, visuellement distinct du flux de données lui-même.

---

### Schéma 2 — Flux de données, de la source au mart

Type suggéré : `flowchart TD`, linéaire.

```
3 fichiers CSV (instantanés OpenAQ, 35 stations)
        ↓  dlt — ingestion idempotente
DuckDB · schéma raw
   delhi_locations · delhi_measurements · delhi_latest
        ↓  dbt
stg_delhi_measurements  (vue)
   nettoyage · indicateur de validité par polluant
   variables temporelles en heure locale indienne
   dérivation des saisons indiennes
        ↓
int_delhi_cleaned  (vue)
   déduplication
        ↓
mart_delhi_hourly  (table) — 4 332 lignes
   pivot des 6 polluants · agrégation horaire
   calcul de l'AQI NAQI · catégorisation
        ↓
train.py  (XGBoost)
```

Annote latéralement les points de contrôle qualité : **7 tests dbt** et **9 tests pytest** couvrant les 5 dimensions.

---

### Schéma 3 — Graphe des assets Dagster

Type suggéré : `flowchart LR`.

- Chaîne principale : `ingest_delhi_data` → `run_dbt` → `train_model`, regroupée sous *Job `delhi_daily_pipeline` — quotidien à 1 h*
- Asset indépendant : `collect_monitoring`, regroupé sous *Job `monitoring_job` — horaire*

Indique sous chaque asset ce qu'il déclenche : `pipeline/ingestion.py`, `dbt run` + `dbt test`, `ml/train.py`, `pipeline/monitoring.py`.

> Ce schéma peut aussi être remplacé par une capture de l'onglet Lineage de Dagster, qui l'affiche nativement.

---

### Schéma 4 — Cycle de vie du modèle

**Le plus important du rapport.** Il illustre la gouvernance du modèle, partie distinctive du projet.

Type suggéré : `flowchart TD` avec un nœud de décision.

**Enchaînement :**

1. `train_model` entraîne une nouvelle version
2. Artefacts déposés sur MinIO, métriques enregistrées dans MLflow
3. Nouvelle version créée au registre — **l'alias `champion` ne bouge pas**
4. Nœud de décision : *« la nouvelle version est-elle meilleure ? »*
   - **Non** → le champion reste en place, aucune action
   - **Oui** → l'alias `champion` est déplacé manuellement dans MLflow
5. `POST /reload` sur l'API
6. L'API sert la nouvelle version, **sans redéploiement ni interruption**

Fais clairement apparaître que l'étape 4 est une **décision humaine**, et non une bascule automatique — c'est le point à retenir. Signale en annotation que le tout premier modèle fait exception : il est promu automatiquement pour amorcer le service.

---

## 8. Éléments fournis par l'équipe

Ces éléments ne sont pas générés — signale-les dans le rapport par un encadré visible :

- **Captures d'écran** : interface MLflow (runs et métriques), documentation Swagger avec une réponse `/predict`, tableau de bord Grafana
- **Noms des membres** et répartition des rôles
- **Dates réelles** des sprints
- Toute mention `[À COMPLÉTER]`

---

## 9. Ce qu'il ne faut pas faire

- **N'invente aucun chiffre.** Si une métrique n'est pas dans la section 3, ne la cite pas.
- **Ne prétends pas** que les données proviennent d'appels API en temps réel.
- **Ne présente pas** la promotion comme automatique.
- **Ne cite aucune métrique absente de la section 3**, même plausible.
- **Ne gonfle pas** les performances du modèle. Un R² de 0,18 se présente tel quel, avec son explication.
- **Ne rédige pas** de section théorique générale sur le MLOps sans lien direct avec ce projet.
