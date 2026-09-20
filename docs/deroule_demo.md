# Déroulé de la démo — Delhi Air Quality MLOps

Format imposé : **15 min présentation · 10 min démo · 5 min questions**
Ce document couvre la **démo (10 min)** et la préparation aux questions.

---

## 1. Checklist — à faire avant d'entrer dans la salle

Ne saute aucune ligne. La plupart des démos ratées le sont pour une de ces raisons.

- [ ] **Relancer `train_model`** — si l'expérience `delhi_air_quality` est vide, tu ouvriras MLflow sur du néant
- [ ] Vérifier que les **schedules sont activés** dans Dagster → Automation
- [ ] Lancer **`monitoring_job`** une fois → Grafana affiche des données fraîches
- [ ] **Ouvrir la console** `http://exp.s3.fsbm.ma:4904` et vérifier le panneau *État du service* :
  *Modèle chargé* doit afficher **oui**, et *Mode de résolution* **`alias @champion`** avec un numéro de version
- [ ] Ouvrir les **6 onglets dans l'ordre du déroulé** (voir §2) et s'y connecter :
  - Dagster `:4903` · MLflow `:4902` · **Console API `:4904`** · Grafana `:4905` · MinIO `:4906` (`minioadmin`/`minioadmin`) · GitHub
- [ ] Avoir **deux versions de modèle** au registre, dont une non promue — c'est le cœur de la démo

---

## 2. Déroulé minuté

### 2.0 — Lancer le pipeline AVANT de parler (0:00)

**Premier geste, avant toute explication** : dans Dagster, lance `delhi_daily_pipeline`.

Il tourne ~45 s en tâche de fond pendant que tu présentes l'architecture. Tu reviendras dessus en 2.1 pour le montrer terminé, en vert. Ça évite le silence gênant d'une attente en direct, et ça montre que tu maîtrises ton outil.

---

### 2.1 — Architecture + orchestration (0:00 → 2:30)

**Pendant que le pipeline tourne**, sur un schéma :

> « Les données passent par dlt pour l'ingestion, DuckDB pour le stockage analytique, dbt pour les transformations, Dagster orchestre l'ensemble, le modèle est suivi dans MLflow avec ses artefacts sur MinIO, servi par FastAPI, le tout conteneurisé, validé par une CI et supervisé dans Grafana. »

Puis **retour sur Dagster** :

- Onglet **Lineage / Assets** → le graphe de dépendances `ingest_delhi_data → run_dbt → train_model`
- Le run lancé en 2.0 est maintenant **vert** → ouvre-le, montre les logs qui défilent
- Onglet **Automation** → les deux schedules actifs

> « Le pipeline complet tourne chaque nuit, la collecte de supervision toutes les heures. »

---

### 2.2 — Qualité des données (2:30 → 3:45)

Dans les logs du run, déroule la sortie de `run_dbt` :

> « dbt exécute les transformations puis **7 tests de qualité**, tous passés. »

Puis montre rapidement les deux fichiers :

- `data_contracts/air_quality_contract.yaml` → « le contrat définit le schéma attendu et les seuils »
- `tests/test_data_quality.py` → « 9 tests couvrant les 5 dimensions exigées : complétude, validité, cohérence, intégrité, fraîcheur »

Cite **le test de cohérence métier**, c'est celui qui impressionne :

> « Celui-ci vérifie que la pollution hivernale dépasse celle de la mousson — un invariant connu de Delhi. Si les données le contredisent, c'est qu'elles sont corrompues. »

---

### 2.3 — MLflow : suivi et registre (3:45 → 5:00)

- Expérience `delhi_air_quality` → la liste des runs
- Ouvre le dernier run → **paramètres**, **métriques** (RMSE, MAE, R²)
- Onglet **Models** du run → *(surtout pas l'onglet Artifacts, il est vide par conception en MLflow 3.x)*
- Onglet **Models** global → `DelhiAirQualityModel`, ses versions, l'alias **champion**

> « Chaque entraînement est tracé et versionné. Le registre indique quelle version est en production via l'alias champion. »

---

### 2.4 — MinIO : stockage objet des artefacts (5:00 → 5:30)

Console MinIO `:4906` → bucket `mlflow` → descends jusqu'aux fichiers du modèle.

> « Les artefacts ne sont pas sur le disque d'un conteneur mais dans un stockage objet compatible S3, servi en mode proxy par MLflow. C'est ce qui rend le modèle accessible à tous les services indépendamment du système de fichiers. »

---

### 2.5 — LE MOMENT FORT : cycle de vie du modèle (5:30 → 7:30)

C'est la séquence qui démontre le plus de maîtrise. **Tout se joue sur deux onglets**, la console API et MLflow — aucun terminal. Enchaîne les quatre gestes sans commenter longuement.

**1. Quelle version est servie ?**
Onglet **console API** `:4904` → panneau *État du service*, ligne **Version servie**.
> « L'API sert actuellement la version 12, et la ligne en dessous indique qu'elle suit l'alias champion. »

**2. Comparer dans MLflow**
Ouvre les deux versions côte à côte, compare les RMSE.
> « La version 14 est meilleure. Mais elle n'est pas en production — l'entraînement ne promeut rien automatiquement. »

**3. Promouvoir** — déplace l'alias `champion` sur la v14 dans MLflow.

**4. Mise en service à chaud**
Retour sur la console → bouton **Recharger le modèle**. La ligne *Version servie* passe à **14** sous les yeux du jury.

> « Le changement est effectif **sans redéploiement ni interruption**. La décision de promotion reste humaine : on ne met pas en production un modèle sans avoir comparé ses métriques. »

*Si on te demande ce que fait ce bouton* : il appelle `POST /reload`, qui vide le cache du modèle et le recharge depuis le registre. La console n'a aucune logique métier — elle ne fait qu'appeler les trois endpoints de l'API.

---

### 2.6 — API en service (7:30 → 8:15)

Reste sur la console → formulaire **Prédiction** (déjà pré-rempli avec un scénario hivernal réaliste) → **Prédire**.

> « Prédiction du PM2.5, catégorie selon le standard indien NAQI, et la version du modèle réellement utilisée. »

Souligne que `version du modèle` affichée sous le résultat correspond à la version du registre — cohérent avec ce qui vient d'être promu en 2.5.

Puis, **en une phrase**, bascule sur `:4904/docs` :

> « L'API est aussi auto-documentée : le schéma d'entrée, les contraintes de validation et les réponses sont générés depuis le code. »

Ne t'attarde pas sur Swagger, la démonstration vient d'être faite sur la console.

---

### 2.7 — Supervision (8:15 → 9:15)

Grafana `:4905` → dashboard, parcours les 4 sections :

| Section | Phrase |
|---|---|
| Disponibilité | « uptime des endpoints sur 72 h » |
| Temps de réponse | « latence médiane par endpoint » |
| Métriques ML | « évolution de RMSE et MAE en production » |
| Dérive | « distribution courante comparée à une référence, avec seuil d'alerte » |

> « Ces données sont collectées toutes les heures par un job Dagster qui sonde l'API, lit MLflow et compare les distributions. »

---

### 2.8 — CI/CD et collaboration (9:15 → 9:45)

GitHub → onglet **Pull requests** → montre les **2 PR fusionnées**.

> « Chaque évolution passe par une branche et une pull request. »

Puis le fichier `.github/workflows/ci.yml` :

> « Trois jobs à chaque commit : reconstruction complète de la chaîne de données suivie des tests, build Docker avec vérification du endpoint de santé, et analyse statique. »

**Ouvre l'onglet Actions** → les **trois jobs au vert**.

> « Chaque commit déclenche ces trois validations. Elles nous ont réellement servi : le linter a intercepté deux références à des variables renommées, qui auraient provoqué une erreur silencieuse en production. »

---

### 2.9 — Conclusion (9:45 → 10:00)

> « Pipeline versionné, orchestré et planifié ; qualité des données contractualisée et testée ; modèle tracé, versionné et promu de façon contrôlée ; service supervisé sur quatre axes ; chaque commit validé automatiquement. »

---

## 3. Plan de repli

| Si… | Alors |
|---|---|
| Le pipeline Dagster échoue en direct | Ouvre un run précédent réussi : « voici une exécution complète » |
| L'API renvoie 503 | Bouton **Recharger le modèle** sur la console. Si ça persiste, montre Swagger `/docs` et explique le flux |
| La console `:4904` ne s'affiche pas | Bascule sur Swagger `/docs` : les trois mêmes endpoints y sont exécutables |
| Grafana est vide | Lance `monitoring_job` et reviens-y en fin de démo |
| MLflow est lent | Passe à la suite, reviens-y si le temps le permet |
| Une URL ne répond pas | Ne t'acharne pas devant le jury : enchaîne, reviens à la fin |

**Règle générale** : ne jamais déboguer en direct plus de 15 secondes. Enchaîne et reviens-y.

---

## 4. Intégration continue — état

Les **trois jobs passent au vert** : `test`, `build-docker` et `lint`. Tu peux ouvrir l'onglet Actions sans réserve.

Vérifie tout de même la veille que le dernier commit sur `main` est bien vert — un déploiement de dernière minute pourrait l'avoir cassé.

---

## 5. Questions probables et réponses

**« Les données viennent-elles de l'API OpenAQ en direct ? »**
> Non. On ingère des instantanés CSV de ces mêmes stations. C'est un choix de reproductibilité : l'intégration continue reconstruit toute la chaîne de données à chaque commit, et elle ne peut pas dépendre de la disponibilité d'une API externe ni d'une clé secrète. Le connecteur dlt reste identique, seule la source change.

**« Pourquoi XGBoost et pas Scikit-Learn ? »**
> XGBoost est plus performant sur des données tabulaires. Scikit-Learn est utilisé pour le découpage et les métriques. Le découpage est temporel, sans mélange aléatoire, pour ne pas fuiter du futur vers le passé.

**« Votre R² est faible (0,18). »**
> Oui, et c'est honnête à dire : prédire le PM2.5 à l'heure à partir des seuls autres polluants et de variables calendaires est difficile. Il manque la météo — vent, température, humidité — qui est le facteur dominant à Delhi. C'est la première amélioration identifiée dans notre backlog.

**« Pourquoi la promotion n'est-elle pas automatique ? »**
> Nos écarts de RMEE entre versions sont de l'ordre de 1 à 2 µg/m³ sur des valeurs autour de 150. Une règle automatique promouvrait sur du bruit. Nous préférons une promotion explicite après comparaison. La logique de comparaison conditionnelle est identifiée au backlog.

**« Pourquoi MinIO plutôt que le système de fichiers ? »**
> Avec un stockage fichier, chaque conteneur résout les chemins sur son propre système de fichiers : le service d'entraînement écrivait à un endroit invisible pour l'API. Le stockage objet supprime cette dépendance — les artefacts transitent par HTTP et sont accessibles à tous les services.

**« Vos identifiants MinIO sont en clair dans le dépôt. »**
> Exact, ce sont les identifiants par défaut, et c'est une faiblesse identifiée. En production ils seraient injectés par un gestionnaire de secrets. *(Si tu as le temps avant la démo, corrige-le et cette question devient un point fort.)*

**« Comment garantissez-vous la reproductibilité ? »**
> Code versionné sur Git, images Docker à versions épinglées, données transformées par dbt de façon déterministe, et chaque modèle tracé dans MLflow avec ses paramètres, ses métriques et le commit source.

**« Que se passe-t-il si les données dérivent ? »**
> Le tableau de bord le détecte : on compare la distribution courante à une référence et on déclenche un indicateur au-delà d'un seuil. Le réentraînement automatique sur dérive est au backlog.

---

## 6. Ce qui joue en ta faveur

Si le jury creuse la partie opérationnelle, tu as vécu de vrais incidents à raconter — backend MLflow devenu obsolète, artefacts non partagés entre conteneurs, image jamais reconstruite au déploiement, conflit de dépendances dans les transformations, données de démonstration masquant les mesures réelles. Chacun a été diagnostiqué et corrigé.

C'est exactement ce qui distingue un projet réellement déployé d'un projet qui tourne seulement sur un poste de développement.
