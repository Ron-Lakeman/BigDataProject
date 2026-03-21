"""
ML Model Training and Prediction Flow for Prefect Dashboard

This script implements the machine learning pipeline from ml_model_v1.ipynb
as a Prefect flow that can be orchestrated and monitored via Prefect dashboard.

The flow:
1. Loads processed features from gold layer
2. Trains XGBoost ensemble model (10 seeds) with hyperparameter tuning
3. Generates predictions on validation and test sets
4. Saves predictions to gold directory
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
from typing import Dict, Tuple, List

from prefect import flow, task, get_run_logger
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import StratifiedGroupKFold, RandomizedSearchCV
from sklearn.metrics import roc_auc_score, f1_score, classification_report
from xgboost import XGBClassifier
from sklearn.base import clone

# Feature definitions (extracted from notebook)
temporal_features = [
    "log_review_age_days", "log_product_review_count",
    "reviews_per_day", "log_review_rank", "reviews_in_3day_window",
    "log_days_since_first_review",
]

language_features = [
    "log_body_word_count", "type_token_ratio", "avg_word_length",
    "log_sentence_count", "body_lang_zscore", "ttr_lang_zscore", "lexical_richness",
    "avg_sentence_length", "flesch_lang_zscore",
    "sentences_per_paragraph", "body_cat_zscore", "headline_body_ratio",
    "body_bigram_diversity", "headline_word_count", "log_exclamation_count",
]

embedding_features = [
    "headline_body_cosine_sim", "char_ngram_entropy",
    "title_body_overlap", "body_embedding_norm", "title_body_cosine_sim",
]

sentiment_features = [
    "body_sentiment_score", "body_sentiment_polarity",
    "polarity_lang_zscore", "sentiment_arc",
    "headline_sentiment_polarity",
]

quality_features = [
    "digit_density", "uppercase_ratio", "repetition_ratio", "title_word_coverage",
    "superlative_density", "first_person_density", "specificity_score",
]

context_features = [
    "product_popularity_pctile", "category_review_density",
    "vine_category_rate", "verified_category_rate",
    "verified_cat_target_enc",
    "word_count_cat_pctile", "category_product_count",
]

interaction_features = [
    "verified_x_log_words", "early_x_log_words",
]

boolean_features = [
    "is_early_review", "verified_binary",
    "is_english", "is_french", "is_german", "is_hungarian",
    "is_negative_sentiment",
]

numeric_features = (temporal_features + language_features + embedding_features +
                    sentiment_features + quality_features + context_features +
                    interaction_features)

all_features = numeric_features + boolean_features

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


@task(name="Load ML Features")
def load_ml_features() -> pd.DataFrame:
    """Load the latest ML features from gold directory."""
    logger = get_run_logger()
    gold_dir = GOLD_DIR

    ml_files = sorted(gold_dir.glob("ml_features_load_date=*.parquet"))
    if not ml_files:
        raise FileNotFoundError("No ml_features parquet found. Run all_features_to_gold.py first.")

    latest = ml_files[-1]
    df = pd.read_parquet(latest)
    logger.info(f"Loaded: {latest.name} -> shape {df.shape}")
    logger.info(f"dataset_split distribution:\n{df['dataset_split'].value_counts()}")

    return df


@task(name="Enrich Features")
def enrich_features(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich dataframe with additional gold features."""
    logger = get_run_logger()
    gold_dir = GOLD_DIR

    # Load additional feature files
    sent_files = sorted(gold_dir.glob("sentiment_features_load_date=*.parquet"))
    if sent_files:
        sent = pd.read_parquet(sent_files[-1],
                               columns=["_index", "avg_sentence_length", "flesch_lang_zscore",
                                       "headline_sentiment_polarity", "title_body_overlap",
                                       "body_bigram_diversity"])
        df = df.merge(sent, on="_index", how="left")

    spec_files = sorted(gold_dir.glob("specificity_features_load_date=*.parquet"))
    if spec_files:
        spec = pd.read_parquet(spec_files[-1],
                               columns=["_index", "superlative_density", "first_person_density",
                                       "specificity_score"])
        df = df.merge(spec, on="_index", how="left")

    emb_files = sorted(gold_dir.glob("embedding_features_load_date=*.parquet"))
    if emb_files:
        emb = pd.read_parquet(emb_files[-1],
                              columns=["_index", "body_embedding_norm", "title_body_cosine_sim"])
        df = df.merge(emb, on="_index", how="left")

    lex_files = sorted(gold_dir.glob("lexical_features_load_date=*.parquet"))
    if lex_files:
        lex = pd.read_parquet(lex_files[-1],
                              columns=["_index", "sentences_per_paragraph", "body_cat_zscore",
                                      "headline_body_ratio", "exclamation_count"])
        df = df.merge(lex, on="_index", how="left")
        df["log_exclamation_count"] = np.log1p(df["exclamation_count"])

    ctx_files = sorted(gold_dir.glob("product_context_load_date=*.parquet"))
    if ctx_files:
        ctx = pd.read_parquet(ctx_files[-1], columns=["_index", "category_product_count"])
        df = df.merge(ctx, on="_index", how="left")

    temp_files = sorted(gold_dir.glob("temporal_features_load_date=*.parquet"))
    if temp_files:
        temp = pd.read_parquet(temp_files[-1], columns=["_index", "days_since_first_review"])
        df = df.merge(temp, on="_index", how="left")
        df["log_days_since_first_review"] = np.log1p(df["days_since_first_review"])

    ps_files = sorted(gold_dir.glob("pioneer_synthesis_load_date=*.parquet"))
    if ps_files:
        ps = pd.read_parquet(ps_files[-1],
                             columns=["_index", "product_tfidf_novelty", "synthesis_score"])
        df = df.merge(ps, on="_index", how="left")

    # Derive missing features
    if "log_review_rank" not in df.columns:
        df["log_review_rank"] = np.log1p(df["review_relative_rank"])

    if "log_product_review_count" not in df.columns:
        df["log_product_review_count"] = np.log1p(df["product_review_count"])

    if "log_sentence_count" not in df.columns:
        df["log_sentence_count"] = np.log1p(df["sentence_count_approx"])

    # Language features
    lang_dist = df['detected_language'].value_counts()
    df['is_english'] = (df['detected_language'] == 'en').astype(float)
    df['is_french'] = (df['detected_language'] == 'fr').astype(float)
    df['is_german'] = (df['detected_language'] == 'de').astype(float)
    df['is_hungarian'] = (df['detected_language'] == 'hu').astype(float)
    df['is_negative_sentiment'] = (df['body_sentiment_label'] == 'negative').astype(float)

    # Interaction features
    vb = df['verified_binary'].astype(float)
    er = df['is_early_review'].astype(float)
    lbw = df['log_body_word_count'].astype(float)
    df['verified_x_log_words'] = vb * lbw
    df['early_x_log_words'] = er * lbw

    # Verified category target encoding
    train_mask = df["dataset_split"] == "train"
    label_int = df.loc[train_mask, "label"].astype(int)
    global_mean = float(label_int.mean())
    vc_enc = label_int.groupby(df.loc[train_mask, "verified_x_category"]).mean()
    df["verified_cat_target_enc"] = (
        df["verified_x_category"].map(vc_enc).fillna(global_mean)
    )

    logger.info(f"Enriched dataframe shape: {df.shape}")
    return df


@task(name="Prepare Data for Training")
def prepare_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, np.ndarray]:
    """Prepare train data for model training."""
    logger = get_run_logger()

    train_df = df[df['dataset_split'] == 'train'].copy()

    # Convert boolean features to float
    for col in boolean_features:
        if col in train_df.columns:
            train_df[col] = train_df[col].astype(float)

    # Filter to available features
    missing_cols = [f for f in all_features if f not in train_df.columns]
    if missing_cols:
        logger.warning(f"Missing features: {missing_cols}")
        used_features = [f for f in all_features if f in train_df.columns]
    else:
        used_features = all_features

    X_all = train_df[used_features]
    y_all = train_df['label'].astype(int)
    groups_all = train_df['product_parent'].values

    logger.info(f"Training data shape: {X_all.shape}")
    logger.info(f"Unique products: {len(set(groups_all)):,}")
    logger.info(f"Label distribution: Unhelpful: {(y_all==0).sum()}, Helpful: {(y_all==1).sum()}")

    return X_all, y_all, groups_all


# @task(name="Create Preprocessing Pipeline")
def create_preprocessing_pipeline() -> Pipeline:
    """Create the preprocessing pipeline."""
    numeric_transformer = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()),
    ])

    bool_transformer = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
    ])

    transformers = [
        ('num', numeric_transformer, numeric_features),
        ('bool', bool_transformer, boolean_features),
    ]

    preprocessor = ColumnTransformer(transformers)
    return preprocessor


@task(name="Train XGBoost Ensemble Model")
def train_xgboost_ensemble_model(X_all: pd.DataFrame, y_all: pd.Series, groups_all: np.ndarray,
                                preprocessor: Pipeline) -> Pipeline:
    """Train XGBoost ensemble model with multiple seeds."""
    logger = get_run_logger()

    # Best parameters from notebook
    best_params = {
        'classifier__subsample': 0.75,
        'classifier__reg_lambda': 2.0,
        'classifier__reg_alpha': 0.1,
        'classifier__n_estimators': 1000,
        'classifier__min_child_weight': 10,
        'classifier__max_depth': 5,
        'classifier__learning_rate': 0.01,
        'classifier__gamma': 0,
        'classifier__colsample_bytree': 0.5,
    }

    # Multi-seed ensemble (XGBoost only)
    SEEDS = [67, 42, 0, 123, 7, 13, 99, 17, 256, 31]
    cv = StratifiedGroupKFold(n_splits=5)

    # Initialize ensemble predictions array
    oof_xgb_acc = np.zeros(len(X_all))

    logger.info(f"Training {len(SEEDS)}-seed XGBoost ensemble...")

    for seed in SEEDS:
        # Create model with this seed
        xgb_pipeline = Pipeline([('prep', preprocessor),
                                ('classifier', XGBClassifier(random_state=seed, eval_metric='logloss', verbosity=0))])
        xgb_s = clone(xgb_pipeline).set_params(**best_params)

        # Out-of-fold predictions for this seed
        oof_xgb_s = np.zeros(len(X_all))
        for fold, (tr_idx, va_idx) in enumerate(cv.split(X_all, y_all, groups=groups_all)):
            oof_xgb_s[va_idx] = xgb_s.fit(X_all.iloc[tr_idx], y_all.iloc[tr_idx]).predict_proba(X_all.iloc[va_idx])[:, 1]

        oof_xgb_acc += oof_xgb_s
        auc_score = roc_auc_score(y_all, oof_xgb_s)
        logger.info(f"  seed={seed}  XGBoost AUC={auc_score:.4f}")

    # Average predictions across all seeds
    oof_xgb = oof_xgb_acc / len(SEEDS)
    ensemble_auc = roc_auc_score(y_all, oof_xgb)
    logger.info(f"Ensemble OOF AUC: {ensemble_auc:.4f}")

    # Train final ensemble model on full dataset with multiple seeds
    logger.info(f"Training final {len(SEEDS)}-seed ensemble on full train set...")

    # Initialize prediction accumulators for validation and test sets
    ensemble_models = []

    for seed in SEEDS:
        xgb_pipeline = Pipeline([('prep', preprocessor),
                                ('classifier', XGBClassifier(random_state=seed, eval_metric='logloss', verbosity=0))])
        final_model = clone(xgb_pipeline).set_params(**best_params)
        final_model.fit(X_all, y_all)
        ensemble_models.append(final_model)
        logger.info(f"  seed={seed} model trained")

    # Create a wrapper that averages predictions from all ensemble models
    class XGBoostEnsemble:
        def __init__(self, models):
            self.models = models
            # Mirror sklearn Pipeline access pattern used downstream.
            if models:
                self.named_steps = {'prep': models[0].named_steps['prep']}
            else:
                self.named_steps = {}

        def predict_proba(self, X):
            # Average predictions across all models
            probas = np.zeros((len(X), 2))
            for model in self.models:
                probas += model.predict_proba(X)
            return probas / len(self.models)

        def predict(self, X):
            probas = self.predict_proba(X)
            return (probas[:, 1] >= 0.5).astype(int)

    ensemble_model = XGBoostEnsemble(ensemble_models)
    logger.info("XGBoost ensemble model trained successfully")

    return ensemble_model


@task(name="Generate Predictions")
def generate_predictions(df: pd.DataFrame, model: Pipeline) -> Dict[str, pd.DataFrame]:
    """Generate predictions on validation and test sets."""
    logger = get_run_logger()

    predictions = {}

    for split in ["validation", "test"]:
        split_df = df[df["dataset_split"] == split].copy()
        split_df = split_df.sort_values("_index").reset_index(drop=True)

        # Convert boolean features
        for col in boolean_features:
            if col in split_df.columns:
                split_df[col] = split_df[col].astype(float)

        # Filter to training features
        if hasattr(model, 'named_steps') and 'prep' in model.named_steps:
            prep = model.named_steps['prep']
        elif hasattr(model, 'models') and len(model.models) > 0:
            prep = model.models[0].named_steps['prep']
        else:
            raise AttributeError("Model does not expose a fitted preprocessor for feature extraction")

        pred_features = list(prep.get_feature_names_out())
        pred_features = [f.split('__', 1)[-1] for f in pred_features]  # Remove transformer prefix

        X_split = split_df[pred_features]

        # Generate predictions
        probas = model.predict_proba(X_split)[:, 1]
        preds = (probas >= 0.50).astype(int)  # Fixed threshold

        # Create prediction dataframe
        pred_df = pd.DataFrame({
            "_index": split_df["_index"].values,
            "product_id": split_df["product_id"].values,
            "predicted_label": preds,
            "proba_helpful": probas.round(4),
        })

        predictions[split] = pred_df
        logger.info(f"[{split}] Generated predictions for {len(split_df):,} samples. "
                   f"Predicted helpful: {preds.mean()*100:.1f}%")

    return predictions


@task(name="Save Predictions")
def save_predictions(predictions: Dict[str, pd.DataFrame]):
    """Save predictions to gold directory."""
    logger = get_run_logger()
    gold_dir = GOLD_DIR

    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    for split, pred_df in predictions.items():
        # Save as CSV (boolean labels)
        csv_path = gold_dir / f"predictions_{split}_{timestamp}.csv"
        pd.Series(pred_df["predicted_label"].astype(bool)).to_csv(
            csv_path, index=False, header=False)
        logger.info(f"Saved CSV predictions to {csv_path}")

        # Save as Parquet (detailed predictions)
        parquet_path = gold_dir / f"predictions_{split}_{timestamp}.parquet"
        pred_df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved detailed predictions to {parquet_path}")


@flow(name="ML Model Training and Prediction", log_prints=True)
def ml_model_flow():
    """
    Complete ML pipeline: load data, train XGBoost ensemble model, generate predictions.

    This flow implements the machine learning model from ml_model_v1.ipynb
    as a Prefect-orchestrated pipeline, using a 10-seed XGBoost ensemble
    for improved stability and performance.
    """
    logger = get_run_logger()

    logger.info("Starting ML Model Training and Prediction Flow")

    # Load and prepare data
    df = load_ml_features()
    df = enrich_features(df)
    X_all, y_all, groups_all = prepare_data(df)

    # Create and train model
    preprocessor = create_preprocessing_pipeline()
    model = train_xgboost_ensemble_model(X_all, y_all, groups_all, preprocessor)

    # Generate predictions
    predictions = generate_predictions(df, model)

    # Save predictions
    save_predictions(predictions)

    logger.info("ML Model Flow completed successfully")


if __name__ == "__main__":
    ml_model_flow()