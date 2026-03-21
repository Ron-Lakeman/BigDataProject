"""
Gold Layer – Prefect Flow
=========================
Orchestrates all feature-engineering steps from Silver parquet files
to gold.ml_features in DuckDB and parquet exports.

Usage:
    python src/processing/gold_flow.py              # run the full gold flow
    python src/processing/gold_flow.py --dry-run    # check silver data exists, then exit
"""

import sys
import os
import re
import unicodedata
from collections import Counter
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import textstat
from langdetect import detect, LangDetectException, DetectorFactory
from prefect import flow, task, get_run_logger
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from transformers import pipeline

DetectorFactory.seed = 0

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _project_root() -> Path:
    """Resolve project root from this file's location (src/processing/)."""
    return Path(__file__).resolve().parent.parent.parent


def _db_path(project_root: Path) -> str:
    return str(project_root / "ProjectData.duckdb")


# ---------------------------------------------------------------------------
# Task 1 – Load silver data
# ---------------------------------------------------------------------------

@task(name="load-silver-data")
def load_silver_data(project_root: Path) -> pd.DataFrame:
    logger = get_run_logger()
    silver_parts = []
    for split in ["train", "test", "validation"]:
        p = project_root / "data" / "silver" / split / f"cleaned_{split}.parquet"
        if not p.exists():
            raise FileNotFoundError(
                f"Silver split not found: {p}. Run clean_transform_to_silver first."
            )
        df = pd.read_parquet(p)
        if "label" not in df.columns:
            df["label"] = None
        silver_parts.append(df)

    silver_df = pd.concat(silver_parts, ignore_index=True)
    silver_df["_index"] = range(len(silver_df))
    logger.info(
        "Loaded silver data: %d rows | splits: %s",
        len(silver_df),
        silver_df["dataset_split"].value_counts().to_dict(),
    )
    return silver_df


# ---------------------------------------------------------------------------
# Task 2 – Temporal features (pure SQL)
# ---------------------------------------------------------------------------

@task(name="compute-temporal-features")
def compute_temporal_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.register("silver_data", silver_df)
        con.execute("DROP TABLE IF EXISTS gold.review_temporal_features")
        con.execute("""
            CREATE TABLE gold.review_temporal_features AS
            WITH base AS (
                SELECT *,
                    MAX(review_date) OVER ()                              AS dataset_max_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_parent ORDER BY review_date
                    )                                                     AS review_relative_rank,
                    COUNT(*) OVER (PARTITION BY product_parent)           AS product_review_count,
                    MIN(review_date) OVER (PARTITION BY product_parent)   AS first_review_date,
                    MAX(review_date) OVER (PARTITION BY product_parent)   AS last_review_date,
                    COUNT(*) OVER (
                        PARTITION BY product_parent, review_date
                    )                                                     AS reviews_same_day
                FROM silver_data
            ),
            ranked AS (
                SELECT *,
                    DATE_DIFF('day', review_date, dataset_max_date)       AS review_age_days,
                    DATE_DIFF('day', first_review_date, review_date)      AS days_since_first_review,
                    MONTH(review_date)                                    AS review_month,
                    DAYOFWEEK(review_date)                                AS review_dayofweek,
                    CASE
                        WHEN review_relative_rank <= CEIL(product_review_count * 0.10)
                        THEN TRUE ELSE FALSE
                    END                                                   AS is_early_review,
                    product_review_count * 1.0
                        / NULLIF(DATE_DIFF('day', first_review_date, last_review_date), 0)
                        AS reviews_per_day
                FROM base
            )
            SELECT * FROM ranked
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_temporal_features").fetchone()[0]
    logger.info("Temporal features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 3 – Language detection + lightweight text features (Python)
# ---------------------------------------------------------------------------

def _detect_language(text) -> str:
    if text is None or str(text).strip() == "" or len(str(text).strip()) < 10:
        return "unknown"
    try:
        return detect(str(text))
    except LangDetectException:
        return "unknown"


def _type_token_ratio(text):
    if text is None or str(text).strip() == "":
        return None
    tokens = str(text).lower().split()
    return len(set(tokens)) / len(tokens) if tokens else None


def _repetition_ratio(text):
    if not text or not str(text).strip():
        return None
    sentences = re.split(r"[.!?]+", str(text))
    sentences = [s.strip().lower() for s in sentences if s.strip()]
    if len(sentences) <= 1:
        return 0.0
    return (len(sentences) - len(set(sentences))) / len(sentences)


_TEXTSTAT_LANG_MAP = {
    "en": "en", "de": "de", "fr": "fr", "es": "es",
    "it": "it", "nl": "nl", "ru": "ru",
}


def _flesch_ease(row):
    lang = _TEXTSTAT_LANG_MAP.get(row.get("detected_language", ""), "en")
    text = row.get("review_body", "") or ""
    if not text.strip():
        return None
    textstat.set_lang(lang)
    try:
        return float(textstat.flesch_reading_ease(text))
    except Exception:
        return None


def _jaccard_sim(text_a, text_b):
    if not text_a or not text_b:
        return None
    a, b = set(str(text_a).lower().split()), set(str(text_b).lower().split())
    union = len(a | b)
    return len(a & b) / union if union > 0 else 0.0


def _overlap_ratio(text_source, text_target):
    if not text_source or not text_target:
        return None
    src = set(str(text_source).lower().split())
    tgt = set(str(text_target).lower().split())
    return len(src & tgt) / len(src) if src else None


def _bigram_diversity(text):
    if not text or not str(text).strip():
        return None
    tokens = str(text).lower().split()
    if len(tokens) < 2:
        return None
    bigrams = list(zip(tokens, tokens[1:]))
    return len(set(bigrams)) / len(bigrams)


@task(name="enrich-language-text-features")
def enrich_language_text_features(silver_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    df = silver_df.copy()
    logger.info("Detecting language...")
    df["detected_language"] = df["review_body"].apply(_detect_language)
    df["type_token_ratio"] = df["review_body"].apply(_type_token_ratio)
    df["repetition_ratio"] = df["review_body"].apply(_repetition_ratio)
    df["flesch_reading_ease"] = df[["review_body", "detected_language"]].apply(_flesch_ease, axis=1)
    df["headline_body_jaccard"] = df.apply(
        lambda r: _jaccard_sim(r["review_headline"], r["review_body"]), axis=1
    )
    df["title_body_jaccard"] = df.apply(
        lambda r: _jaccard_sim(r["product_title"], r["review_body"]), axis=1
    )
    df["title_body_overlap"] = df.apply(
        lambda r: _overlap_ratio(r["product_title"], r["review_body"]), axis=1
    )
    df["body_bigram_diversity"] = df["review_body"].apply(_bigram_diversity)
    logger.info(
        "Language distribution (top 5): %s",
        df["detected_language"].value_counts().head(5).to_dict(),
    )
    return df


# ---------------------------------------------------------------------------
# Task 4 – Lexical features (SQL, reads silver_with_lang in-memory view)
# ---------------------------------------------------------------------------

@task(name="compute-lexical-features")
def compute_lexical_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("silver_with_lang", silver_df)
        con.execute("DROP TABLE IF EXISTS gold.review_lexical_features")
        con.execute("""
            CREATE TABLE gold.review_lexical_features AS
            WITH text_stats AS (
                SELECT
                    _index, product_id, product_parent, product_title,
                    vine, verified_purchase, review_headline, review_body,
                    review_date, marketplace_id, product_category_id,
                    label, dataset_split, detected_language, type_token_ratio,

                    ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' '))     AS body_word_count,
                    CASE
                        WHEN review_headline IS NOT NULL AND TRIM(review_headline) != ''
                        THEN ARRAY_LENGTH(STRING_SPLIT(TRIM(review_headline), ' '))
                        ELSE 0
                    END                                                     AS headline_word_count,
                    (LENGTH(review_body) - LENGTH(REPLACE(review_body, '!', ''))) AS exclamation_count,
                    (LENGTH(review_body) - LENGTH(REPLACE(review_body, '?', ''))) AS question_count,
                    GREATEST(
                        LENGTH(review_body) - LENGTH(REPLACE(review_body, '.', '')), 1
                    )                                                        AS sentence_count_approx,
                    (LENGTH(REPLACE(review_body, ' ', '')) * 1.0)
                        / NULLIF(ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')), 0)
                                                                             AS avg_word_length,
                    CONCAT(vine, '_', CAST(marketplace_id AS VARCHAR))       AS vine_x_marketplace,
                    CONCAT(verified_purchase, '_', CAST(product_category_id AS VARCHAR))
                                                                             AS verified_x_category,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '<br[\\s]*/?>')), 0)
                      + COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '\\n\\n|\\r\\n\\r\\n')), 0)
                                                                             AS paragraph_break_count,
                    CASE
                        WHEN (
                            COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '<br[\\s]*/?>')), 0)
                          + COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '\\n\\n|\\r\\n\\r\\n')), 0)
                        ) >= 2
                        THEN TRUE ELSE FALSE
                    END                                                      AS has_structured_body,
                    GREATEST(
                        LENGTH(review_body) - LENGTH(REPLACE(review_body, '.', '')), 1
                    ) * 1.0 / NULLIF(
                        GREATEST(1,
                            COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '<br[\\s]*/?>')), 0)
                          + COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '\\n\\n|\\r\\n\\r\\n')), 0)
                          + 1
                        ), 0
                    )                                                        AS sentences_per_paragraph
                FROM silver_with_lang
                WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
            ),
            train_lang_bwc AS (
                SELECT detected_language,
                    AVG(body_word_count)    AS lang_mean_bwc,
                    STDDEV(body_word_count) AS lang_std_bwc
                FROM text_stats WHERE dataset_split = 'train'
                GROUP BY detected_language
            ),
            train_cat_bwc AS (
                SELECT product_category_id,
                    AVG(body_word_count)    AS cat_mean_bwc,
                    STDDEV(body_word_count) AS cat_std_bwc
                FROM text_stats WHERE dataset_split = 'train'
                GROUP BY product_category_id
            ),
            lang_norms AS (
                SELECT
                    ts.*,
                    (ts.body_word_count - tl.lang_mean_bwc) / NULLIF(tl.lang_std_bwc, 0)
                        AS body_lang_zscore,
                    (ts.body_word_count - tc.cat_mean_bwc)  / NULLIF(tc.cat_std_bwc,  0)
                        AS body_cat_zscore,
                    ts.headline_word_count * 1.0
                        / NULLIF(ts.body_word_count, 0)                      AS headline_body_ratio,
                    ts.exclamation_count * 1.0
                        / NULLIF(ts.sentence_count_approx, 0)                AS exclamation_density,
                    ts.question_count * 1.0
                        / NULLIF(ts.sentence_count_approx, 0)                AS question_density
                FROM text_stats ts
                LEFT JOIN train_lang_bwc tl ON tl.detected_language    = ts.detected_language
                LEFT JOIN train_cat_bwc  tc ON tc.product_category_id  = ts.product_category_id
            )
            SELECT * FROM lang_norms
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_lexical_features").fetchone()[0]
    logger.info("Lexical features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 5 – Load embedding model
# ---------------------------------------------------------------------------

@task(name="load-embedding-model")
def load_embedding_model() -> SentenceTransformer:
    logger = get_run_logger()
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    logger.info("Embedding dim: %d", model.get_sentence_embedding_dimension())
    return model


# ---------------------------------------------------------------------------
# Task 6 – Compute embeddings + cosine similarities
# ---------------------------------------------------------------------------

def _encode_field(model, series, batch_size=64, field_name=""):
    texts = series.fillna("").str.strip()
    valid_mask = (texts != "").values
    inputs = texts.where(texts != "", other=" ").tolist()
    embeddings = model.encode(
        inputs, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=False
    )
    return embeddings.astype(np.float32), valid_mask


def _cosine_sim_rowwise(a, b, mask_a, mask_b):
    a_norm = a / np.clip(np.linalg.norm(a, axis=1, keepdims=True), 1e-8, None)
    b_norm = b / np.clip(np.linalg.norm(b, axis=1, keepdims=True), 1e-8, None)
    sim = np.einsum("ij,ij->i", a_norm, b_norm).astype(float)
    sim[~(mask_a & mask_b)] = np.nan
    return sim


@task(name="compute-embeddings", timeout_seconds=3600)
def compute_embeddings(model: SentenceTransformer, silver_df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Encoding review_body...")
    body_emb,     body_valid     = _encode_field(model, silver_df["review_body"],     field_name="review_body")
    logger.info("Encoding review_headline...")
    headline_emb, headline_valid = _encode_field(model, silver_df["review_headline"], field_name="review_headline")
    logger.info("Encoding product_title...")
    title_emb,    title_valid    = _encode_field(model, silver_df["product_title"],   field_name="product_title")

    df = silver_df.copy()
    df["headline_body_cosine_sim"] = _cosine_sim_rowwise(headline_emb, body_emb, headline_valid, body_valid)
    df["title_body_cosine_sim"]    = _cosine_sim_rowwise(title_emb, body_emb, title_valid, body_valid)
    body_norms = np.linalg.norm(body_emb, axis=1).astype(float)
    body_norms[~body_valid] = np.nan
    df["body_embedding_norm"] = body_norms

    logger.info("Embeddings computed. body shape: %s", body_emb.shape)
    return df, body_emb, headline_emb, title_emb


# ---------------------------------------------------------------------------
# Task 7 – Write embedding features + PCA to DuckDB
# ---------------------------------------------------------------------------

@task(name="write-embedding-features")
def write_embedding_features(
    project_root: Path,
    silver_df: pd.DataFrame,
    body_emb: np.ndarray,
    headline_emb: np.ndarray,
    title_emb: np.ndarray,
) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        # Embedding features
        emb_df = silver_df[
            ["_index", "product_id", "label",
             "headline_body_cosine_sim", "title_body_cosine_sim", "body_embedding_norm"]
        ].copy()
        con.register("embedding_features_temp", emb_df)
        con.execute("DROP TABLE IF EXISTS gold.review_embedding_features")
        con.execute("CREATE TABLE gold.review_embedding_features AS SELECT * FROM embedding_features_temp")

        # PCA
        N_COMPONENTS = 15
        pca_model = PCA(n_components=N_COMPONENTS, random_state=42)
        body_pca = pca_model.fit_transform(body_emb)
        pca_col_names = [f"body_emb_pca_{i}" for i in range(N_COMPONENTS)]
        pca_df = pd.DataFrame(body_pca, columns=pca_col_names)
        pca_df["_index"]     = silver_df["_index"].values
        pca_df["product_id"] = silver_df["product_id"].values
        pca_df["label"]      = silver_df["label"].values
        con.register("pca_temp", pca_df)
        con.execute("DROP TABLE IF EXISTS gold.review_embedding_pca")
        con.execute("CREATE TABLE gold.review_embedding_pca AS SELECT * FROM pca_temp")

        n_emb = con.execute("SELECT COUNT(*) FROM gold.review_embedding_features").fetchone()[0]
        n_pca = con.execute("SELECT COUNT(*) FROM gold.review_embedding_pca").fetchone()[0]
    logger.info("Embedding features: %d rows | PCA: %d rows x %d components", n_emb, n_pca, N_COMPONENTS)


# ---------------------------------------------------------------------------
# Task 8 – Load sentiment model
# ---------------------------------------------------------------------------

@task(name="load-sentiment-model")
def load_sentiment_model():
    logger = get_run_logger()
    sentiment_pipe = pipeline(
        "text-classification",
        model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
        return_all_scores=True,
        truncation=True,
        device=-1,
    )
    logger.info("Sentiment model loaded.")
    return sentiment_pipe


# ---------------------------------------------------------------------------
# Task 9 – Score sentiment (Python)
# ---------------------------------------------------------------------------

def _score_sentiment(sentiment_pipe, texts, batch_size=32):
    clean = [str(t).strip() if t and str(t).strip() else " " for t in texts]
    valid = [t != " " for t in clean]
    results = sentiment_pipe(clean, batch_size=batch_size, truncation=True, max_length=512)
    rows = []
    for res, is_valid in zip(results, valid):
        if not is_valid:
            rows.append({"label": None, "score": None, "polarity": None})
            continue
        scores = {r["label"].lower(): r["score"] for r in res}
        pos = scores.get("positive", 0.0)
        neg = scores.get("negative", 0.0)
        top = max(res, key=lambda x: x["score"])
        rows.append({"label": top["label"], "score": top["score"], "polarity": float(pos - neg)})
    return pd.DataFrame(rows)


@task(name="compute-sentiment-scores", timeout_seconds=3600)
def compute_sentiment_scores(sentiment_pipe, silver_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    df = silver_df.copy()
    logger.info("Scoring body sentiment...")
    body_sent = _score_sentiment(sentiment_pipe, df["review_body"].tolist())
    df["body_sentiment_label"]    = body_sent["label"].values
    df["body_sentiment_score"]    = body_sent["score"].values
    df["body_sentiment_polarity"] = body_sent["polarity"].values

    logger.info("Scoring headline sentiment...")
    headline_sent = _score_sentiment(sentiment_pipe, df["review_headline"].tolist())
    df["headline_sentiment_label"]    = headline_sent["label"].values
    df["headline_sentiment_score"]    = headline_sent["score"].values
    df["headline_sentiment_polarity"] = headline_sent["polarity"].values

    df["sentiment_mismatch"] = np.where(
        df["body_sentiment_polarity"].isna() | df["headline_sentiment_polarity"].isna(),
        np.nan,
        np.abs(df["body_sentiment_polarity"] - df["headline_sentiment_polarity"]),
    )
    logger.info(
        "Body sentiment distribution: %s",
        df["body_sentiment_label"].value_counts().to_dict(),
    )
    return df


# ---------------------------------------------------------------------------
# Task 10 – Write sentiment features (SQL)
# ---------------------------------------------------------------------------

@task(name="write-sentiment-features")
def write_sentiment_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("silver_enriched", silver_df)
        con.execute("DROP TABLE IF EXISTS gold.review_sentiment_features")
        con.execute("""
            CREATE TABLE gold.review_sentiment_features AS
            WITH base AS (
                SELECT
                    _index, product_id, product_parent, label,
                    detected_language, product_category_id, dataset_split,
                    body_sentiment_label, body_sentiment_score, body_sentiment_polarity,
                    headline_sentiment_label, headline_sentiment_score, headline_sentiment_polarity,
                    sentiment_mismatch, repetition_ratio, flesch_reading_ease,
                    headline_body_jaccard, title_body_jaccard, title_body_overlap, body_bigram_diversity,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[A-Z]')), 0) * 1.0
                        / NULLIF(COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[a-zA-Z]')), 0), 0) AS uppercase_ratio,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[0-9]')), 0) * 1.0
                        / NULLIF(ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')), 0) AS digit_density,
                    GREATEST(COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[.!?]+')), 0), 1) AS sentence_count,
                    ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')) * 1.0
                        / NULLIF(GREATEST(COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[.!?]+')), 0), 1), 0) AS avg_sentence_length
                FROM silver_enriched
                WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
            ),
            train_lang_stats AS (
                SELECT detected_language,
                    AVG(flesch_reading_ease)        AS lang_mean_flesch,
                    STDDEV(flesch_reading_ease)     AS lang_std_flesch,
                    AVG(body_sentiment_polarity)    AS lang_mean_polarity,
                    STDDEV(body_sentiment_polarity) AS lang_std_polarity
                FROM base WHERE dataset_split = 'train'
                GROUP BY detected_language
            ),
            train_cat_stats AS (
                SELECT product_category_id,
                    AVG(flesch_reading_ease)    AS cat_mean_flesch,
                    STDDEV(flesch_reading_ease) AS cat_std_flesch
                FROM base WHERE dataset_split = 'train'
                GROUP BY product_category_id
            ),
            normed AS (
                SELECT
                    b.*,
                    (b.flesch_reading_ease - tl.lang_mean_flesch) / NULLIF(tl.lang_std_flesch, 0)
                        AS flesch_lang_zscore,
                    (b.flesch_reading_ease - tc.cat_mean_flesch)  / NULLIF(tc.cat_std_flesch,  0)
                        AS flesch_cat_zscore,
                    (b.body_sentiment_polarity - tl.lang_mean_polarity) / NULLIF(tl.lang_std_polarity, 0)
                        AS polarity_lang_zscore
                FROM base b
                LEFT JOIN train_lang_stats tl ON tl.detected_language   = b.detected_language
                LEFT JOIN train_cat_stats  tc ON tc.product_category_id = b.product_category_id
            )
            SELECT * FROM normed
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_sentiment_features").fetchone()[0]
    logger.info("Sentiment features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 11 – Evidence features (SQL)
# ---------------------------------------------------------------------------

@task(name="compute-evidence-features")
def compute_evidence_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("silver_with_lang", silver_df)
        con.execute("CREATE OR REPLACE TEMP TABLE silver_temp AS SELECT * FROM silver_with_lang")
        con.execute("DROP TABLE IF EXISTS gold.review_evidence_features")
        con.execute("""
            CREATE TABLE gold.review_evidence_features AS
            WITH base AS (
                SELECT
                    _index, product_id, product_parent, label, product_category_id, dataset_split, review_body,
                    GREATEST(COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[.!?]+')), 0), 1) AS sentence_count_approx,
                    ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')) AS word_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body,
                        '\\d+[\\.,]?\\d*\\s{0,2}(cm|mm|km|mg|kg|gb|mb|tb|ml|cl|dl|hz|khz|mhz|ghz|mp|fps|dpi|ppi|rpm|watt|wh|mah|lm|db|nm|ft|inch|oz|lb|yd|°c|°f|°|%|p\\b|h\\b|min\\b|sec\\b|g\\b|l\\b|m\\b|k\\b)'
                    )), 0) AS measurement_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body,
                        '[$€£₹₽]\\s?\\d+[\\.,]?\\d*|\\d+[\\.,]?\\d*\\s?[$€£₹₽]'
                    )), 0) AS price_ref_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body,
                        '\\b(best|worst|better|worse|compared to|versus|vs\\.?|unlike|superior|inferior|first|second|third|top|bottom|highest|lowest|greatest|least|most|fewer|more than|less than)\\b'
                    )), 0) AS ordinal_comparison_count
                FROM silver_temp
                WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
            ),
            densities AS (
                SELECT
                    _index, product_id, product_parent, label, product_category_id, dataset_split,
                    measurement_count, price_ref_count, ordinal_comparison_count,
                    measurement_count    * 1.0 / sentence_count_approx AS measurement_density,
                    price_ref_count      * 1.0 / sentence_count_approx AS price_reference_density,
                    ordinal_comparison_count * 1.0 / sentence_count_approx AS ordinal_comparison_density,
                    (3.0 * measurement_count + 2.0 * price_ref_count + 1.0 * ordinal_comparison_count)
                        * 1.0 / NULLIF(sentence_count_approx, 0) AS quantitative_evidence_score
                FROM base
            ),
            train_cat_ev AS (
                SELECT product_category_id,
                    AVG(quantitative_evidence_score)    AS cat_mean_ev,
                    STDDEV(quantitative_evidence_score) AS cat_std_ev
                FROM densities WHERE dataset_split = 'train'
                GROUP BY product_category_id
            ),
            normed AS (
                SELECT
                    d.*,
                    (d.quantitative_evidence_score - tc.cat_mean_ev) / NULLIF(tc.cat_std_ev, 0)
                        AS evidence_cat_zscore
                FROM densities d
                LEFT JOIN train_cat_ev tc ON tc.product_category_id = d.product_category_id
            )
            SELECT * FROM normed
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_evidence_features").fetchone()[0]
    logger.info("Evidence features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 12 – Product context (SQL)
# ---------------------------------------------------------------------------

@task(name="compute-product-context")
def compute_product_context(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("silver_data", silver_df)
        con.execute("DROP TABLE IF EXISTS gold.review_product_context")
        con.execute("""
            CREATE TABLE gold.review_product_context AS
            WITH base AS (
                SELECT
                    _index, product_id, product_parent, label,
                    vine, verified_purchase, review_date, marketplace_id, product_category_id,
                    COUNT(*) OVER (PARTITION BY product_parent)           AS product_review_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_parent ORDER BY review_date
                    )                                                     AS review_relative_rank,
                    MIN(review_date) OVER (PARTITION BY product_parent)   AS first_review_date
                FROM silver_data
            ),
            with_early AS (
                SELECT *,
                    CASE
                        WHEN review_relative_rank <= CEIL(product_review_count * 0.10)
                        THEN TRUE ELSE FALSE
                    END AS is_early_review,
                    CASE
                        WHEN review_relative_rank <= 3
                        THEN TRUE ELSE FALSE
                    END AS is_top3_review,
                    DATE_DIFF('day', first_review_date, review_date) AS days_since_first_review
                FROM base
            ),
            with_category_stats AS (
                SELECT *,
                    PERCENT_RANK() OVER (
                        PARTITION BY product_category_id ORDER BY product_review_count
                    ) AS product_popularity_pctile,
                    AVG(product_review_count) OVER (
                        PARTITION BY product_category_id
                    ) AS category_review_density,
                    COUNT(DISTINCT product_parent) OVER (
                        PARTITION BY product_category_id
                    ) AS category_product_count,
                    AVG(CASE WHEN vine = 'Y' THEN 1.0 ELSE 0.0 END) OVER (
                        PARTITION BY product_category_id
                    ) AS vine_category_rate,
                    AVG(CASE WHEN verified_purchase = 'Y' THEN 1.0 ELSE 0.0 END) OVER (
                        PARTITION BY product_category_id
                    ) AS verified_category_rate
                FROM with_early
            )
            SELECT
                _index, product_id, product_parent, label, product_category_id,
                product_review_count,
                product_popularity_pctile,
                ROUND(category_review_density, 2)                                      AS category_review_density,
                category_product_count,
                vine_category_rate,
                verified_category_rate,
                is_early_review,
                is_top3_review,
                (is_early_review AND product_popularity_pctile > 0.75)                 AS early_in_popular_product,
                (vine = 'Y' AND product_review_count < 10)                             AS vine_in_sparse_product,
                days_since_first_review * product_popularity_pctile                    AS days_since_first_x_popularity,
                (verified_purchase = 'Y' AND product_popularity_pctile > 0.75)         AS verified_in_popular_product,
                ROUND(product_popularity_pctile * product_review_count, 2)             AS popularity_weight,
                LN(1.0 + review_relative_rank)                                         AS log_review_rank
            FROM with_category_stats
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_product_context").fetchone()[0]
    logger.info("Product context: %d rows", n)


# ---------------------------------------------------------------------------
# Task 13 – Specificity features (SQL)
# ---------------------------------------------------------------------------

@task(name="compute-specificity-features")
def compute_specificity_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("silver_temp", silver_df)
        con.execute("DROP TABLE IF EXISTS gold.review_specificity_features")
        con.execute("""
            CREATE TABLE gold.review_specificity_features AS
            WITH base AS (
                SELECT
                    _index, product_id, product_parent, label, product_category_id,
                    ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' '))  AS word_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body, '(?i)\\b(I|my|me|myself|mine)\\b'
                    )), 0) AS first_person_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body,
                        '(?i)\\b(best|worst|amazing|perfect|terrible|awesome|horrible|excellent|awful|fantastic|outstanding|incredible|useless|pathetic|brilliant|superb|dreadful|magnificent|exceptional|phenomenal|extraordinary|flawless|garbage|disgusting|wonderful|breathtaking|atrocious)\\b'
                    )), 0) AS superlative_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body,
                        '\\b\\d+[.,]?\\d*\\s{0,2}(cm|mm|km|mg|kg|gb|mb|tb|ml|hz|mp|fps|\\$|€|£|%|inch|oz|lb)\\b'
                    )), 0) AS specific_detail_count,
                    COALESCE(ARRAY_LENGTH(regexp_extract_all(
                        review_body, '\\n\\s*[-*•]\\s+|\\n\\s*[0-9]+[.)]\\s+'
                    )), 0) AS list_item_count,
                    PERCENT_RANK() OVER (
                        PARTITION BY product_parent
                        ORDER BY ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' '))
                    ) AS body_word_count_pctile_in_product,
                    PERCENT_RANK() OVER (
                        PARTITION BY product_category_id
                        ORDER BY ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' '))
                    ) AS word_count_cat_pctile,
                    ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')) * 1.0
                        / NULLIF(AVG(ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')))
                                 OVER (PARTITION BY product_parent), 0)
                        AS word_count_vs_product_avg
                FROM silver_temp
                WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
            )
            SELECT
                _index, product_id, product_parent, label,
                first_person_count, superlative_count, specific_detail_count, list_item_count,
                first_person_count    * 1.0 / NULLIF(word_count, 0) AS first_person_density,
                superlative_count     * 1.0 / NULLIF(word_count, 0) AS superlative_density,
                specific_detail_count * 1.0 / NULLIF(word_count, 0) AS specific_detail_density,
                list_item_count       * 1.0 / NULLIF(word_count, 0) AS list_density,
                (2.0 * specific_detail_count + first_person_count)
                    * 1.0 / NULLIF(word_count, 0)                   AS specificity_score,
                body_word_count_pctile_in_product,
                word_count_cat_pctile,
                word_count_vs_product_avg
            FROM base
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_specificity_features").fetchone()[0]
    logger.info("Specificity features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 14 – Advanced Python features (vocab overlap, entropy, burst, TF-IDF...)
# ---------------------------------------------------------------------------

@task(name="compute-advanced-python-features", timeout_seconds=3600)
def compute_advanced_python_features(silver_df: pd.DataFrame, sentiment_pipe) -> pd.DataFrame:
    logger = get_run_logger()
    df = silver_df.copy()
    n = len(df)
    bodies = df["review_body"].fillna("").tolist()
    titles = df["product_title"].fillna("").tolist()

    # 1/8 Vocab overlap with product siblings
    logger.info("1/8 Vocabulary overlap...")
    def _tokenize_set(text):
        return set(re.findall(r"\b[a-z]{2,}\b", text.lower())) if isinstance(text, str) else set()

    token_sets  = [_tokenize_set(t) for t in bodies]
    prod_groups = df.groupby("product_parent", sort=False)["_index"].apply(list).to_dict()
    vocab_overlap = np.full(n, np.nan)
    for indices in prod_groups.values():
        if len(indices) < 2:
            continue
        for i in indices:
            my = token_sets[i]
            if not my:
                continue
            others = set().union(*(token_sets[j] for j in indices if j != i))
            vocab_overlap[i] = len(my & others) / len(my)

    # 2/8 Char n-gram entropy
    logger.info("2/8 Char n-gram entropy...")
    def _char_ngram_entropy(text, ng=3):
        if not isinstance(text, str) or len(text) < ng:
            return np.nan
        s      = text[:1000]
        ngrams = [s[i:i+ng] for i in range(len(s) - ng + 1)]
        counts = Counter(ngrams)
        total  = sum(counts.values())
        probs  = np.array([c / total for c in counts.values()])
        return float(-np.sum(probs * np.log2(probs + 1e-10)))

    char_entropy = np.array([_char_ngram_entropy(t) for t in bodies])

    # 3/8 Script homogeneity
    logger.info("3/8 Script homogeneity...")
    def _script_homogeneity(text):
        if not isinstance(text, str) or not text:
            return np.nan
        scripts = Counter()
        for ch in text[:500]:
            if unicodedata.category(ch).startswith("L"):
                name = unicodedata.name(ch, "UNKNOWN")
                scripts[name.split()[0]] += 1
        if not scripts:
            return np.nan
        dominant = scripts.most_common(1)[0][1]
        return dominant / sum(scripts.values())

    script_homo = np.array([_script_homogeneity(t) for t in bodies])

    # 4/8 Review burst (±3 days)
    logger.info("4/8 Review burst (±3 days)...")
    sf = df[["_index", "product_parent", "review_date"]].copy()
    sf["review_date_d"] = pd.to_datetime(sf["review_date"]).dt.normalize()
    burst = np.zeros(n, dtype=np.int32)
    for prod, grp in sf.groupby("product_parent", sort=False):
        if len(grp) < 2:
            continue
        dates = grp["review_date_d"].values.astype("datetime64[D]").astype(np.int64)
        idxs  = grp["_index"].values
        for i in range(len(dates)):
            burst[idxs[i]] = int(np.sum(np.abs(dates - dates[i]) <= 3)) - 1

    # 5/8 Title word coverage
    logger.info("5/8 Title word coverage...")
    def _title_coverage(title, body):
        if not isinstance(title, str) or not isinstance(body, str):
            return np.nan
        tw = set(re.findall(r"\b[a-z]{2,}\b", title.lower()))
        bw = set(re.findall(r"\b[a-z]{2,}\b", body.lower()))
        return len(tw & bw) / len(tw) if tw else np.nan

    title_cov = np.array([_title_coverage(t, b) for t, b in zip(titles, bodies)])

    # 6/8 Sentiment arc
    logger.info("6/8 Sentiment arc...")
    def _split_halves(text):
        if not isinstance(text, str) or not text.strip():
            return " ", " "
        words = text.split()
        mid   = max(1, len(words) // 2)
        return " ".join(words[:mid]), " ".join(words[mid:])

    first_halves  = [_split_halves(t)[0] for t in bodies]
    second_halves = [_split_halves(t)[1] for t in bodies]

    def _batch_polarity(texts, batch_size=64):
        out = np.zeros(len(texts))
        for i in range(0, len(texts), batch_size):
            batch = [t if t.strip() else " " for t in texts[i:i+batch_size]]
            try:
                res = sentiment_pipe(batch, truncation=True, max_length=128)
                for j, r_list in enumerate(res):
                    sc = {r["label"].lower(): r["score"] for r in r_list}
                    out[i+j] = sc.get("positive", 0.0) - sc.get("negative", 0.0)
            except Exception:
                pass
        return out

    first_pol     = _batch_polarity(first_halves)
    second_pol    = _batch_polarity(second_halves)
    sentiment_arc = second_pol - first_pol

    # 7/8 TF-IDF novelty & synthesis
    logger.info("7/8 Product TF-IDF novelty & synthesis...")
    _tfidf_vec    = TfidfVectorizer(
        max_features=15_000, min_df=2, sublinear_tf=True,
        strip_accents="unicode", token_pattern=r"(?u)\b[^\W\d_]{2,}\b",
    )
    _tfidf_matrix = _tfidf_vec.fit_transform(bodies)
    _sdf_meta = df[["_index", "product_parent", "review_date"]].copy().reset_index(drop=True)
    _sdf_meta["_sdf_pos"] = _sdf_meta.index

    tfidf_novelty   = np.full(n, np.nan)
    tfidf_synthesis = np.full(n, np.nan)

    for _, grp in _sdf_meta.groupby("product_parent", sort=False):
        grp_sorted = grp.sort_values(["review_date", "_index"])
        positions  = grp_sorted["_sdf_pos"].values
        k          = len(positions)

        if k == 1:
            tfidf_novelty[positions[0]] = 1.0
            continue

        vecs       = np.asarray(_tfidf_matrix[positions].todense())
        norms      = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms_safe = np.where(norms == 0, 1.0, norms)
        vecs_norm  = vecs / norms_safe

        row_sum = vecs_norm.sum(axis=0)
        for i in range(k):
            others_centroid = (row_sum - vecs_norm[i]) / (k - 1)
            c_norm = np.linalg.norm(others_centroid)
            sim    = float(np.dot(vecs_norm[i], others_centroid / c_norm)) if c_norm > 0 else 0.0
            tfidf_synthesis[positions[i]] = max(0.0, sim)

        tfidf_novelty[positions[0]] = 1.0
        running_sum = vecs_norm[0].copy()
        for i in range(1, k):
            prior_centroid = running_sum / i
            p_norm = np.linalg.norm(prior_centroid)
            sim    = float(np.dot(vecs_norm[i], prior_centroid / p_norm)) if p_norm > 0 else 0.0
            tfidf_novelty[positions[i]] = 1.0 - max(0.0, sim)
            running_sum += vecs_norm[i]

    # 8/8 Write back to df
    logger.info("8/8 Writing advanced features back to DataFrame...")
    df["vocab_overlap_with_product"] = vocab_overlap
    df["char_ngram_entropy"]          = char_entropy
    df["script_homogeneity"]          = script_homo
    df["reviews_in_3day_window"]      = burst
    df["title_word_coverage"]         = title_cov
    df["first_half_sentiment"]        = first_pol
    df["second_half_sentiment"]       = second_pol
    df["sentiment_arc"]               = sentiment_arc
    df["product_tfidf_novelty"]       = tfidf_novelty
    df["product_tfidf_synthesis"]     = tfidf_synthesis
    return df


# ---------------------------------------------------------------------------
# Task 15 – Write advanced features to DuckDB
# ---------------------------------------------------------------------------

@task(name="write-advanced-features")
def write_advanced_features(project_root: Path, silver_df: pd.DataFrame) -> None:
    logger = get_run_logger()
    adv_py = silver_df[[
        "_index", "product_id", "product_parent", "label",
        "char_ngram_entropy", "reviews_in_3day_window",
        "first_half_sentiment", "second_half_sentiment", "sentiment_arc",
        "product_tfidf_novelty", "product_tfidf_synthesis",
    ]].copy()
    with duckdb.connect(_db_path(project_root)) as con:
        con.register("adv_py_temp", adv_py)
        con.execute("DROP TABLE IF EXISTS gold.review_advanced_features")
        con.execute("""
            CREATE TABLE gold.review_advanced_features AS
            WITH train_lang_ttr AS (
                SELECT
                    lx.detected_language,
                    AVG(lx.type_token_ratio)    AS lang_mean_ttr,
                    STDDEV(lx.type_token_ratio) AS lang_std_ttr
                FROM gold.review_lexical_features lx
                WHERE lx.dataset_split = 'train'
                GROUP BY lx.detected_language
            )
            SELECT
                py._index, py.product_id, py.product_parent, py.label,
                py.char_ngram_entropy,
                py.reviews_in_3day_window,
                py.sentiment_arc,
                py.product_tfidf_novelty,
                py.product_tfidf_synthesis,
                (lx.type_token_ratio - tl.lang_mean_ttr) / NULLIF(tl.lang_std_ttr, 0)
                    AS ttr_lang_zscore
            FROM adv_py_temp py
            LEFT JOIN gold.review_lexical_features lx ON lx._index = py._index
            LEFT JOIN train_lang_ttr tl ON tl.detected_language = lx.detected_language
        """)
        n = con.execute("SELECT COUNT(*) FROM gold.review_advanced_features").fetchone()[0]
    logger.info("Advanced features: %d rows", n)


# ---------------------------------------------------------------------------
# Task 16 – Build final ml_features table
# ---------------------------------------------------------------------------

@task(name="build-ml-features")
def build_ml_features(project_root: Path) -> None:
    logger = get_run_logger()
    with duckdb.connect(_db_path(project_root)) as con:
        # Initial join across all feature tables
        con.execute("DROP TABLE IF EXISTS gold.ml_features")
        con.execute("""
            CREATE TABLE gold.ml_features AS
            SELECT
                t._index, t.product_id, t.product_parent, t.label, t.dataset_split,
                t.marketplace_id, t.product_category_id,
                t.vine, t.verified_purchase, t.review_date,
                lx.detected_language,
                t.review_age_days, t.review_relative_rank, t.product_review_count,
                t.review_month, t.reviews_per_day,
                lx.body_word_count, lx.headline_word_count, lx.type_token_ratio,
                lx.avg_word_length, lx.sentence_count_approx,
                lx.body_lang_zscore, lx.body_cat_zscore,
                lx.headline_body_ratio, lx.exclamation_density, lx.question_density,
                lx.paragraph_break_count, lx.has_structured_body, lx.sentences_per_paragraph,
                lx.vine_x_marketplace, lx.verified_x_category,
                em.headline_body_cosine_sim,
                sm.body_sentiment_label, sm.body_sentiment_score, sm.body_sentiment_polarity,
                sm.flesch_reading_ease, sm.avg_sentence_length,
                sm.flesch_lang_zscore, sm.polarity_lang_zscore,
                pc.product_popularity_pctile, pc.category_review_density,
                pc.vine_category_rate, pc.verified_category_rate,
                pc.is_early_review, pc.log_review_rank,
                sp.word_count_cat_pctile,
                av.ttr_lang_zscore, av.char_ngram_entropy,
                av.reviews_in_3day_window, av.sentiment_arc,
                av.product_tfidf_novelty, av.product_tfidf_synthesis
            FROM gold.review_temporal_features   t
            LEFT JOIN gold.review_lexical_features     lx ON lx._index = t._index
            LEFT JOIN gold.review_embedding_features   em ON em._index = t._index
            LEFT JOIN gold.review_sentiment_features   sm ON sm._index = t._index
            LEFT JOIN gold.review_product_context      pc ON pc._index = t._index
            LEFT JOIN gold.review_specificity_features sp ON sp._index = t._index
            LEFT JOIN gold.review_advanced_features    av ON av._index = t._index
        """)

        # Apply transformations to final column shapes
        con.execute("""
            CREATE OR REPLACE TABLE gold.ml_features AS
            SELECT
                _index, product_id, product_parent, label, dataset_split,
                marketplace_id, product_category_id,
                review_date, detected_language,
                CASE WHEN vine = 'Y' THEN 1.0 ELSE 0.0 END             AS vine_binary,
                CASE WHEN verified_purchase = 'Y' THEN 1.0 ELSE 0.0 END AS verified_binary,
                vine_x_marketplace, verified_x_category,
                LN(1 + review_age_days)                                 AS log_review_age_days,
                product_review_count,
                review_month,
                COALESCE(reviews_per_day, 0.0)                          AS reviews_per_day,
                COALESCE(log_review_rank,
                    LN(1.0 + review_relative_rank))                     AS log_review_rank,
                is_early_review,
                LN(1 + body_word_count)                                 AS log_body_word_count,
                headline_word_count,
                CASE WHEN headline_word_count > 0 THEN 1 ELSE 0 END     AS has_headline,
                type_token_ratio,
                avg_word_length,
                sentence_count_approx,
                body_lang_zscore,
                ttr_lang_zscore,
                LN(1 + body_word_count) * type_token_ratio              AS lexical_richness,
                COALESCE(paragraph_break_count, 0)                      AS paragraph_break_count,
                exclamation_density,
                question_density,
                headline_body_cosine_sim,
                body_sentiment_label,
                body_sentiment_score,
                body_sentiment_polarity,
                GREATEST(LEAST(flesch_reading_ease, 200.0), -200.0)     AS flesch_reading_ease,
                flesch_lang_zscore,
                avg_sentence_length,
                sentiment_arc,
                product_popularity_pctile,
                category_review_density,
                vine_category_rate,
                verified_category_rate,
                COALESCE(word_count_cat_pctile, 0.5)                    AS word_count_cat_pctile,
                char_ngram_entropy,
                CAST(reviews_in_3day_window AS DOUBLE)                  AS reviews_in_3day_window,
                product_tfidf_novelty,
                product_tfidf_synthesis
            FROM gold.ml_features
        """)

        rows, cols = con.execute("""
            SELECT COUNT(*), (SELECT COUNT(*) FROM pragma_table_info('gold.ml_features'))
            FROM gold.ml_features
        """).fetchone()
    logger.info("ml_features finalised: %d rows x %d columns", rows, cols)


# ---------------------------------------------------------------------------
# Task 17 – Export to parquet
# ---------------------------------------------------------------------------

@task(name="export-gold-to-parquet")
def export_gold_to_parquet(
    project_root: Path,
    silver_df: pd.DataFrame,
    body_emb: np.ndarray,
    headline_emb: np.ndarray,
    title_emb: np.ndarray,
) -> list:
    logger = get_run_logger()
    gold_dir = project_root / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()

    exports = {
        "temporal_features":    "gold.review_temporal_features",
        "lexical_features":     "gold.review_lexical_features",
        "embedding_features":   "gold.review_embedding_features",
        "embedding_pca":        "gold.review_embedding_pca",
        "sentiment_features":   "gold.review_sentiment_features",
        "evidence_features":    "gold.review_evidence_features",
        "product_context":      "gold.review_product_context",
        "specificity_features": "gold.review_specificity_features",
        "advanced_features":    "gold.review_advanced_features",
        "ml_features":          "gold.ml_features",
    }

    saved = []
    with duckdb.connect(_db_path(project_root)) as con:
        for name, table in exports.items():
            for old in gold_dir.glob(f"{name}_load_date=*.parquet"):
                old.unlink()
            out = gold_dir / f"{name}_load_date={today}.parquet"
            con.execute(f"COPY (SELECT * FROM {table}) TO '{out.as_posix()}' (FORMAT PARQUET)")
            rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info("Saved %s -> %s (%d rows)", table, out.name, rows)
            saved.append(str(out))

    # Raw embeddings (not stored in DuckDB)
    for old in gold_dir.glob("embeddings_load_date=*.parquet"):
        old.unlink()
    emb_out = gold_dir / f"embeddings_load_date={today}.parquet"
    emb_df = pd.DataFrame({
        "_index":             silver_df["_index"].values,
        "body_embedding":     list(body_emb),
        "headline_embedding": list(headline_emb),
        "title_embedding":    list(title_emb),
    })
    emb_df.to_parquet(emb_out, index=False)
    logger.info("Raw embeddings -> %s (%d rows)", emb_out.name, len(emb_df))
    saved.append(str(emb_out))
    return saved


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="gold-feature-engineering", log_prints=True)
def gold_flow(project_root: Path | None = None) -> list:
    """
    Full Gold layer pipeline.

    Parameters
    ----------
    project_root : Path, optional
        Override the project root (defaults to two levels above this file).

    Returns
    -------
    list of str
        Paths of all exported parquet files.
    """
    if project_root is None:
        project_root = _project_root()

    # --- data loading ---
    silver_df = load_silver_data(project_root)

    # --- language & lightweight text enrichment (needed by lexical + sentiment SQL) ---
    silver_df = enrich_language_text_features(silver_df)

    # --- SQL feature tables that only need the base silver data ---
    compute_temporal_features(project_root, silver_df)
    compute_lexical_features(project_root, silver_df)
    compute_evidence_features(project_root, silver_df)
    compute_product_context(project_root, silver_df)
    compute_specificity_features(project_root, silver_df)

    # --- embedding pipeline ---
    emb_model = load_embedding_model()
    silver_df, body_emb, headline_emb, title_emb = compute_embeddings(emb_model, silver_df)
    write_embedding_features(project_root, silver_df, body_emb, headline_emb, title_emb)

    # --- sentiment pipeline ---
    sent_model = load_sentiment_model()
    silver_df = compute_sentiment_scores(sent_model, silver_df)
    write_sentiment_features(project_root, silver_df)

    # --- advanced Python features (needs sentiment_pipe for arc) ---
    silver_df = compute_advanced_python_features(silver_df, sent_model)
    write_advanced_features(project_root, silver_df)

    # --- final ml_features join + transform ---
    build_ml_features(project_root)

    # --- export ---
    exported = export_gold_to_parquet(project_root, silver_df, body_emb, headline_emb, title_emb)
    return exported


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    root = _project_root()

    if dry_run:
        print(f"Project root: {root}")
        for split in ["train", "test", "validation"]:
            p = root / "data" / "silver" / split / f"cleaned_{split}.parquet"
            status = "OK" if p.exists() else "MISSING"
            print(f"  {status}  {p}")
        sys.exit(0)

    result = gold_flow()
    print("\nExported files:")
    for f in result:
        print(f" - {f}")
