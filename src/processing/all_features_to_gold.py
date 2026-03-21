"""
Gold Layer Transformation

Builds the Gold layer features from the Silver dataset.

This script executes the final stage of the ETL pipeline:
1. Data Loading: Imports the cleaned train, test, and validation 'silver' splits and unifies them into a DuckDB view, joining them using a single global `_index`.
2. Feature Engineering Modules:
   - Temporal Features: Extracts review age, relative product review rank, density, and flags early product reviews.
   - Lexical & Language Features: Parses body/headline word count, grammatical markers (exclamations/questions), text statistics like Type-Token Ratio (TTR), HTML presence, and calculates category/language z-scores.
   - Embedding Features: Processes text through the `paraphrase-multilingual-MiniLM-L12-v2` SentenceTransformer to generate heavy text vector embeddings and vector distances (simulating context differences between products/heads/bodies).
   - Sentiment Features: Evaluates body vs. headline sentiment via a RoBERTa-based pipeline (providing explicit pos/neg split scoring and measuring "sentiment mismatch"). Assesses cognitive readability using Flesch scores.
   - Evidence & Context: Looks for quantitative claims (measurements, price references, and comparison rhetoric). Applies product context scoring (popularity percentiles).
   - Novelty Scoring: Groups reviews by product to calculate tf-idf drift and vocabulary variance per review within a product space.
3. Master Output: Combines all discrete modules into a single `gold.ml_features` wide table.
4. Export: Orchestrates a file save writing Parquet datasets by loading date out to the `data/gold` directory.
"""

import sys
import os
from pathlib import Path

def run_gold_transformation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '../../'))
    os.chdir(project_root)
    import duckdb
    import pandas as pd
    import numpy as np
    import re
    from pathlib import Path
    from datetime import date
    
    import textstat
    from langdetect import detect, LangDetectException, DetectorFactory
    from sentence_transformers import SentenceTransformer
    from transformers import pipeline
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    DetectorFactory.seed = 0
    con = duckdb.connect('ProjectData.duckdb')
    con.execute('CREATE SCHEMA IF NOT EXISTS gold')
    
    # Load all three silver splits and concat into one DataFrame.
    # test/validation have no 'label' column — it is added as None for consistency.
    silver_parts = []
    for split in ['train', 'test', 'validation']:
        p = Path(f'data/silver/{split}/cleaned_{split}.parquet')
        if not p.exists():
            raise FileNotFoundError(f'Silver split not found: {p}. Run clean_transform_to_silver.ipynb first.')
        df = pd.read_parquet(p)
        if 'label' not in df.columns:
            df['label'] = None
        silver_parts.append(df)
    
    silver_df = pd.concat(silver_parts, ignore_index=True)
    
    # Assign a globally unique _index so all gold tables can join on it.
    silver_df['_index'] = range(len(silver_df))
    
    print(f'Rows: {len(silver_df):,}  |  Splits: {silver_df["dataset_split"].value_counts().to_dict()}')
    print(f'Columns: {list(silver_df.columns)}')
    
    # Register as a DuckDB view for SQL-based feature cells.
    con.register('silver_df_view', silver_df)
    con.execute('CREATE OR REPLACE VIEW silver_data AS SELECT * FROM silver_df_view')
    con.execute('DROP TABLE IF EXISTS gold.review_temporal_features')
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
    
    con.execute("""
        SELECT
            COUNT(*)                                           AS total_rows,
            SUM(CASE WHEN is_early_review THEN 1 END)         AS early_reviews,
            ROUND(AVG(review_age_days), 1)                    AS avg_age_days,
            ROUND(AVG(days_since_first_review), 1)            AS avg_days_since_first,
            MAX(product_review_count)                         AS max_reviews_per_product,
            ROUND(AVG(reviews_per_day), 3)                    AS avg_reviews_per_day
        FROM gold.review_temporal_features
    """).df()
    def detect_language(text):
        if text is None or str(text).strip() == '' or len(str(text).strip()) < 10:
            return 'unknown'
        try:
            return detect(str(text))
        except LangDetectException:
            return 'unknown'
    
    def type_token_ratio(text):
        if text is None or str(text).strip() == '':
            return None
        tokens = str(text).lower().split()
        return len(set(tokens)) / len(tokens) if tokens else None
    
    silver_df['detected_language'] = silver_df['review_body'].apply(detect_language)
    print('\nLanguage distribution (top 10):')
    print(silver_df['detected_language'].value_counts().head(10))
    
    silver_df['type_token_ratio'] = silver_df['review_body'].apply(type_token_ratio)
    con.register('silver_with_lang', silver_df)
    
    con.execute('DROP TABLE IF EXISTS gold.review_lexical_features')
    con.execute("""
        CREATE TABLE gold.review_lexical_features AS
        WITH text_stats AS (
            SELECT
                _index, product_id, product_parent, product_title,
                vine, verified_purchase, review_headline, review_body,
                review_date, marketplace_id, product_category_id,
                label, record_origin, detected_language, type_token_ratio,
    
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
                COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '&[a-zA-Z]+;|&#[0-9]+')), 0) * 1.0
                    / NULLIF(ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')), 0)
                                                                         AS html_entity_density,
                COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '<br\s*/?>')), 0)
                                                                         AS paragraph_break_count,
                CASE
                    WHEN COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '<br\s*/?>')), 0) >= 2
                    THEN TRUE ELSE FALSE
                END                                                      AS has_structured_body
            FROM silver_with_lang
            WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
        ),
        lang_norms AS (
            SELECT
                *,
                (body_word_count - AVG(body_word_count) OVER (PARTITION BY detected_language))
                    / NULLIF(STDDEV(body_word_count) OVER (PARTITION BY detected_language), 0)
                    AS body_lang_zscore,
                (body_word_count - AVG(body_word_count) OVER (PARTITION BY product_category_id))
                    / NULLIF(STDDEV(body_word_count) OVER (PARTITION BY product_category_id), 0)
                    AS body_cat_zscore,
                headline_word_count * 1.0
                    / NULLIF(body_word_count, 0)                         AS headline_body_ratio,
                exclamation_count * 1.0
                    / NULLIF(sentence_count_approx, 0)                   AS exclamation_density,
                question_count * 1.0
                    / NULLIF(sentence_count_approx, 0)                   AS question_density
            FROM text_stats
        )
        SELECT * FROM lang_norms
    """)
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(body_word_count), 1)    AS avg_words,
            ROUND(AVG(type_token_ratio), 3)   AS avg_ttr,
            ROUND(AVG(body_lang_zscore), 3)   AS avg_lang_zscore,
            ROUND(AVG(exclamation_density), 4) AS avg_excl_density
        FROM gold.review_lexical_features
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
    model = SentenceTransformer(MODEL_NAME)
    print(f'Embedding dim:  {model.get_sentence_embedding_dimension()}')
    print(f'Max seq length: {model.max_seq_length} tokens')
    def encode_field(model, series, batch_size=64, field_name=''):
        texts = series.fillna('').str.strip()
        valid_mask = (texts != '').values
        inputs = texts.where(texts != '', other=' ').tolist()
        print(f'Encoding {field_name} ({valid_mask.sum():,} / {len(texts):,} non-empty) ...')
        embeddings = model.encode(inputs, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=False)
        return embeddings.astype(np.float32), valid_mask
    
    def cosine_sim_rowwise(a, b, mask_a, mask_b):
        a_norm = a / np.clip(np.linalg.norm(a, axis=1, keepdims=True), 1e-8, None)
        b_norm = b / np.clip(np.linalg.norm(b, axis=1, keepdims=True), 1e-8, None)
        sim = np.einsum('ij,ij->i', a_norm, b_norm).astype(float)
        sim[~(mask_a & mask_b)] = np.nan
        return sim
    
    body_emb,     body_valid     = encode_field(model, silver_df['review_body'],     field_name='review_body')
    headline_emb, headline_valid = encode_field(model, silver_df['review_headline'], field_name='review_headline')
    title_emb,    title_valid    = encode_field(model, silver_df['product_title'],   field_name='product_title')
    
    silver_df['headline_body_cosine_sim'] = cosine_sim_rowwise(headline_emb, body_emb, headline_valid, body_valid)
    silver_df['title_body_cosine_sim']    = cosine_sim_rowwise(title_emb, body_emb, title_valid, body_valid)
    body_norms = np.linalg.norm(body_emb, axis=1).astype(float)
    body_norms[~body_valid] = np.nan
    silver_df['body_embedding_norm'] = body_norms
    
    print('\nSimilarity feature stats:')
    silver_df[['headline_body_cosine_sim', 'title_body_cosine_sim', 'body_embedding_norm']].describe().round(3)
    emb_features_df = silver_df[['_index', 'product_id', 'label',
                                   'headline_body_cosine_sim', 'title_body_cosine_sim',
                                   'body_embedding_norm']].copy()
    
    con.register('embedding_features_temp', emb_features_df)
    con.execute('DROP TABLE IF EXISTS gold.review_embedding_features')
    con.execute('CREATE TABLE gold.review_embedding_features AS SELECT * FROM embedding_features_temp')
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(headline_body_cosine_sim), 4) AS avg_headline_body_sim,
            ROUND(AVG(title_body_cosine_sim),    4) AS avg_title_body_sim,
            ROUND(AVG(body_embedding_norm),      3) AS avg_body_norm
        FROM gold.review_embedding_features
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    SENTIMENT_MODEL = 'cardiffnlp/twitter-xlm-roberta-base-sentiment'
    sentiment_pipe = pipeline(
        'text-classification',
        model=SENTIMENT_MODEL,
        top_k=None,
        truncation=True,
        max_length=256,
        device=-1,
    )
    def score_sentiment(texts, batch_size=32):
        clean = [str(t).strip() if t and str(t).strip() else ' ' for t in texts]
        valid = [t != ' ' for t in clean]
        results = sentiment_pipe(clean, batch_size=batch_size)
        rows = []
        for res, is_valid in zip(results, valid):
            if not is_valid:
                rows.append({'label': None, 'score': None, 'polarity': None})
                continue
            scores = {r['label'].lower(): r['score'] for r in res}
            pos = scores.get('positive', 0.0)
            neg = scores.get('negative', 0.0)
            top = max(res, key=lambda x: x['score'])
            rows.append({'label': top['label'], 'score': top['score'], 'polarity': float(pos - neg)})
        return pd.DataFrame(rows)
    
    body_sent = score_sentiment(silver_df['review_body'].tolist())
    silver_df['body_sentiment_label']    = body_sent['label'].values
    silver_df['body_sentiment_score']    = body_sent['score'].values
    silver_df['body_sentiment_polarity'] = body_sent['polarity'].values
    
    
    headline_sent = score_sentiment(silver_df['review_headline'].tolist())
    silver_df['headline_sentiment_label']    = headline_sent['label'].values
    silver_df['headline_sentiment_score']    = headline_sent['score'].values
    silver_df['headline_sentiment_polarity'] = headline_sent['polarity'].values
    
    silver_df['sentiment_mismatch'] = np.where(
        silver_df['body_sentiment_polarity'].isna() | silver_df['headline_sentiment_polarity'].isna(),
        np.nan,
        np.abs(silver_df['body_sentiment_polarity'] - silver_df['headline_sentiment_polarity'])
    )
    print('Body sentiment distribution:')
    print(silver_df['body_sentiment_label'].value_counts())
    print('\nPolarity stats (should be non-zero now):')
    print(silver_df['body_sentiment_polarity'].describe().round(3))
    TEXTSTAT_LANG_MAP = {'en': 'en', 'de': 'de', 'fr': 'fr', 'es': 'es', 'it': 'it', 'nl': 'nl', 'ru': 'ru'}
    
    def repetition_ratio(text):
        if not text or not str(text).strip():
            return None
        sentences = re.split(r'[.!?]+', str(text))
        sentences = [s.strip().lower() for s in sentences if s.strip()]
        if len(sentences) <= 1:
            return 0.0
        return (len(sentences) - len(set(sentences))) / len(sentences)
    
    def flesch_ease(row):
        lang = TEXTSTAT_LANG_MAP.get(row.get('detected_language', ''), 'en')
        text = row.get('review_body', '') or ''
        if not text.strip():
            return None
        textstat.set_lang(lang)
        try:
            return float(textstat.flesch_reading_ease(text))
        except Exception:
            return None
    
    def jaccard_sim(text_a, text_b):
        if not text_a or not text_b:
            return None
        a, b = set(str(text_a).lower().split()), set(str(text_b).lower().split())
        union = len(a | b)
        return len(a & b) / union if union > 0 else 0.0
    
    def overlap_ratio(text_source, text_target):
        if not text_source or not text_target:
            return None
        src, tgt = set(str(text_source).lower().split()), set(str(text_target).lower().split())
        return len(src & tgt) / len(src) if src else None
    
    def bigram_diversity(text):
        if not text or not str(text).strip():
            return None
        tokens = str(text).lower().split()
        if len(tokens) < 2:
            return None
        bigrams = list(zip(tokens, tokens[1:]))
        return len(set(bigrams)) / len(bigrams)
    
    silver_df['repetition_ratio']      = silver_df['review_body'].apply(repetition_ratio)
    silver_df['flesch_reading_ease']   = silver_df[['review_body', 'detected_language']].apply(flesch_ease, axis=1)
    silver_df['headline_body_jaccard'] = silver_df.apply(lambda r: jaccard_sim(r['review_headline'], r['review_body']), axis=1)
    silver_df['title_body_jaccard']    = silver_df.apply(lambda r: jaccard_sim(r['product_title'], r['review_body']), axis=1)
    silver_df['title_body_overlap']    = silver_df.apply(lambda r: overlap_ratio(r['product_title'], r['review_body']), axis=1)
    silver_df['body_bigram_diversity'] = silver_df['review_body'].apply(bigram_diversity)
    
    con.register('silver_enriched', silver_df)
    
    con.execute('DROP TABLE IF EXISTS gold.review_sentiment_features')
    con.execute("""
        CREATE TABLE gold.review_sentiment_features AS
        WITH base AS (
            SELECT
                _index, product_id, product_parent, label,
                detected_language, product_category_id,
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
        normed AS (
            SELECT *,
                (flesch_reading_ease - AVG(flesch_reading_ease) OVER (PARTITION BY detected_language))
                    / NULLIF(STDDEV(flesch_reading_ease) OVER (PARTITION BY detected_language), 0)
                    AS flesch_lang_zscore,
                (flesch_reading_ease - AVG(flesch_reading_ease) OVER (PARTITION BY product_category_id))
                    / NULLIF(STDDEV(flesch_reading_ease) OVER (PARTITION BY product_category_id), 0)
                    AS flesch_cat_zscore,
                (body_sentiment_polarity - AVG(body_sentiment_polarity) OVER (PARTITION BY detected_language))
                    / NULLIF(STDDEV(body_sentiment_polarity) OVER (PARTITION BY detected_language), 0)
                    AS polarity_lang_zscore
            FROM base
        )
        SELECT * FROM normed
    """)
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(body_sentiment_polarity), 3)  AS avg_polarity,
            ROUND(AVG(sentiment_mismatch), 3)       AS avg_mismatch,
            ROUND(AVG(flesch_reading_ease), 2)      AS avg_flesch,
            ROUND(AVG(repetition_ratio), 4)         AS avg_repetition,
            ROUND(AVG(body_bigram_diversity), 3)    AS avg_bigram_diversity
        FROM gold.review_sentiment_features
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    con.execute("CREATE OR REPLACE TEMP TABLE silver_temp AS SELECT * FROM silver_with_lang")
    con.execute('DROP TABLE IF EXISTS gold.review_evidence_features')
    con.execute("""
        CREATE TABLE gold.review_evidence_features AS
        WITH base AS (
            SELECT
                _index, product_id, product_parent, label, product_category_id, review_body,
                GREATEST(COALESCE(ARRAY_LENGTH(regexp_extract_all(review_body, '[.!?]+')), 0), 1) AS sentence_count_approx,
                ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')) AS word_count,
                COALESCE(ARRAY_LENGTH(regexp_extract_all(
                    review_body,
                    '\d+[\.,]?\d*\s{0,2}(cm|mm|km|mg|kg|gb|mb|tb|ml|cl|dl|hz|khz|mhz|ghz|mp|fps|dpi|ppi|rpm|watt|wh|mah|lm|db|nm|ft|inch|oz|lb|yd|°c|°f|°|%|p\b|h\b|min\b|sec\b|g\b|l\b|m\b|k\b)'
                )), 0) AS measurement_count,
                COALESCE(ARRAY_LENGTH(regexp_extract_all(
                    review_body,
                    '[$€£¥]\s?\d+[\.,]?\d*|\d+[\.,]?\d*\s?[$€£¥]'
                )), 0) AS price_ref_count,
                COALESCE(ARRAY_LENGTH(regexp_extract_all(
                    review_body,
                    '\b(best|worst|better|worse|compared to|versus|vs\.?|unlike|superior|inferior|first|second|third|top|bottom|highest|lowest|greatest|least|most|fewer|more than|less than)\b'
                )), 0) AS ordinal_comparison_count
            FROM silver_temp
            WHERE review_body IS NOT NULL AND TRIM(review_body) != ''
        ),
        densities AS (
            SELECT
                _index, product_id, product_parent, label, product_category_id,
                measurement_count, price_ref_count, ordinal_comparison_count,
                measurement_count    * 1.0 / sentence_count_approx AS measurement_density,
                price_ref_count      * 1.0 / sentence_count_approx AS price_reference_density,
                ordinal_comparison_count * 1.0 / sentence_count_approx AS ordinal_comparison_density,
                (3.0 * measurement_count + 2.0 * price_ref_count + 1.0 * ordinal_comparison_count)
                    * 1.0 / NULLIF(sentence_count_approx, 0) AS quantitative_evidence_score
            FROM base
        ),
        normed AS (
            SELECT *,
                (quantitative_evidence_score
                    - AVG(quantitative_evidence_score) OVER (PARTITION BY product_category_id))
                    / NULLIF(STDDEV(quantitative_evidence_score) OVER (PARTITION BY product_category_id), 0)
                    AS evidence_cat_zscore
            FROM densities
        )
        SELECT * FROM normed
    """)
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(measurement_density),          4) AS avg_meas_density,
            ROUND(AVG(price_reference_density),      4) AS avg_price_density,
            ROUND(AVG(quantitative_evidence_score),  4) AS avg_evidence_score,
            ROUND(AVG(evidence_cat_zscore),          3) AS avg_evidence_zscore
        FROM gold.review_evidence_features
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    def compute_novelty_for_product(group_df):
        n = len(group_df)
        indices = group_df.index.tolist()
        texts = group_df['review_body'].fillna('').tolist()
    
        novelty = np.full(n, np.nan)
        vocab_expansion = np.full(n, np.nan)
    
        if n >= 2:
            try:
                vec = TfidfVectorizer(min_df=1, sublinear_tf=True)
                tfidf_matrix = vec.fit_transform(texts)
                # Convert to dense array to avoid numpy.matrix deprecation issues
                total = np.asarray(tfidf_matrix.sum(axis=0))  # shape (1, vocab)
                for i in range(n):
                    row_i = np.asarray(tfidf_matrix[i].todense())  # shape (1, vocab)
                    centroid = (total - row_i) / (n - 1)
                    novelty[i] = 1.0 - float(cosine_similarity(row_i, centroid)[0, 0])
            except Exception as e:
                pass
    
        seen_tokens = set()
        for i, text in enumerate(texts):
            tokens = set(str(text).lower().split())
            if i == 0:
                seen_tokens = tokens
            else:
                new_tokens = tokens - seen_tokens
                vocab_expansion[i] = len(new_tokens) / len(tokens) if tokens else np.nan
                seen_tokens |= tokens
    
        return pd.DataFrame({
            '_row_pos': indices,
            'product_tfidf_novelty': novelty,
            'product_vocab_expansion_ratio': vocab_expansion,
        })
    
    
    novelty_base = silver_df[['_index', 'product_id', 'product_parent', 'label',
                               'product_category_id', 'review_date', 'review_body']].copy()
    novelty_base = novelty_base.sort_values(['product_parent', 'review_date', '_index']).reset_index(drop=False)
    novelty_base = novelty_base.rename(columns={'index': '_row_pos'})
    
    result_parts = []
    for pid, group in novelty_base.groupby('product_parent', sort=False):
        result_parts.append(compute_novelty_for_product(group))
    
    novelty_df = pd.concat(result_parts, ignore_index=True)
    novelty_merged = novelty_base.merge(novelty_df, on='_row_pos')
    
    print(f'Novelty NaN rate: {novelty_merged["product_tfidf_novelty"].isna().mean():.2%}')
    print(f'Vocab expansion NaN rate: {novelty_merged["product_vocab_expansion_ratio"].isna().mean():.2%}')
    print(f'\nNovelty stats:')
    print(novelty_merged['product_tfidf_novelty'].describe().round(3))
    con.register('novelty_temp', novelty_merged)
    
    con.execute('DROP TABLE IF EXISTS gold.review_novelty_features')
    con.execute("""
        CREATE TABLE gold.review_novelty_features AS
        SELECT
            _index, product_id, product_parent, label,
            product_tfidf_novelty,
            product_vocab_expansion_ratio,
            PERCENT_RANK() OVER (
                PARTITION BY product_parent
                ORDER BY product_tfidf_novelty NULLS FIRST
            ) AS product_review_rank_novelty
        FROM novelty_temp
        WHERE _index IS NOT NULL
    """)
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(product_tfidf_novelty),         3) AS avg_novelty,
            ROUND(AVG(product_vocab_expansion_ratio), 3) AS avg_vocab_expansion,
            ROUND(AVG(product_review_rank_novelty),   3) AS avg_rank_novelty
        FROM gold.review_novelty_features
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    con.execute('DROP TABLE IF EXISTS gold.review_product_context')
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
                ) AS category_product_count
            FROM with_early
        )
        SELECT
            _index, product_id, product_parent, label, product_category_id,
            product_review_count,
            product_popularity_pctile,
            ROUND(category_review_density, 2)                                      AS category_review_density,
            category_product_count,
            (is_early_review AND product_popularity_pctile > 0.75)                 AS early_in_popular_product,
            (vine = 'Y' AND product_review_count < 10)                             AS vine_in_sparse_product,
            days_since_first_review * product_popularity_pctile                    AS days_since_first_x_popularity,
            (verified_purchase = 'Y' AND product_popularity_pctile > 0.75)         AS verified_in_popular_product,
            ROUND(product_popularity_pctile * product_review_count, 2)             AS popularity_weight
        FROM with_category_stats
    """)
    
    
    con.execute("""
        SELECT label, COUNT(*) AS reviews,
            ROUND(AVG(product_popularity_pctile),      3) AS avg_pop_pctile,
            ROUND(AVG(category_review_density),        2) AS avg_cat_density,
            SUM(CASE WHEN early_in_popular_product  THEN 1 ELSE 0 END) AS early_in_popular,
            SUM(CASE WHEN vine_in_sparse_product    THEN 1 ELSE 0 END) AS vine_sparse,
            ROUND(AVG(popularity_weight),              2) AS avg_pop_weight
        FROM gold.review_product_context
        WHERE label IS NOT NULL
        GROUP BY label ORDER BY label DESC
    """).df()
    con.execute('DROP TABLE IF EXISTS gold.ml_features')
    con.execute("""
        CREATE TABLE gold.ml_features AS
        SELECT
            -- Identity & metadata
            t._index,
            t.product_id,
            t.product_parent,
            t.label,
            t.dataset_split,
            t.marketplace_id,
            t.product_category_id,
            t.vine,
            t.verified_purchase,
            t.review_date,
            lx.detected_language,
    
            -- Temporal features
            t.review_age_days,
            t.review_relative_rank,
            t.product_review_count,
            t.days_since_first_review,
            t.is_early_review,
            t.reviews_same_day,
            t.review_month,
            t.review_dayofweek,
            t.reviews_per_day,
    
            -- Lexical & language features
            lx.body_word_count,
            lx.headline_word_count,
            lx.type_token_ratio,
            lx.avg_word_length,
            lx.exclamation_count,
            lx.question_count,
            lx.sentence_count_approx,
            lx.html_entity_density,
            lx.paragraph_break_count,
            lx.has_structured_body,
            lx.body_lang_zscore,
            lx.body_cat_zscore,
            lx.headline_body_ratio,
            lx.exclamation_density,
            lx.question_density,
            lx.vine_x_marketplace,
            lx.verified_x_category,
    
            -- Embedding features
            em.headline_body_cosine_sim,
            em.title_body_cosine_sim,
            em.body_embedding_norm,
    
            -- Sentiment & quality features
            sm.body_sentiment_label,
            sm.body_sentiment_score,
            sm.body_sentiment_polarity,
            sm.headline_sentiment_label,
            sm.headline_sentiment_score,
            sm.headline_sentiment_polarity,
            sm.sentiment_mismatch,
            sm.repetition_ratio,
            sm.flesch_reading_ease,
            sm.headline_body_jaccard,
            sm.title_body_jaccard,
            sm.title_body_overlap,
            sm.body_bigram_diversity,
            sm.uppercase_ratio,
            sm.digit_density,
            sm.sentence_count,
            sm.avg_sentence_length,
            sm.flesch_lang_zscore,
            sm.flesch_cat_zscore,
            sm.polarity_lang_zscore,
    
            -- Evidence features
            ev.measurement_count,
            ev.price_ref_count,
            ev.ordinal_comparison_count,
            ev.measurement_density,
            ev.price_reference_density,
            ev.ordinal_comparison_density,
            ev.quantitative_evidence_score,
            ev.evidence_cat_zscore,
    
            -- Novelty features
            nv.product_tfidf_novelty,
            nv.product_vocab_expansion_ratio,
            nv.product_review_rank_novelty,
    
            -- Product context features
            pc.product_popularity_pctile,
            pc.category_review_density,
            pc.category_product_count,
            pc.early_in_popular_product,
            pc.vine_in_sparse_product,
            pc.days_since_first_x_popularity,
            pc.verified_in_popular_product,
            pc.popularity_weight
    
        FROM gold.review_temporal_features   t
        LEFT JOIN gold.review_lexical_features   lx ON lx._index = t._index
        LEFT JOIN gold.review_embedding_features em ON em._index = t._index
        LEFT JOIN gold.review_sentiment_features sm ON sm._index = t._index
        LEFT JOIN gold.review_evidence_features  ev ON ev._index = t._index
        LEFT JOIN gold.review_novelty_features   nv ON nv._index = t._index
        LEFT JOIN gold.review_product_context    pc ON pc._index = t._index
    """)
    
    total_rows, total_cols = con.execute("""
        SELECT COUNT(*), (SELECT COUNT(*) FROM pragma_table_info('gold.ml_features'))
        FROM gold.ml_features
    """).fetchone()
    print(f"Shape: {total_rows:,} rows x {total_cols} columns")
    
    
    orphans = con.execute("""
        SELECT COUNT(*) FROM gold.ml_features WHERE _index IS NULL
    """).fetchone()[0]
    print(f"Orphan rows (NULL _index): {orphans}")
    
    
    con.execute("""
        SELECT label, COUNT(*) AS n,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM gold.ml_features
        GROUP BY label ORDER BY label DESC
    """).df()
    cols = [r[0] for r in con.execute("SELECT name FROM pragma_table_info('gold.ml_features')").fetchall()]
    null_exprs = ", ".join([f"ROUND(SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS \"{c}\"" for c in cols])
    
    null_rates = con.execute(f"SELECT {null_exprs} FROM gold.ml_features").df()
    null_rates = null_rates.T.reset_index()
    null_rates.columns = ["column_name", "null_pct"]
    null_rates = null_rates[null_rates["null_pct"] > 0].sort_values("null_pct", ascending=False)
    print(f"Columns with nulls: {len(null_rates)}")
    null_rates
    
    gold_dir = Path('data/gold')
    gold_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    
    exports = {
        'temporal_features':  'gold.review_temporal_features',
        'lexical_features':   'gold.review_lexical_features',
        'embedding_features': 'gold.review_embedding_features',
        'sentiment_features': 'gold.review_sentiment_features',
        'evidence_features':  'gold.review_evidence_features',
        'novelty_features':   'gold.review_novelty_features',
        'product_context':    'gold.review_product_context',
        'ml_features':        'gold.ml_features',
    }
    
    for name, table in exports.items():
        # Remove any older dated versions of this file before writing
        for old in gold_dir.glob(f'{name}_load_date=*.parquet'):
            old.unlink()
    
        out = gold_dir / f'{name}_load_date={today}.parquet'
        con.execute(f"COPY (SELECT * FROM {table}) TO '{out.as_posix()}' (FORMAT PARQUET)")
        rows = con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f'Saved {table} -> {out.name}  ({rows:,} rows)')
    
    # Embeddings
    for old in gold_dir.glob('embeddings_load_date=*.parquet'):
        old.unlink()
    
    emb_out = gold_dir / f'embeddings_load_date={today}.parquet'
    emb_df = pd.DataFrame({
        '_index':             silver_df['_index'].values,
        'body_embedding':     list(body_emb),
        'headline_embedding': list(headline_emb),
        'title_embedding':    list(title_emb),
    })
    emb_df.to_parquet(emb_out, index=False)
    print(f'{emb_out.name}  ({len(emb_df):,} rows x {body_emb.shape[1]} dims)')
    tables = [
        'gold.review_temporal_features',
        'gold.review_lexical_features',
        'gold.review_embedding_features',
        'gold.review_sentiment_features',
        'gold.review_evidence_features',
        'gold.review_novelty_features',
        'gold.review_product_context',
        'gold.ml_features',
    ]
    
    print(f'{"Table":<45} {"Rows":>8} {"Columns":>9}')
    print('-' * 65)
    for t in tables:
        rows = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = len(con.execute(f'SELECT * FROM {t} LIMIT 0').description)
        marker = '  ← ML-ready' if t == 'gold.ml_features' else ''
        print(f'{t:<45} {rows:>8,} {cols:>9}{marker}')
    con.close()

if __name__ == '__main__':
    run_gold_transformation()
