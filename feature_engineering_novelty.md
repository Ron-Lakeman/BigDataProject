# Feature Engineering: Novel Contribution and Research Direction

**Context:** Multilingual Amazon-style review dataset (Medallion Pipeline: Bronze → Silver → Gold)
**Schema:** `product_id`, `product_parent`, `product_title`, `vine`, `verified_purchase`, `review_headline`, `review_body`, `review_date`, `marketplace_id`, `product_category_id`, `label`
**Tooling:** DuckDB (Silver → Gold transforms), PySpark (distributed ingestion and Silver cleaning)

---

## 1. Background and Motivation

The task — binary classification of review helpfulness — has attracted substantial research attention. The predominant finding is that naïve bag-of-words representations are insufficient: *who* wrote a review, *when* it was written, and *in what language and marketplace context* are at least as predictive as the review text itself (Sharma, Singh & Tiwari, 2023; Nayeem & Rafiei, 2023).

Our dataset is particularly well-suited for a novel contribution because it spans multiple `marketplace_id` values with reviews in English, French, German, Hungarian, Vietnamese, Catalan, and others. Standard helpfulness prediction pipelines assume monolingual text. The PMC survey (Saumya, Roy & Singh, 2023) identifies *cross-lingual review analysis* as an open research challenge, explicitly noting that no existing work combines marketplace-level language detection with structural feature normalisation in a unified model. This is the gap our primary contribution addresses.

---

## 2. Primary Contribution — Cross-Lingual Structural Normalisation

### 2.1 The Problem

Structural and lexical text features — word count, type-token ratio, reading ease — are language-dependent. French reviews in our dataset average 87 words; German reviews average 110 words; English reviews average 76 words. These differences are cultural and linguistic, not quality-related. A model trained on raw word counts learns thresholds calibrated to the dominant language (English) and applies them incorrectly to other languages.

The bias is measurable. For reviews in the 0–30 word range — a shared, objective bin — the helpful rate is **17.1% in English** but **31.5% in German** and **35.4% in French**. A short review in German is typical-length for that language; a short review in English is unusually sparse. A model that treats them identically will systematically mislabel one.

Sharma, Singh & Tiwari (2023) identify this as a limitation of existing feature engineering: lexical features are *language-dependent and poorly transferable across marketplaces without normalisation*. Their work does not address it. The PMC survey (Saumya, Roy & Singh, 2023) lists cross-lingual feature harmonisation as an open problem. Our contribution is a concrete, implementable solution.

### 2.2 The Solution

We run language detection (`langdetect`) as a PySpark UDF at the Silver layer, adding a `detected_language` column to every review. At the Gold layer, we compute z-scores of each structural feature **within detected language group** rather than across the full dataset:

| Feature | What it normalises |
|---|---|
| `body_lang_zscore` | Word count within language |
| `ttr_lang_zscore` | Type-token ratio within language |
| `flesch_lang_zscore` | Reading ease score within language |
| `polarity_lang_zscore` | Sentiment polarity within language |
| `body_cat_zscore` | Word count within product category |

The transformation is:

```
z = (x - mean(x | language)) / std(x | language)
```

This converts "this review has 40 words" into "this review is 0.3 standard deviations below average *for a French review*" — a cross-lingually comparable signal.

### 2.3 DuckDB Implementation (Gold Layer)

```sql
CREATE OR REPLACE TABLE gold.lexical_features AS
WITH text_stats AS (
    SELECT
        *,
        ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' '))     AS body_word_count,
        ARRAY_LENGTH(STRING_SPLIT(TRIM(review_headline), ' ')) AS headline_word_count,
        (LENGTH(REPLACE(review_body, ' ', '')) * 1.0)
            / NULLIF(ARRAY_LENGTH(STRING_SPLIT(TRIM(review_body), ' ')), 0)
            AS avg_word_length
    FROM silver.cleaned_data
),
lang_norms AS (
    SELECT
        *,
        (body_word_count - AVG(body_word_count) OVER (PARTITION BY detected_language))
            / NULLIF(STDDEV(body_word_count) OVER (PARTITION BY detected_language), 0)
            AS body_lang_zscore,
        (body_word_count - AVG(body_word_count) OVER (PARTITION BY product_category_id))
            / NULLIF(STDDEV(body_word_count) OVER (PARTITION BY product_category_id), 0)
            AS body_cat_zscore
    FROM text_stats
)
SELECT * FROM lang_norms;
```

**Why DuckDB here:** the z-score computation requires `OVER (PARTITION BY detected_language)` window functions on the consolidated Gold table. DuckDB's vectorised OLAP engine handles these 2–4× faster than equivalent PySpark jobs at this scale (Cole, 2024), and the data is already a single consolidated table at Gold layer — no distributed reads needed.

### 2.4 PySpark: Language Detection at Silver Layer

Language detection requires calling a Python library row-wise, which is appropriate for a PySpark UDF at Silver:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from langdetect import detect, LangDetectException

@udf(returnType=StringType())
def detect_language(text):
    if text is None or len(text.strip()) < 10:
        return "unknown"
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

df_silver = df_silver.withColumn(
    "detected_language",
    detect_language(F.col("review_body"))
)
df_silver.cache()
```

**Why PySpark here:** language detection is a row-wise Python operation — exactly what UDFs are for. Running it at Silver, before the Gold consolidation, means it is available for all downstream features including the z-score normalisation and the language indicator boolean flags.

### 2.5 Evidence

**Ablation result:** the Language & Lexical group (which contains the z-score features) produces the largest single AUC jump in the ablation study:

| Feature group added | Features | Holdout AUC |
|---|---|---|
| Temporal only | 6 | 0.751 |
| **+ Language & Lexical** | **21** | **0.817** |
| + Sentiment | 26 | 0.824 |
| + Embedding | 31 | 0.823 |
| + Context | 38 | 0.830 |
| + Boolean | 45 | 0.829 |

The +0.066 AUC jump from adding the Language group is five times larger than any subsequent group. The Language group also carries 28.2% of total model weight in the final trained model — the highest of any group.

---

## 3. Supporting Engineering — Temporal and Context Features

These features are in the final model and contribute meaningfully, but they are not novel in the academic sense. Temporal features for helpfulness prediction are well-covered by Nayeem & Rafiei (2023) and Liu et al. (2022). Product-category target encoding is standard ML practice. They are documented here for completeness.

**Temporal stack (12.2% model importance):** six features derived from `review_date` and `product_parent` — absolute review age, ordinal rank within product stream, lifecycle position, review rate, and burst detection. `log_review_age_days` is the 4th most important individual feature in the model (importance 0.053).

**Product-category context (14.4% model importance):** seven features placing each review in its category context. `verified_cat_target_enc` — mean-target encoding of `(verified_purchase, category_id)` — is the top feature in the group (0.047). It encodes that a verified purchase has different predictive value in different categories.

---

## 4. Future Research Direction — Pioneer × Synthesis Typology

During exploratory analysis we identified a pattern that was not incorporated into the final model but warrants further investigation.

**The idea:** characterise each review's informativeness along two axes:

- **Pioneer score** (`product_tfidf_novelty`): TF-IDF cosine distance between this review and all chronologically earlier reviews of the same product. High = introduces vocabulary and topics that earlier reviewers did not use.
- **Synthesis score** (`synthesis_score`): TF-IDF cosine similarity between this review and the leave-one-out average of all other reviews of the same product. High = reflects what the overall reviewer community has said.

**What the data shows:** splitting the training set by median on both scores produces a 2×2 typology with a 38.3 percentage point spread in helpful rate:

| Type | Pioneer | Synthesis | Helpful rate |
|---|---|---|---|
| Validates | High | High | **65.8%** |
| Bold Claim | High | Low | 47.8% |
| Echo | Low | High | 38.6% |
| Outlier / Noise | Low | Low | 27.5% |

The baseline helpful rate in the training set is 44.2%. The Validates quadrant is 21 points above baseline; Outlier/Noise is 17 points below it.

**Why it was not included in the model:** the features were computed and stored separately from the main feature pipeline. A column naming inconsistency between the two parquet files (`product_tfidf_synthesis` vs `synthesis_score`) meant they were loaded into the dataframe but not wired into the training pipeline. The 0.828 AUC is achieved without them.

**Why it is still interesting:** the finding is independent of the model. It suggests that helpfulness is not purely a property of a review in isolation — it depends on what has already been said about the product. A review that both introduces new information and reflects community consensus is most useful; one that does neither is least useful. This framing — informativeness as a two-axis property relative to an existing conversation — is not present in the published literature on helpfulness prediction and is worth pursuing as a standalone research question.

---

## 5. DuckDB vs PySpark — Architectural Rationale

| Layer | Tool | Rationale |
|---|---|---|
| **Bronze** (raw ingest of 8 CSV shards) | **PySpark** | Parallel reads across partitions; schema inference at scale; PERMISSIVE mode captures malformed rows |
| **Silver** (cleaning, language detection) | **PySpark** | UDF support for Python libraries (`langdetect`); distributed string operations |
| **Gold** (window functions, z-scores, context features) | **DuckDB** | Vectorised OLAP engine; window functions on consolidated data are faster; SQL syntax is cleaner for analytical transforms |

This matches Cole (2024): DuckDB outperforms Spark 2–4× for single-node analytical transforms; Spark is superior for distributed ingest and UDF-heavy operations.

---

## References

- Cole, M. (2024). *Should You Ditch Spark for DuckDB or Polars?* milescole.dev. Retrieved 4 March 2026.

- Liu, Y. et al. (2022). *Analyzing the impact of review recency on helpfulness.* International Journal of System Assurance Engineering and Management, Springer. https://doi.org/10.1007/s13198-020-00992-x

- Nayeem, M. T., & Rafiei, D. (2023). *On the Role of Reviewer Expertise in Temporal Review Helpfulness Prediction.* arXiv:2303.00923.

- Saumya, S., Roy, P. K., & Singh, J. P. (2023). *Review helpfulness prediction.* PeerJ Computer Science. https://pmc.ncbi.nlm.nih.gov/articles/PMC11323031/

- Sharma, S. P., Singh, L., & Tiwari, R. (2023). *Design of an Efficient Integrated Feature Engineering based Deep Learning Model Using CNN for Customer's Review Helpfulness Prediction.* Wireless Personal Communications, Springer. https://doi.org/10.1007/s11277-023-10834-1

- Sipos, R. et al. (referenced in Liu et al., 2022). *Temporal Model of the Online Customer Review Helpfulness Prediction with Regression Methods.* IEEE Access.
