# Feature Engineering Summary

**Purpose:** Internal reference for the team and poster preparation.
**Scope:** What features we built, what is genuinely novel, and what the data shows.

> **Note on Pioneer × Synthesis:** the 2×2 quadrant analysis is a valid standalone data insight but was not included in the trained model. It does not contribute to the reported AUC. See Section 6.

---

## 1. The Novelty Claim in One Sentence

Every published helpfulness classifier we found assumes reviews are in a single language. Ours does not — and that assumption, when broken, is measurable: **a 0–30 word review is helpful 17% of the time in English but 35% of the time in French**. The same raw feature, two very different meanings. Our primary contribution is a pipeline that detects language at inference time and normalises structural features within language groups, making them cross-lingually comparable in a single unified model.

---

## 2. Feature Buckets at a Glance

54 features across 8 groups, all confirmed in the trained XGBoost model.

| # | Group | Features | Model importance |
|---|---|---|---|
| 1 | Language & Lexical | 15 | **28.2%** ← novel contribution |
| 2 | Embedding & Semantic | 5 | 16.7% |
| 3 | Product & Category Context | 7 | 14.4% |
| 4 | Temporal | 6 | 12.2% |
| 5 | Boolean flags | 7 | 10.3% |
| 6 | Sentiment | 5 | 6.7% |
| 7 | Interaction | 2 | 5.8% |
| 8 | Quality & Structure | 7 | 5.8% |

The Language & Lexical group is the novel one. The remaining groups are sound engineering choices that add performance — they are not academically novel.

### Group descriptions

**Language & Lexical** — 15 features covering text length, vocabulary richness, sentence structure, and reading ease. The novel subset is the five z-score normalised variants (`body_lang_zscore`, `ttr_lang_zscore`, `flesch_lang_zscore`, `polarity_lang_zscore`, `body_cat_zscore`), which are computed within detected language group. This is what separates this group from standard NLP feature engineering. See Section 3.

**Embedding & Semantic** — Five features from sentence-transformer embeddings: headline–body cosine similarity, product title–body cosine similarity, lexical title overlap, body embedding L2 norm, and character n-gram entropy. `char_ngram_entropy` is the second most important individual feature in the model (0.117).

**Product & Category Context** — Seven features placing each review in its product and category context. The most important is `verified_cat_target_enc`, a mean-target encoding of `(verified_purchase × category_id)` that encodes the category-specific value of purchase verification. See Section 4.

**Temporal** — Six features derived from `review_date` and `product_parent`, treating review date as an ordinal position within the product's review history. `log_review_age_days` is the 4th most important individual feature in the model. See Section 5.

**Boolean flags** — Verified purchase, early review indicator, and one-hot language flags. 10.3% of model weight.

**Sentiment** — Sentiment strength and polarity from body and headline, plus a sentiment arc (how tone shifts through the review), all normalised within language group.

**Interaction** — Two multiplicative terms: `verified_purchase × log(word count)` and `is_early_review × log(word count)`.

**Quality & Structure** — Digit density, uppercase ratio, repetition ratio, title word coverage, superlative density, first-person pronoun density, and a composite specificity score.

---

## 3. The Novel Contribution — Cross-Lingual Normalisation

### What existing work does

The published literature on helpfulness prediction (Sharma et al., 2023; Nayeem & Rafiei, 2023; Ocampo et al.) treats word count, type-token ratio, and reading ease as universal features. All of these studies use monolingual datasets — predominantly English Amazon reviews. Saumya, Roy & Singh (2023) explicitly list *cross-lingual review analysis* as an open research challenge, noting that no existing work addresses structural feature normalisation across languages in a unified helpfulness model. This is the gap.

### Why it matters for this dataset

Our dataset is multilingual by design. Reviews span English, French, German, Hungarian, Vietnamese, Catalan, and others. The languages do not have the same writing norms:

| Language | Mean word count | Median word count |
|---|---|---|
| English | 76 | 31 |
| French | 87 | 40 |
| German | 110 | 50 |
| Hungarian | 115 | 55 |

A model trained on raw word counts learns thresholds calibrated to whichever language dominates the training set. Applied to other languages, those thresholds are wrong. The consequence is measurable: within the 0–30 word bin, the helpful rate is **17% for English reviews** and **35% for French and German reviews**. The reviews are the same length; the languages are not.

### What we did differently
We run language detection (`langdetect`) as a PySpark UDF at the Silver layer, adding `detected_language` to every review. At Gold layer, we compute z-scores of structural features **within detected language group**:


| Feature | What it normalises |
|---|---|
| `body_lang_zscore` | Word count within language | Is this review long or short for its language?    
| `ttr_lang_zscore` | Type-token ratio within language | Is this vocabulary rich or repetitive for its language? 
| `flesch_lang_zscore` | Reading ease within language |  Is this review complex for its language?  
| `polarity_lang_zscore` | Sentiment polarity within language | Is this unusually positive/negative for its language? 
| `body_cat_zscore` | Word count within product category | Is this review long or short for its product category?

The transformation is: `z = (x − mean(x | language)) / std(x | language)`

This converts "this review has 40 words" into "this review is 0.3 standard deviations below average *for a French review*" — a signal that means the same thing regardless of which language the review is in.

### What the results show

**Ablation — cumulative AUC as groups are added:**

| Feature group added | Features | Holdout AUC | Δ AUC |
|---|---|---|---|
| Temporal only | 6 | 0.751 | — |
| **+ Language & Lexical** | **21** | **0.817** | **+0.066** |
| + Sentiment | 26 | 0.824 | +0.006 |
| + Embedding | 31 | 0.823 | −0.001 |
| + Context | 38 | 0.830 | +0.006 |
| + Boolean | 45 | 0.829 | −0.001 |

The Language & Lexical group adds **+0.066 AUC** — more than all subsequent groups combined (+0.012). It also carries 28.2% of total model weight, the highest of any group. This is the empirical case for why normalising within language, rather than treating the dataset as monolingual, makes a measurable difference.

---

## 4. Supporting Engineering — Product-Category Context

Not novel, but effective. Seven features place each review in its category context rather than treating it in isolation. The key one is `verified_cat_target_enc`: mean-target encoding of `(verified_purchase, category_id)`. This encodes that a verified purchase on professional equipment carries a different signal to a verified purchase on a paperback — the model learns the category-specific weight from data rather than assuming it is constant. The Context group carries 14.4% of model weight.

---

## 5. Supporting Engineering — Temporal Position Stack

Not novel — temporal features for helpfulness prediction are well-covered by Nayeem & Rafiei (2023) and Liu et al. (2022). What we contribute here is a specific framing: we treat `review_date` as an ordinal position within the product's review history, not a calendar date. Six features: absolute age, rank within product stream, product lifecycle position, review rate, and burst detection. `log_review_age_days` ranks 4th in individual feature importance. Temporal features alone, before any text is read, produce AUC 0.751.

---

## 6. Model Performance

| Setting | AUC |
|---|---|
| XGBoost, 5-fold stratified group CV | 0.828 |
| XGBoost, 80/20 holdout | 0.834 |
| 5-seed ensemble (XGB + LGB), OOF | 0.832 |
| Leaderboard (private test set) | 0.753 |

Cross-validation is grouped by `product_parent` — reviews of the same product never span both training and validation fold. This prevents leakage from context and temporal features computed within product groups.

---

## 7. Pioneer × Synthesis — Data Insight, Not a Model Contribution

During analysis we identified a pattern worth reporting separately. Each review was characterised on two axes: how much new vocabulary it introduces relative to earlier reviews of the same product (Pioneer), and how closely it reflects the collective reviewer consensus (Synthesis). Splitting on both scores produces a 2×2 typology:

| Type | Helpful rate |
|---|---|
| Validates (high Pioneer + high Synthesis) | 65.8% |
| Bold Claim (high Pioneer only) | 47.8% |
| Echo (high Synthesis only) | 38.6% |
| Outlier / Noise (neither) | 27.5% |

The 38.3 percentage point spread is substantial. The insight — that helpfulness depends not only on content quality but on a review's relationship to the existing product conversation — is not present in the published literature.

These features were not wired into the trained model due to a column naming inconsistency between two parquet files. The 0.828 AUC is achieved without them. On the poster, this belongs in a *future work* section: the pattern is identified, the features exist, and incorporating them into the model is the natural next step.
