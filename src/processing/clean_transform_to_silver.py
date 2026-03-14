"""
Silver Layer Data Cleaning

This script builds Silver datasets from Bronze Delta splits.

Scope:
- Input: Bronze Delta tables in data/bronze/train, data/bronze/test, data/bronze/validation
- Goal: detect, document, and fix data quality issues for each split
- Corrupt-row handling: use _corrupt_record for structural repair when Bronze parsing failed
- Output: cleaned split dataframes + exported split files under data/silver/{train,test,validation}
"""

import csv
import html
import json
import os
import re
import unicodedata
from pathlib import Path
from collections import Counter

import duckdb
import pandas as pd


# Ordered list of expected fields when reconstructing a malformed raw record.
EXPECTED_FIELDS = [
    'Row_id',
    'product_id',
    'product_parent',
    'product_title',
    'vine',
    'verified_purchase',
    'review_headline',
    'review_body',
    'review_date',
    'marketplace_id',
    'product_category_id',
    'label',
]

# Only these three columns contain free-text requiring HTML cleaning and normalization.
TEXT_FIELDS = ['product_title', 'review_headline', 'review_body']

# Module-level reference data (populated during initialization)
valid_marketplace_ids = set()
valid_category_ids = set()


def setup_paths():
    """Set up all required paths."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = Path(current_dir).parent.parent.resolve()
    
    raw_dir = project_root / 'reviews (copy)'
    bronze_dir = project_root / 'data' / 'bronze'
    silver_dir = project_root / 'data' / 'silver'

    bronze_split_paths = {
        'train': bronze_dir / 'train',
        'test': bronze_dir / 'test',
        'validation': bronze_dir / 'validation',
    }

    silver_split_dirs = {
        'train': silver_dir / 'train',
        'test': silver_dir / 'test',
        'validation': silver_dir / 'validation',
    }

    return project_root, raw_dir, bronze_dir, silver_dir, bronze_split_paths, silver_split_dirs


def load_reference_data(raw_dir):
    """Load reference data for domain validation."""
    global valid_marketplace_ids, valid_category_ids

    with open(raw_dir / 'marketplace.json', 'r', encoding='utf-8') as f:
        marketplace_raw = json.load(f)

    with open(raw_dir / 'category.json', 'r', encoding='utf-8') as f:
        category_raw = json.load(f)

    if isinstance(marketplace_raw, dict):
        marketplace_ref = pd.DataFrame(marketplace_raw)
    else:
        marketplace_ref = pd.DataFrame(marketplace_raw)

    if isinstance(category_raw, dict):
        category_ref = pd.DataFrame(category_raw)
    else:
        category_ref = pd.DataFrame(category_raw)

    valid_marketplace_ids = set(pd.to_numeric(marketplace_ref['id'], errors='coerce').dropna().astype(int).tolist())
    valid_category_ids = set(pd.to_numeric(category_ref['id'], errors='coerce').dropna().astype(int).tolist())

    print('Valid marketplace IDs:', sorted(valid_marketplace_ids))
    print('Valid category IDs:   ', sorted(valid_category_ids)[:20], '...')
    print('Marketplace reference rows:', len(marketplace_ref))
    print('Category reference rows:', len(category_ref))


def clean_text(text, marketplace_id=None):
    """
    Clean a single text value through five stages:
      1. HTML entity decoding
      2. HTML tag removal
      3. Diacritic removal (only for UK; marketplace_id=1)
      4. Unicode normalization
      5. Whitespace collapse

    Returns (cleaned_value, was_changed).
    """
    if text is None:
        return None, False

    original = str(text)
    value = original.strip()
    if value == '':
        return None, original != ''

    value = html.unescape(value)
    value = re.sub(r'<br\s*/?>', ' ', value, flags=re.IGNORECASE)
    value = re.sub(r'<[^>]+>', ' ', value)

    # Remove diacritics only for UK marketplace (marketplace_id=1)
    if marketplace_id == 1:
        value = unicodedata.normalize('NFD', value)
        value = ''.join(c for c in value if unicodedata.category(c) != 'Mn')

    value = unicodedata.normalize('NFKC', value)
    value = re.sub(r'\s+', ' ', value).strip()

    if value == '':
        return None, True

    return value, value != original


def normalize_yn(value):
    """Map Y/N field variants to canonical 'Y' or 'N'."""
    if value is None:
        return None
    v = str(value).strip().upper()
    if v in {'Y', 'YES', 'TRUE', 'T', '1'}:
        return 'Y'
    if v in {'N', 'NO', 'FALSE', 'F', '0'}:
        return 'N'
    return None


def normalize_label(value):
    """Map label variants to Python bool."""
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in {'true', '1', 't', 'yes', 'y'}:
        return True
    if v in {'false', '0', 'f', 'no', 'n'}:
        return False
    return None


def apply_quality_rules(parsed, expect_label=True):
    """Apply type normalization, domain checks, and text cleaning to a parsed record."""
    record = dict(parsed)
    issues = []

    record['Row_id'] = (record.get('Row_id') or '').strip() or None
    if record['Row_id'] is None:
        issues.append('missing_row_id')

    # Parse marketplace_id early for diacritic handling
    marketplace_id_raw = record.get('marketplace_id')
    marketplace_id_numeric = pd.to_numeric(marketplace_id_raw, errors='coerce')
    marketplace_id_for_cleaning = int(marketplace_id_numeric) if not pd.isna(marketplace_id_numeric) else None

    for col in TEXT_FIELDS:
        cleaned, changed = clean_text(record.get(col), marketplace_id=marketplace_id_for_cleaning)
        record[col] = cleaned
        if changed:
            issues.append(f'text_cleaned_{col}')

    record['product_id'] = (record.get('product_id') or '').strip() or None
    record['product_parent'] = pd.to_numeric(record.get('product_parent'), errors='coerce')
    record['marketplace_id'] = marketplace_id_numeric
    record['product_category_id'] = pd.to_numeric(record.get('product_category_id'), errors='coerce')

    record['review_date'] = pd.to_datetime(record.get('review_date'), errors='coerce')
    if pd.isna(record['review_date']):
        issues.append('invalid_or_missing_review_date')
        record['review_date'] = None
    else:
        record['review_date'] = record['review_date'].date()

    record['vine'] = normalize_yn(record.get('vine'))
    record['verified_purchase'] = normalize_yn(record.get('verified_purchase'))

    if expect_label:
        record['label'] = normalize_label(record.get('label'))
    else:
        record['label'] = None

    if record['product_id'] is None:
        issues.append('missing_product_id')

    if pd.isna(record['marketplace_id']):
        issues.append('marketplace_id_not_numeric')
        record['marketplace_id'] = None
    else:
        record['marketplace_id'] = int(record['marketplace_id'])
        if record['marketplace_id'] not in valid_marketplace_ids:
            issues.append('marketplace_id_unknown')

    if pd.isna(record['product_category_id']):
        issues.append('product_category_id_not_numeric')
        record['product_category_id'] = None
    else:
        record['product_category_id'] = int(record['product_category_id'])
        if record['product_category_id'] not in valid_category_ids:
            issues.append('product_category_id_unknown')

    if pd.isna(record['product_parent']):
        issues.append('product_parent_not_numeric')
        record['product_parent'] = None
    else:
        record['product_parent'] = int(record['product_parent'])

    if record['vine'] is None:
        issues.append('vine_invalid_or_missing')
    if record['verified_purchase'] is None:
        issues.append('verified_purchase_invalid_or_missing')
    if expect_label and record['label'] is None:
        issues.append('label_invalid_or_missing')

    return record, issues


def parse_row_with_repair(raw_record):
    """Parse and repair a potentially malformed row."""
    if raw_record is None or str(raw_record).strip() == '':
        return None, ['empty_raw_row'], None

    try:
        fields = next(csv.reader([raw_record]))
    except Exception:
        return None, ['csv_parse_exception'], None

    fields = [f.strip() if f is not None else None for f in fields]
    issues = []
    strategy = 'direct'

    if len(fields) == len(EXPECTED_FIELDS):
        parsed = dict(zip(EXPECTED_FIELDS, fields))
    elif len(fields) > len(EXPECTED_FIELDS):
        # Reconstruct around stable anchors for malformed rows
        left = fields[:6]
        right = fields[-4:]
        middle = fields[6:-4]
        parsed = {
            'Row_id': left[0],
            'product_id': left[1],
            'product_parent': left[2],
            'product_title': left[3],
            'vine': left[4],
            'verified_purchase': left[5],
            'review_headline': middle[0] if len(middle) > 0 else None,
            'review_body': ','.join(middle[1:]) if len(middle) > 1 else None,
            'review_date': right[0],
            'marketplace_id': right[1],
            'product_category_id': right[2],
            'label': right[3],
        }
        strategy = 'repaired'
        issues.append('field_count_mismatch_repaired')
    else:
        return None, ['too_few_fields_unrecoverable'], None

    parsed, quality_issues = apply_quality_rules(parsed, expect_label=True)
    issues.extend(quality_issues)

    return parsed, issues, strategy


def load_bronze_split(split_name, bronze_split_paths):
    """Load a Bronze split from Delta table."""
    split_path = bronze_split_paths[split_name]
    if not split_path.exists():
        raise FileNotFoundError(f'Bronze split not found: {split_path}')

    query = f"SELECT * FROM delta_scan('{split_path.as_posix()}')"
    with duckdb.connect(database=':memory:') as con:
        return con.execute(query).df()


def process_splits(bronze_split_paths):
    """Process all Bronze splits and build clean candidate datasets."""
    split_results = {}
    issue_counter = Counter()
    total_raw_rows = 0

    for split_name in ['train', 'test', 'validation']:
        print(f"\n--- Processing {split_name} split ---")
        bronze_df = load_bronze_split(split_name, bronze_split_paths)
        accepted_rows = []
        rejected_rows = []

        for row_idx, row in bronze_df.iterrows():
            total_raw_rows += 1
            row_data = row.to_dict()
            source_file = str(row_data.get('_source_file') or '')
            line_number = row_data.get('_index')

            corrupt_record = row_data.get('_corrupt_record')
            has_corrupt_record = isinstance(corrupt_record, str) and corrupt_record.strip() != ''

            if has_corrupt_record:
                parsed, issues, strategy = parse_row_with_repair(corrupt_record)

                if parsed is None:
                    if split_name == 'train':
                        for issue in issues:
                            issue_counter[issue] += 1
                        rejected_rows.append({
                            'dataset_split': split_name,
                            '_source_file': source_file,
                            '_line_number': line_number,
                            'rejection_reasons': ';'.join(issues),
                            'raw_record': str(corrupt_record)[:1000],
                        })
                        continue

                    parsed = {
                        'Row_id': row_data.get('Row_id'),
                        'product_id': row_data.get('product_id'),
                        'product_parent': row_data.get('product_parent'),
                        'product_title': row_data.get('product_title'),
                        'vine': row_data.get('vine'),
                        'verified_purchase': row_data.get('verified_purchase'),
                        'review_headline': row_data.get('review_headline'),
                        'review_body': row_data.get('review_body'),
                        'review_date': row_data.get('review_date'),
                        'marketplace_id': row_data.get('marketplace_id'),
                        'product_category_id': row_data.get('product_category_id'),
                        'label': row_data.get('label'),
                    }
                    parsed, fallback_issues = apply_quality_rules(parsed, expect_label=False)
                    issues = list(set(issues + fallback_issues + ['corrupt_record_unrepaired_kept']))
                    strategy = 'bronze_clean_from_corrupt_record_kept'
                else:
                    issues.append('from_corrupt_record')
                    strategy = f'{strategy}_from_corrupt_record'
            else:
                parsed = {
                    'Row_id': row_data.get('Row_id'),
                    'product_id': row_data.get('product_id'),
                    'product_parent': row_data.get('product_parent'),
                    'product_title': row_data.get('product_title'),
                    'vine': row_data.get('vine'),
                    'verified_purchase': row_data.get('verified_purchase'),
                    'review_headline': row_data.get('review_headline'),
                    'review_body': row_data.get('review_body'),
                    'review_date': row_data.get('review_date'),
                    'marketplace_id': row_data.get('marketplace_id'),
                    'product_category_id': row_data.get('product_category_id'),
                    'label': row_data.get('label'),
                }
                parsed, issues = apply_quality_rules(parsed, expect_label=(split_name == 'train'))
                strategy = 'bronze_clean'

            for issue in issues:
                issue_counter[issue] += 1

            parsed['dataset_split'] = split_name
            parsed['_source_file'] = source_file
            parsed['_line_number'] = line_number if line_number is not None else (row_idx + 1)
            parsed['record_origin'] = strategy
            parsed['detected_issues'] = ';'.join(sorted(set(issues))) if issues else ''
            accepted_rows.append(parsed)

        cleaned_df_split = pd.DataFrame(accepted_rows)
        rejected_df_split = pd.DataFrame(rejected_rows)
        split_results[split_name] = {
            'bronze_rows': len(bronze_df),
            'cleaned_df': cleaned_df_split,
            'rejected_df': rejected_df_split,
        }
        print(f"  Bronze rows: {len(bronze_df)}, Accepted: {len(cleaned_df_split)}, Rejected: {len(rejected_df_split)}")

    return split_results, issue_counter, total_raw_rows


def deduplicate_and_finalize(split_results, issue_counter):
    """Deduplicate and finalize Silver-ready datasets."""
    silver_splits = {}
    duplicates_removed_by_split = {}

    for split_name in ['train', 'test', 'validation']:
        cleaned_split = split_results[split_name]['cleaned_df']
        if cleaned_split.empty:
            silver_split = cleaned_split.copy()
            duplicates_removed = 0
        elif split_name == 'train':
            before = len(cleaned_split)
            silver_split = (
                cleaned_split
                .sort_values(['_source_file', '_line_number'])
                .drop_duplicates(subset=['product_id', 'review_date', 'review_body'], keep='first')
                .reset_index(drop=True)
            )
            duplicates_removed = before - len(silver_split)
        else:
            silver_split = cleaned_split.reset_index(drop=True)
            duplicates_removed = 0

        # Submission rules for held-out splits
        if split_name in ['test', 'validation']:
            if 'label' in silver_split.columns:
                silver_split = silver_split.drop(columns=['label'])

            object_cols = silver_split.select_dtypes(include=['object', 'string']).columns
            if len(object_cols) > 0:
                silver_split.loc[:, object_cols] = silver_split.loc[:, object_cols].replace(
                    to_replace=r'(?i)^\s*(nan|none|null)\s*$',
                    value=None,
                    regex=True,
                )

        if duplicates_removed > 0:
            issue_counter['duplicate_rows_removed'] += duplicates_removed

        silver_split['_ingested_at'] = pd.Timestamp.now('UTC')
        silver_splits[split_name] = silver_split
        duplicates_removed_by_split[split_name] = duplicates_removed

    return silver_splits, duplicates_removed_by_split


def print_data_quality_summary(silver_splits, split_results, issue_counter, total_raw_rows, duplicates_removed_by_split):
    """Print data quality findings."""
    silver_df = pd.concat([silver_splits[s] for s in silver_splits], ignore_index=True)
    cleaned_df = pd.concat([split_results[s]['cleaned_df'] for s in split_results], ignore_index=True)
    rejected_df = pd.concat([split_results[s]['rejected_df'] for s in split_results], ignore_index=True)
    duplicates_removed = sum(duplicates_removed_by_split.values())

    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY")
    print("=" * 60)

    print(f"\nTotal raw rows: {total_raw_rows}")
    print(f"Accepted before dedup: {len(cleaned_df)}")
    print(f"Accepted after dedup: {len(silver_df)}")
    print(f"Rejected rows: {len(rejected_df)}")
    print(f"Duplicates removed: {duplicates_removed}")
    print(f"Coverage: {round(100.0 * len(silver_df) / max(total_raw_rows, 1), 2)}%")

    print("\nRows after split finalization:")
    for split_name in ['train', 'test', 'validation']:
        dedup_applied = 'yes' if split_name == 'train' else 'no'
        print(f"  {split_name:<10} rows={len(silver_splits[split_name]):<8} "
              f"duplicates_removed={duplicates_removed_by_split[split_name]} dedup_applied={dedup_applied}")

    print("\nTop issues detected:")
    sorted_issues = sorted(issue_counter.items(), key=lambda x: (-x[1], x[0]))
    for issue, count in sorted_issues[:15]:
        pct = round(100.0 * count / max(total_raw_rows, 1), 2)
        print(f"  {issue}: {count} ({pct}%)")


def export_to_parquet(silver_splits, silver_split_dirs, silver_dir):
    """Export cleaned outputs by split to Parquet."""
    silver_dir.mkdir(parents=True, exist_ok=True)

    print("\n--- Exporting to Parquet ---")
    for split_name in ['train', 'test', 'validation']:
        out_dir = silver_split_dirs[split_name]
        out_dir.mkdir(parents=True, exist_ok=True)

        clean_parquet = out_dir / f'cleaned_{split_name}.parquet'
        silver_splits[split_name].to_parquet(clean_parquet, index=False)

        print(f"  {split_name:<10} -> {clean_parquet}")


def run_silver_transformation():
    """Main silver transformation pipeline."""
    print("=" * 60)
    print("SILVER LAYER TRANSFORMATION")
    print("=" * 60)

    # Setup paths
    project_root, raw_dir, bronze_dir, silver_dir, bronze_split_paths, silver_split_dirs = setup_paths()

    print(f'\nProject root: {project_root}')
    print(f'Bronze dir:   {bronze_dir}')
    for split, path in bronze_split_paths.items():
        print(f'  - {split:<10} {path} (exists={path.exists()})')

    # Load reference data
    print("\n--- Loading Reference Data ---")
    load_reference_data(raw_dir)

    # Process splits
    print("\n--- Processing Bronze Splits ---")
    split_results, issue_counter, total_raw_rows = process_splits(bronze_split_paths)

    # Deduplicate and finalize
    print("\n--- Deduplicating and Finalizing ---")
    silver_splits, duplicates_removed_by_split = deduplicate_and_finalize(split_results, issue_counter)

    # Print data quality summary
    print_data_quality_summary(silver_splits, split_results, issue_counter, total_raw_rows, duplicates_removed_by_split)

    # Export to Parquet
    export_to_parquet(silver_splits, silver_split_dirs, silver_dir)

    print("\n" + "=" * 60)
    print("SILVER LAYER TRANSFORMATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_silver_transformation()
