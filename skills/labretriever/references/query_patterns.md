# Common Query Patterns

## Ranking TFs by target count in a condition

```sql
SELECT regulator_symbol, COUNT(*) AS n_targets
FROM harbison
WHERE condition = 'GAL' AND pvalue < 0.001
GROUP BY regulator_symbol
ORDER BY n_targets DESC
LIMIT 20
```

## Finding targets of a specific TF

```sql
SELECT target_symbol, pvalue, fold_enrichment
FROM harbison
WHERE regulator_symbol = 'GAL4' AND condition = 'GAL'
ORDER BY pvalue
```

## Listing conditions for a dataset

```sql
SELECT DISTINCT condition FROM harbison_meta ORDER BY condition
```

## Counting significant hits per condition

```sql
SELECT condition, COUNT(*) AS n_sig
FROM harbison
WHERE pvalue < 0.001
GROUP BY condition
ORDER BY n_sig DESC
```

## Cross-dataset overlap (ChIP + Calling Cards)

```sql
SELECT h.target_symbol
FROM harbison h
JOIN callingcards c ON h.target_symbol = c.target_symbol
WHERE h.regulator_symbol = 'GAL4'
  AND h.condition = 'GAL'
  AND h.pvalue < 0.001
  AND c.regulator_symbol = 'GAL4'
```

## Inspecting metadata for a subset of samples

```sql
SELECT *
FROM harbison_meta
WHERE condition = 'GAL'
LIMIT 10
```

## Checking data shape before fetching

Always run a COUNT first to avoid pulling large result sets:

```sql
SELECT COUNT(*) FROM harbison WHERE pvalue < 0.05
```

Then fetch with `return_data=True` only after confirming the size is manageable.
