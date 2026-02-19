# CNA Table Formats for Databricks

This document outlines the supported source table formats for Copy Number Alteration (CNA) data in Databricks that can be converted to cBioPortal CNA format.

## Output Format (cBioPortal Standard)

All CNA tables are transformed into the standard cBioPortal CNA format:
- **First column**: `Hugo_Symbol` (gene names)
- **Subsequent columns**: Sample IDs
- **Values**: CNA values for each gene-sample combination

Example output (`data_CNA`):
```
Hugo_Symbol    SAMPLE_1    SAMPLE_2    SAMPLE_3
TP53          -2          0           1
EGFR          2           2           0
KRAS          0           -1          0
```

## Supported Source Table Formats

### Format 1: Three-Column Format (`data_cna`)

**Table Name**: `data_cna`

**Structure**: Long format with three columns
1. Sample ID (e.g., `sample_id`, `SAMPLE_ID`)
2. Gene (e.g., `gene`, `hugo_symbol`)
3. CNA Value (e.g., `cna_value`, `cna`)

**Example Source Table**:
```
sample_id    gene      cna_value
SAMPLE_1     TP53      -2
SAMPLE_1     EGFR      2
SAMPLE_1     KRAS      0
SAMPLE_2     TP53      0
SAMPLE_2     EGFR      2
SAMPLE_2     KRAS      -1
SAMPLE_3     TP53      1
SAMPLE_3     EGFR      0
SAMPLE_3     KRAS      0
```

**Processing**:
- The `WritePivotedCNAData` function reads the table
- Validates that exactly 3 columns exist
- Pivots the data so genes become rows and samples become columns
- Missing gene-sample combinations are filled with empty strings

**Use Case**: Best for sparse data or when CNA data is stored in a normalized database format.

---

### Format 2: Transposed Format (`data_cna_transposed`)

**Table Name**: `data_cna_transposed`

**Structure**: Wide format where samples are rows and genes are columns
- **First column**: Sample IDs
- **Subsequent columns**: Gene names
- **Values**: CNA values

**Example Source Table**:
```
sample_id    TP53    EGFR    KRAS
SAMPLE_1     -2      2       0
SAMPLE_2     0       2       -1
SAMPLE_3     1       0       0
```

**Processing**:
- The `WriteTransposedTableData` function reads the table
- Transposes rows and columns (samples become columns, genes become rows)
- The first column header is replaced with `Hugo_Symbol` in the output

**Use Case**: When CNA data is already in wide format but needs to be transposed for cBioPortal.

---

## CNA Value Conventions

CNA values typically follow these conventions:
- `-2`: Homozygous deletion
- `-1`: Heterozygous deletion
- `0`: Diploid / No change
- `1`: Low-level gain
- `2`: High-level amplification

---

## Table Detection and Processing

The tool automatically detects which format to use based on the table name:

1. If table name is `data_cna_transposed`:
   - Uses `WriteTransposedTableData`
   - Transposes the table

2. If table name is `data_cna`:
   - Uses `WritePivotedCNAData`
   - Pivots from three-column format

3. Both produce the same output file: `data_CNA`

---

## Metadata Requirements

Both formats require corresponding metadata in the `metadata` table with:
- `data_filename` matching the pattern: `data_cna.%` or `data_cna_transposed.%`
- Standard cBioPortal metadata fields (e.g., `genetic_alteration_type`, `datatype`, etc.)

---

## Best Practices

### Choose Three-Column Format When:
- CNA data is stored in a normalized/long format
- Data is sparse (not all genes have values for all samples)
- Data comes from a relational database with proper normalization

### Choose Transposed Format When:
- CNA data is already in wide format (e.g., from a matrix file)
- Data is dense (most genes have values for most samples)
- You're migrating from existing matrix-based CNA files

---

## Error Handling

### Three-Column Format
- **Error**: Table does not have exactly 3 columns
- **Solution**: Verify your source table has sample_id, gene, and cna_value columns

### Transposed Format
- No specific column count requirement
- First column is treated as the identifier
- All other columns are treated as data columns

---

## Examples

### Creating a Three-Column CNA Table in Databricks

```sql
CREATE TABLE my_schema.data_cna (
    sample_id STRING,
    gene STRING,
    cna_value INT
);

INSERT INTO my_schema.data_cna VALUES
    ('SAMPLE_1', 'TP53', -2),
    ('SAMPLE_1', 'EGFR', 2),
    ('SAMPLE_2', 'TP53', 0),
    ('SAMPLE_2', 'EGFR', 2);
```

### Creating a Transposed CNA Table in Databricks

```sql
CREATE TABLE my_schema.data_cna_transposed (
    sample_id STRING,
    TP53 INT,
    EGFR INT,
    KRAS INT
);

INSERT INTO my_schema.data_cna_transposed VALUES
    ('SAMPLE_1', -2, 2, 0),
    ('SAMPLE_2', 0, 2, -1);
```
