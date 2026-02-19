# cBioPortal Databricks Gateway

A Go CLI tool that exports data from Databricks tables to cBioPortal-compatible file formats.

## Building

```bash
go build ./cmd/cli
```

## Usage

```bash
./cli \
  --token=<databricks_token> \
  --host=<databricks_host> \
  --port=<port> \
  --path=<http_path> \
  --catalog=<catalog> \
  --schema=<schema> \
  --directory=<output_directory>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `--token` | Databricks personal access token for authentication |
| `--host` | Databricks workspace hostname (e.g., `your-workspace.cloud.databricks.com`) |
| `--port` | Databricks SQL warehouse port (typically `443`) |
| `--path` | HTTP path from Databricks SQL warehouse connection details (e.g., `/sql/1.0/warehouses/abc123`) |
| `--catalog` | Databricks Unity Catalog name to query |
| `--schema` | Schema name within the catalog (represents a cBioPortal study) |
| `--directory` | Local directory where output files will be written |

### Example

```bash
./cli \
  --token=dapi1234567890abcdef \
  --host=myworkspace.cloud.databricks.com \
  --port=443 \
  --path=/sql/1.0/warehouses/abc123def456 \
  --catalog=cbioportal_data \
  --schema=study_brca_2024 \
  --directory=./output
```

## How It Determines What to Export

The gateway automatically discovers which tables to export by:

1. **Querying the schema** for all available tables
2. **Filtering against supported datatypes** - only tables with names matching the supported list (see below) are exported
3. **Applying format-specific processing** based on table name:
   - `data_cna`: Pivoted from three-column format (Sample, Gene, Value) to cBioPortal matrix format
   - `data_cna_transposed`: Transposed from wide format (Sample rows, Gene columns) to cBioPortal format
   - All other tables: Exported as-is with tab-delimited formatting

4. **Generating metadata files** by reading from a `metadata` table in the same schema and matching records by `data_filename`

## Supported Datatypes

The following table names are recognized and will be exported:

### Clinical Data
| Table Name | Output File |
|------------|-------------|
| `data_clinical_patient` | `data_clinical_patient.txt` |
| `data_clinical_sample` | `data_clinical_sample.txt` |
| `data_clinical` | `data_clinical.txt` |

### Genomic Data
| Table Name | Output File |
|------------|-------------|
| `data_mutations_extended` | `data_mutations_extended.txt` |
| `data_sv` | `data_sv.txt` |
| `data_gene_matrix` | `data_gene_matrix.txt` |

### Copy Number Alteration (CNA) Data
| Table Name | Output File | Notes |
|------------|-------------|-------|
| `data_cna` | `data_CNA` | Three-column format, pivoted to matrix |
| `data_cna_transposed` | `data_CNA` | Wide format, transposed to matrix |
| `msk_tempo_data_cna_hg19` | `msk_tempo_data_cna_hg19` | MSK-specific format |

See [CNA_TABLE_FORMATS.md](CNA_TABLE_FORMATS.md) for detailed CNA format documentation.

### Timeline Data
| Table Name | Output File |
|------------|-------------|
| `data_timeline` | `data_timeline.txt` |
| `data_timeline_bmi` | `data_timeline_bmi` |
| `data_timeline_ca_125_labs` | `data_timeline_ca_125_labs` |
| `data_timeline_ca_15_3_labs` | `data_timeline_ca_15_3_labs` |
| `data_timeline_ca_19_9_labs` | `data_timeline_ca_19_9_labs` |
| `data_timeline_cancer_presence` | `data_timeline_cancer_presence` |
| `data_timeline_cea_labs` | `data_timeline_cea_labs` |
| `data_timeline_diagnosis` | `data_timeline_diagnosis` |
| `data_timeline_ecog_kps` | `data_timeline_ecog_kps` |
| `data_timeline_follow_up` | `data_timeline_follow_up` |
| `data_timeline_gleason` | `data_timeline_gleason` |
| `data_timeline_mmr` | `data_timeline_mmr` |
| `data_timeline_pdl1` | `data_timeline_pdl1` |
| `data_timeline_prior_meds` | `data_timeline_prior_meds` |
| `data_timeline_progression` | `data_timeline_progression` |
| `data_timeline_psa_labs` | `data_timeline_psa_labs` |
| `data_timeline_radiation` | `data_timeline_radiation` |
| `data_timeline_specimen` | `data_timeline_specimen` |
| `data_timeline_specimen_surgery` | `data_timeline_specimen_surgery` |
| `data_timeline_surgery` | `data_timeline_surgery` |
| `data_timeline_treatment` | `data_timeline_treatment` |
| `data_timeline_tsh_labs` | `data_timeline_tsh_labs` |
| `data_timeline_tumor_sites` | `data_timeline_tumor_sites` |

## Databricks Schema Requirements

For the gateway to work correctly, your Databricks schema should contain:

1. **Data tables** with names matching the supported datatypes listed above
2. **A `metadata` table** with columns including `data_filename` to match against data tables

Tables not in the supported list will be ignored during export.

## Output

The gateway generates:
- **Data files**: Tab-delimited text files following cBioPortal naming conventions
- **Meta files**: Corresponding `meta_*` files containing cBioPortal metadata

All files are written to the specified output directory.
