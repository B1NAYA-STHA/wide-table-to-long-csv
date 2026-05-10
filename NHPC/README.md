# NHPC Census Pipeline

The pipeline performs a multi-stage transformation:

```
Raw Data (Excel/CSV) → Parsed (Long Format) → Cleaned → EAV Format → Storage (S3/Database)
```

---

## NHPC Pipeline (`pipeline.py`)

Processes census tables from Nepal's NSO data portal.

### Flow

1. **Pull Metadata**: Fetches and caches metadata for all configured packages
2. **Fetch**: Downloads the raw data file from the NSO API
3. **Detect Layout**: Automatically identifies the data structure (flat, grouped, hierarchical, or transposed)
4. **Parse**: Converts raw data into a long format DataFrame
5. **Resolve**: Cleans and normalizes the data
6. **EAV Conversion**: Transforms into Entity-Attribute-Value format with indicators
7. **Save**: Writes three CSV files (parsed, clean, eav)
8. **Push (Optional)**: Inserts EAV data into the warehouse database

### Pipeline Stages

#### 1. Metadata Pull

```python
pipeline = NSOCensusPipeline()
pipeline.pull()  # Caches metadata for all PACKAGE_IDS
```

Fetches dataset descriptions, resource metadata, and URLs for all packages defined in `constants.py`.

#### 2. Data Processing

```python
eav_df = pipeline.process(resource_id="28cc1367-d99b-4911-b43c-b4f2e1c8f5f7", push_to_db=False)
```

#### 3. Output Files

For each processed resource, the pipeline creates a folder in `./data/` with three CSVs:

| File           | Purpose                                                          |
| -------------- | ---------------------------------------------------------------- |
| `parsed.csv` | Raw data converted to long format,  intermediate representation |
| `clean.csv`  | Resolved and normalized data, cleaned dimensions and values     |
| `eav.csv`    | Entity-Attribute-Value format, structured for warehousing        |

### Usage

#### Command Line

```bash
# Fetch and cache all metadata
python run.py --pull

# Process a single resource by ID
python run.py --resource-id 28cc1367-d99b-4911-b43c-b4f2e1c8f5f7

# Process and push to database
python run.py --resource-id 28cc1367-d99b-4911-b43c-b4f2e1c8f5f7 --push
```

### Configuration

Edit `constants.py` to add or remove NSO package IDs:

```python
PACKAGE_IDS = [
    "28cc1367-d99b-4911-b43c-b4f2e1c8f5f7",  # Economically active population
    "2dfc312f-d880-4b22-b86a-2fcf62ca7857",  # Household members by relation
    # Add more package IDs here...
]
```

---

## Generic File Processor (`process_file.py`)

A flexible processor for any Excel or CSV file that follows similar data structures to NHPC tables. Supports both local files and URLs.

### Flow

```
Any File (URL or Local Path) → Auto-Detect Format → Process Each Sheet → Save & Upload
```

### Pipeline Stages

#### 1. Fetch

Downloads or loads file from URL/local path:

```python
_fetch("https://example.com/data.xlsx")  # From URL
_fetch("./data/local_table.csv")         # Local file
```

#### 2. Detect Sheet Structure

- For CSV: Single sheet (None)
- For Excel: Lists all sheet names, filters out metadata sheets

```python
sheets = _sheet_names(content)
# Returns: ["Sheet1", "Sheet2", ...] or [None] for CSV
```

#### 3. Process Each Sheet

For each valid sheet:

- Detects layout type (flat, grouped, hierarchical, transposed)
- Parses to long format
- Resolves/cleans data
- Converts to EAV format
- Validates not empty at each step
- Saves to local folder
- Uploads to S3 bucket

#### 4. Generate Reports

Logs summary: `{succeeded} succeeded, {skipped} skipped, {failed} failed`

### Output Structure

```
./data/{filename}/
├── original.{ext}          # Original file (xlsx/csv/bin)
├── {sheet_name}/
│   ├── parsed.csv         # Long format data
│   ├── clean.csv          # Cleaned data
│   └── eav.csv            # EAV format
├── {another_sheet}/
│   ├── parsed.csv
│   ├── clean.csv
│   └── eav.csv
└── ...
```

S3 equivalent: `nhpc/{sheet_slug}/`

### Usage

#### Command Line

```bash
# Process a local Excel file
python process_file.py "./data/my_census_table.xlsx"

# Process a CSV file
python process_file.py "./data/my_table.csv"

# Process from URL
python process_file.py "https://example.com/census_data.xlsx"
```

## Components

- **`fetcher/`**: NSO API client for downloading and caching metadata
- **`parsers/`**: Layout-specific data parsers (flat, grouped, hierarchical, transposed)
- **`builder/`**: Data cleaning and EAV conversion utilities
- **`pipeline.py`**: Main orchestrator for NSO packages
- **`process_file.py`**: Generic file processor for any table
