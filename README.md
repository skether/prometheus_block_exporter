# Prometheus Block Exporter

This tool is designed to export Prometheus TSDB (Time Series Database) blocks from a source directory to a target directory. It's useful for backing up or archiving Prometheus data.

The exporter checks for blocks that are older than a configurable minimum age and copies them to the target directory. It maintains a status file (`block.exporter.json`) in the target directory to keep track of exported blocks and ensure consistency.

## How it Works

1.  **Scans Directories:** The tool scans both the Prometheus data directory and the export directory to identify existing blocks.
2.  **Consistency Check:** It compares the blocks in the export directory with the status file to detect any inconsistencies, such as partially exported or missing blocks.
3.  **Exports Blocks:** It iterates through the Prometheus blocks and copies those that are older than the specified `MINIMUM_AGE_HOURS` and have not been previously exported.
4.  **Updates Status:** After each successful block copy, it updates the status file with the block's information.

## Configuration

The tool is configured using environment variables:

*   `PROMETHEUS_DATA_DIR`: The path to the Prometheus data directory. (Default: `/prometheus`)
*   `TARGET_DATA_DIR`: The path to the directory where the blocks will be exported. (Default: `/export`)
*   `MINIMUM_AGE_HOURS`: The minimum age of a block in hours before it can be exported. (Default: `24`)

## Usage

### Docker

A Docker image is available for running the exporter.

```bash
docker run -d \
    -v /path/to/prometheus/data:/prometheus \
    -v /path/to/export/dir:/export \
    -e PROMETHEUS_DATA_DIR=/prometheus \
    -e TARGET_DATA_DIR=/export \
    -e MINIMUM_AGE_HOURS=24 \
    ghcr.io/skether/prometheus_block_exporter:latest
```

### From Source

This project uses [Poetry](https://python-poetry.org/) to manage its dependencies. You'll need to install it first by following their [official installation instructions](https://python-poetry.org/docs/#installation).

Once you have Poetry installed, you can run the application from source:

1.  **Install Dependencies:**
    To install the application and its core dependencies, run:
    ```bash
    poetry install
    ```
2.  **Run the Exporter:**
    ```bash
    PROMETHEUS_DATA_DIR=/path/to/prometheus/data \
    TARGET_DATA_DIR=/path/to/export/dir \
    MINIMUM_AGE_HOURS=24 \
    poetry run prometheus-block-exporter
    ```

## Development

If you want to contribute to the project, you'll need to install the development dependencies as well.

```bash
poetry install --with dev
```
This will install all dependencies, including tools for linting and formatting.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
