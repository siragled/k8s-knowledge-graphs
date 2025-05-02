# GitHub Kubernetes YAML Scraper

This Python script searches public repositories on GitHub for Kubernetes YAML configuration files based on a specified query and downloads them to a local directory. 

## Installation

1.  **Clone or download the script.**
2.  **Install the required Python libraries:**
    ```bash
    pip install PyGithub tqdm
    ```
## Configuration

1.  **Generate a GitHub Personal Access Token (PAT):**
    *   Go to your GitHub Settings -> Developer settings -> Personal access tokens -> Tokens (classic).
    *   Generate a new token.
    *   Under **Scopes**, ensure at least the `public_repo` scope is checked.
    *   Click **Generate token** and **copy the token immediately**. You won't be able to see it again.

2.  **Set the Environment Variable:**
    The script reads the PAT from the `GITHUB_TOKEN` environment variable. You need to set this variable in your shell before running the script.

    *   **Linux/macOS:**
        ```bash
        export GITHUB_TOKEN="your_copied_github_pat"
        ```
    *   **Windows (PowerShell):**
        ```powershell
        $env:GITHUB_TOKEN="your_copied_github_pat"
        ```

## Usage

Run the script from your terminal. Make sure the `GITHUB_TOKEN` environment variable is set.

```bash
python scrape_k8s_yaml.py [OPTIONS]
```

### Command-Line Options

*   `-q QUERY`, `--query QUERY`:
    *   Specifies the GitHub code search query.
    *   **Default:** `"apiVersion kind language:YAML"` (targets common Kubernetes manifest structure in YAML files).
    *   *Example:* `-q "apiVersion skaffold"`
*   `-o OUTPUT_DIR`, `--output-dir OUTPUT_DIR`:
    *   The directory where downloaded YAML files will be saved.
    *   **Default:** `./kubernetes_yaml_files`
*   `-m MAX_FILES`, `--max-files MAX_FILES`:
    *   The maximum number of files to attempt to download.
    *   **Default:** `1000`
    *   **Note:** The GitHub Search API typically limits results to the first 1000 items, so setting this higher will not yield more files. 
    *   *Example:* `-m 500`
*   `--flat`:
    *   If specified, saves all files directly into the `--output-dir` using unique, sanitized names combining repository and path information (e.g., `owner_repo_dir_subdir_file.yaml`).
    *   If omitted (default), files are saved in a structured format: `output-dir/owner_repo/path/in/repo/file.yaml`.
*   `--debug`:
    *   Enables verbose debug logging output to the console and `scraper.log`.


### WIP

Trying to figure out the way to stop being rate limited.
