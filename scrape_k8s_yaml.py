import os
import time
import base64
import logging
from pathlib import Path
import argparse
import re # Import regex for sanitizing filenames
from github import Github
from github import RateLimitExceededException, UnknownObjectException, GithubException
from tqdm import tqdm # Optional: for progress indication

# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OUTPUT_DIR = Path("./kubernetes_yaml_files")
DEFAULT_SEARCH_QUERY = "apiVersion kind language:YAML"
DEFAULT_MAX_FILES = 1000

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

# --- Functions ---

def setup_github_client():
    """Initializes and returns the PyGithub client instance."""
    if not GITHUB_TOKEN:
        logging.error("GITHUB_TOKEN environment variable not set.")
        raise ValueError("Missing GitHub Personal Access Token")
    try:
        g = Github(GITHUB_TOKEN)
        user = g.get_user()
        logging.info(f"Authenticated as GitHub user: {user.login}")
        rate_limit = g.get_rate_limit()
        logging.info(f"Initial rate limit: {rate_limit.core.remaining}/{rate_limit.core.limit} (Resets at {rate_limit.core.reset})")
        logging.info(f"Search rate limit: {rate_limit.search.remaining}/{rate_limit.search.limit} (Resets at {rate_limit.search.reset})")
        if rate_limit.core.remaining < 50 or rate_limit.search.remaining < 5:
             logging.warning("Rate limit low. Consider waiting before running.")
        return g
    except GithubException as e:
        logging.error(f"Failed to authenticate or get user info: {e}")
        raise

def sanitize_filename_part(part):
    """Removes or replaces characters invalid for filenames."""
    # Replace path separators with underscores
    part = part.replace("/", "_").replace("\\", "_")
    # Remove other potentially problematic characters (adjust regex as needed)
    part = re.sub(r'[<>:"|?*\s]+', '_', part)
    # Remove leading/trailing underscores/dots that might cause issues
    part = part.strip('_.')
    return part

def save_file(content_file, output_base_dir, use_flat_structure: bool):
    """
    Saves the content of a GitHub ContentFile object to the local filesystem.

    Args:
        content_file: The PyGithub ContentFile object.
        output_base_dir: The base directory for saving files.
        use_flat_structure: If True, save files directly in output_base_dir
                             with unique names. Otherwise, use repo/path structure.
    """
    try:
        repo_full_name = content_file.repository.full_name
        file_path_in_repo = Path(content_file.path)
        original_filename = file_path_in_repo.name

        if use_flat_structure:
            # Create a unique filename for the flat structure
            sanitized_repo = sanitize_filename_part(repo_full_name)
            # Sanitize the parent path, use 'root' if it's the repo root
            parent_path_str = str(file_path_in_repo.parent)
            sanitized_parent_path = sanitize_filename_part(parent_path_str) if parent_path_str != '.' else 'root'
            sanitized_original_filename = sanitize_filename_part(original_filename)

            # Combine parts for a unique name, ensure it doesn't start/end badly
            unique_filename = f"{sanitized_repo}_{sanitized_parent_path}_{sanitized_original_filename}".strip('_')
            # Ensure the filename ends with .yaml or .yml if the original did
            if original_filename.lower().endswith(('.yaml', '.yml')) and not unique_filename.lower().endswith(('.yaml', '.yml')):
                 unique_filename += file_path_in_repo.suffix # Add original suffix

            local_path = output_base_dir / unique_filename
            # Ensure the base output directory exists (no subdirs needed here)
            output_base_dir.mkdir(parents=True, exist_ok=True)
            logging.debug(f"Saving with flat structure to: {local_path}")
        else:
            # Original structured path logic
            repo_dir_name = sanitize_filename_part(repo_full_name) # Use sanitized name for dir
            local_dir = output_base_dir / repo_dir_name / file_path_in_repo.parent
            local_path = local_dir / original_filename # Keep original filename here
            # Create directories if they don't exist
            local_dir.mkdir(parents=True, exist_ok=True)
            logging.debug(f"Saving with structured path to: {local_path}")

        # --- Content retrieval and writing logic ---
        try:
            if content_file.content is None:
                 logging.warning(f"Content is None for {repo_full_name}/{content_file.path}. Skipping save.")
                 return False
            content_bytes = content_file.decoded_content
        except AssertionError as e:
             logging.warning(f"Could not decode content for {repo_full_name}/{content_file.path}. Skipping save. Error: {e}")
             return False
        except GithubException as e:
            logging.error(f"GitHub API error fetching content details for {repo_full_name}/{content_file.path}: {e}. Skipping save.")
            return False
        except Exception as e:
             logging.error(f"Error decoding content for {repo_full_name}/{content_file.path}: {e}. Skipping save.")
             return False

        # Write the content to the determined local file path
        try:
            with open(local_path, "wb") as f: # Write in binary mode
                f.write(content_bytes)
            # Use INFO for successful saves for better visibility
            logging.info(f"Successfully saved: {local_path}")
            return True
        except OSError as e:
            logging.error(f"Failed to write file {local_path}: {e}")
            return False
        except Exception as e: # Catch any other write errors
            logging.error(f"Unexpected error writing file {local_path}: {e}", exc_info=True)
            return False

    except RateLimitExceededException:
        logging.warning(f"Rate limit hit while processing file metadata for {content_file.repository.full_name}/{content_file.path}. Re-raising.")
        raise # Re-raise to be caught by the main loop's rate limit handling
    except UnknownObjectException:
        logging.warning(f"File object {content_file.repository.full_name}/{content_file.path} seems invalid during save setup. Skipping.")
        return False
    except Exception as e:
        # Catch errors during path manipulation or initial setup in save_file
        logging.error(f"An unexpected error occurred during setup for saving {repo_full_name}/{content_file.path}: {e}", exc_info=True)
        return False


def search_and_download(g: Github, query: str, output_dir: Path, max_files: int, flat_structure: bool): # Added flat_structure arg
    """Searches GitHub for code matching the query and downloads the files."""
    logging.info(f"Starting code search with query: '{query}'")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Maximum files to download: {max_files}")
    if flat_structure:
        logging.info("Using flat directory structure for saving files.")
    else:
        logging.info("Using repository/path structure for saving files.")

    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded_count = 0
    processed_items = 0

    try:
        results = g.search_code(query)
        total_results = results.totalCount
        effective_total = min(total_results, 1000)
        logging.info(f"Found {total_results} potential results (API usually limits iteration to first {effective_total}).")

        iteration_limit = min(effective_total, max_files)
        progress_bar = tqdm(results, total=iteration_limit, desc="Processing files", unit="file")

        for item in progress_bar:
            # Check limits before processing
            if downloaded_count >= max_files:
                logging.info(f"Reached maximum file download limit ({max_files}). Stopping iteration.")
                break
            if processed_items >= iteration_limit:
                 # This check prevents processing beyond the 1000th item even if max_files is larger
                logging.info(f"Processed {iteration_limit} items, reaching effective API or user limit. Stopping.")
                break

            processed_items += 1
            repo_full_name = item.repository.full_name
            progress_bar.set_postfix({"downloaded": downloaded_count, "repo": repo_full_name})

            try:
                repo = item.repository
                default_branch = repo.default_branch
                logging.debug(f"Attempting fetch for {repo_full_name}/{item.path} from default branch '{default_branch}'")
                content_file = repo.get_contents(item.path, ref=default_branch)

                if isinstance(content_file, list):
                     logging.warning(f"Path {repo_full_name}/{item.path} on branch '{default_branch}' is a directory, not a file. Skipping.")
                     continue

                # Pass the flat_structure flag to save_file
                if save_file(content_file, output_dir, flat_structure):
                    downloaded_count += 1

                # time.sleep(0.1) # Optional delay

            except RateLimitExceededException:
                rate_limit = g.get_rate_limit()
                reset_time = rate_limit.core.reset
                wait_time = max(0, (reset_time - time.time())) + 5
                logging.warning(f"Core rate limit exceeded during get_contents. Sleeping for {wait_time:.2f} seconds...")
                progress_bar.write(f"Rate limit hit. Pausing for {wait_time:.1f}s...")
                time.sleep(wait_time)
                logging.info("Resuming after rate limit pause.")
                continue
            except UnknownObjectException as e:
                 logging.warning(f"Content not found for {repo_full_name}/{item.path} on default branch '{default_branch}'. Skipping. Details: {e.status} - {e.data}")
            except GithubException as e:
                 logging.error(f"GitHub API error processing {repo_full_name}/{item.path}: Status={e.status}, Data={e.data}. Skipping.")
            except Exception as e:
                 logging.error(f"Unexpected error processing item {repo_full_name}/{item.path}: {e}", exc_info=True)

            # Periodic search rate limit check (less critical for get_contents, but doesn't hurt)
            if processed_items % 50 == 0:
                 try:
                     search_limit = g.get_rate_limit().search
                     if search_limit.remaining < 3:
                         wait_time = max(0, (search_limit.reset - time.time())) + 5
                         logging.warning(f"Search rate limit low ({search_limit.remaining}). Sleeping for {wait_time:.2f} seconds...")
                         progress_bar.write(f"Search rate limit low. Pausing for {wait_time:.1f}s...")
                         time.sleep(wait_time)
                 except GithubException as e:
                     logging.warning(f"Could not check search rate limit: {e}")


        progress_bar.close()
        logging.info(f"Search finished. Processed {processed_items} items from search results, successfully downloaded {downloaded_count} files.")

    except RateLimitExceededException:
        # Handle rate limit during initial search call or limit check
        rate_limit = g.get_rate_limit()
        limit_type = "search" if hasattr(rate_limit, 'search') else "core"
        reset_time = getattr(getattr(rate_limit, limit_type, rate_limit.core), 'reset', time.time() + 60)
        wait_time = max(0, (reset_time - time.time())) + 5
        logging.error(f"Initial search or rate limit check failed due to rate limiting ({limit_type}). Try again in {wait_time:.2f} seconds.")
    except GithubException as e:
        logging.error(f"An error occurred during search setup: {e.status} - {e.data}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during the search process: {e}", exc_info=True)


# --- Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Kubernetes YAML files from public GitHub repositories.")
    parser.add_argument(
        "-q", "--query",
        type=str,
        default=DEFAULT_SEARCH_QUERY,
        help=f"GitHub code search query (Default: '{DEFAULT_SEARCH_QUERY}')"
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory to save downloaded YAML files (Default: {OUTPUT_DIR})"
    )
    parser.add_argument(
        "-m", "--max-files",
        type=int,
        default=DEFAULT_MAX_FILES,
        help=f"Maximum number of files to download (Default: {DEFAULT_MAX_FILES}, capped by API limit of ~1000 results)"
    )
    parser.add_argument(
        "--flat",
        action="store_true", # Makes it a boolean flag, default is False
        help="Save all downloaded YAML files directly into the output directory using unique names, without repository/path structure."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging."
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
         logging.getLogger("urllib3").setLevel(logging.WARNING)

    try:
        github_client = setup_github_client()
        # Pass the args.flat value to the main function
        search_and_download(github_client, args.query, args.output_dir, args.max_files, args.flat)
        logging.info("Pipeline finished.")
    except ValueError as e:
         logging.error(f"Setup error: {e}")
    except Exception as e:
         logging.error(f"Unhandled exception in main execution: {e}", exc_info=True)
