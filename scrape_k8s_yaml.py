import os
import time
import logging
from pathlib import Path
import argparse
import re
from github import Github
from github import RateLimitExceededException, UnknownObjectException, GithubException
from tqdm import tqdm 

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OUTPUT_DIR = Path("./kubernetes_yaml_files")
DEFAULT_SEARCH_QUERY = "apiVersion kind language:YAML"
DEFAULT_MAX_FILES = 1000


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

def setup_github_client():
    try:
        g = Github(GITHUB_TOKEN)
        user = g.get_user()
        logging.info(f"Authenticated as GitHub user: {user.login}")
        rate_limit = g.get_rate_limit()
        logging.info(f"Initial rate limit: {rate_limit.core.remaining}/{rate_limit.core.limit} (Resets at {rate_limit.core.reset})")
        logging.info(f"Search rate limit: {rate_limit.search.remaining}/{rate_limit.search.limit} (Resets at {rate_limit.search.reset})")
        return g
    except GithubException as e:
        logging.error(f"Failed to authenticate or get user info: {e}")
        raise

def sanitize_filename_part(part):
    """Removes or replaces characters invalid for filenames."""
    part = part.replace("/", "_").replace("\\", "_")
    part = re.sub(r'[<>:"|?*\s]+', '_', part)
    part = part.strip('_.')
    return part


def save_file(content_file, output_base_dir):
    repo_full_name = content_file.repository.full_name
    file_path_in_repo = Path(content_file.path)
    original_filename = file_path_in_repo.name


    repo_dir_name = sanitize_filename_part(repo_full_name) 
    local_dir = output_base_dir / repo_dir_name / file_path_in_repo.parent
    local_path = local_dir / original_filename
    local_dir.mkdir(parents=True, exist_ok=True)
    logging.debug(f"Saving to: {local_path}")


    if content_file.content is None:
            return False
    content_bytes = content_file.decoded_content

    with open(local_path, "wb") as f: 
        f.write(content_bytes)
    logging.info(f"Successfully saved: {local_path}")
    return True

def search_and_download(g: Github, query: str, output_dir: Path, max_files: int): 
    """Searches GitHub for code matching the query and downloads the files."""
    logging.info(f"Starting code search with query: '{query}'")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Maximum files to download: {max_files}")

    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded_count = 0
    processed_items = 0


    results = g.search_code(query)
    total_results = results.totalCount
    effective_total = min(total_results, 1000)
    logging.info(f"Found {total_results} potential results (API usually limits iteration to first {effective_total}).")

    iteration_limit = min(effective_total, max_files)
    progress_bar = tqdm(results, total=iteration_limit, desc="Processing files", unit="file")

    for item in progress_bar:
        if downloaded_count >= max_files:
            break
        if processed_items >= iteration_limit:
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



        except RateLimitExceededException:
            rate_limit = g.get_rate_limit()
            reset_time = rate_limit.core.reset
            wait_time = max(0, (reset_time - time.time())) + 5
            progress_bar.write(f"Rate limit hit. Pausing for {wait_time:.1f}s...")
            time.sleep(wait_time)
            continue


    progress_bar.close()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-q",
        type=str,
        default=DEFAULT_SEARCH_QUERY,
    )
    parser.add_argument(
        "-o", 
        type=Path,
        default=OUTPUT_DIR,
    )
    parser.add_argument(
        "-m",
        type=int,
        default=DEFAULT_MAX_FILES,
    )

    args = parser.parse_args()


    github_client = setup_github_client()
    search_and_download(github_client, args.query, args.output_dir, args.max_files)
    logging.info("Pipeline finished.")

