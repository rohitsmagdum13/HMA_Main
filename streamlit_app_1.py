# streamlit_app.py — fixed AWS profile handling + duplicate checks
# - Uses your existing hma_main modules
# - Always saves a local copy and uploads to S3 (unless dry-run is enabled)
# - Duplicate checks: S3-key existence (HEAD) before upload; optional local name conflict handling
# - FIX: No more "ProfileNotFound: The config profile () could not be found" — we sanitize empty profile

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

from hma_main.core.settings import settings
from hma_main.services.file_utils import discover_files, parse_extensions, build_s3_key
from hma_main.services.s3_client import build_session, upload_file

import boto3
from botocore.exceptions import ClientError

SCOPES = ("mba", "policy")
DEFAULT_INPUT = "./data"
DEFAULT_INCLUDE = ["pdf", "csv"]


# -----------------------------
# Helpers
# -----------------------------

def sanitize_profile(value: Optional[str]) -> Optional[str]:
    """Return a clean profile string or None if empty/whitespace."""
    if value is None:
        return None
    val = value.strip()
    return val if val else None


def make_session(aws_profile_input: Optional[str], region_input: Optional[str]):
    """Create a boto3 Session via our project's build_session, ignoring empty profiles.
    This prevents botocore.exceptions.ProfileNotFound when AWS_PROFILE==''.
    """
    prof = sanitize_profile(aws_profile_input) or sanitize_profile(settings.aws_profile)
    region = region_input or settings.aws_default_region
    return build_session(
        profile=prof,
        access_key=settings.aws_access_key_id,
        secret_key=settings.aws_secret_access_key,
        region=region,
    )


def s3_client(session) -> boto3.client:
    return session.client("s3")


def s3_key_exists(session, bucket: str, key: str) -> bool:
    client = s3_client(session)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        # Other errors should surface
        raise


def infer_scope_from_path(file_path: Path) -> Optional[str]:
    parts = {p.lower() for p in file_path.parts}
    if "mba" in parts:
        return "mba"
    if "policy" in parts:
        return "policy"
    return None


def infer_bucket_and_key(scope: str, p: Path) -> Tuple[str, str]:
    bucket = settings.get_bucket(scope)
    prefix = settings.get_prefix(scope)
    key = build_s3_key(scope, p, prefix)
    return bucket, key


def ensure_unique_path(dst: Path) -> Path:
    if not dst.exists():
        return dst
    stem, suffix = dst.stem, dst.suffix
    i = 1
    while True:
        candidate = dst.with_name(f"{stem}({i}){suffix}")
        if not candidate.exists():
            return candidate
        i += 1


@dataclass
class FileRow:
    path: Path
    scope: Optional[str]
    s3_bucket: Optional[str]
    s3_key: Optional[str]
    size_bytes: int


def sizeof(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return 0


def build_rows(files: Iterable[Path], scope_mode: str) -> List[FileRow]:
    rows: List[FileRow] = []
    for p in files:
        scope = infer_scope_from_path(p) if scope_mode == "Auto-detect" else scope_mode
        bucket = key = None
        if scope in SCOPES:
            bucket, key = infer_bucket_and_key(scope, p)
        rows.append(FileRow(p, scope, bucket, key, sizeof(p)))
    return rows


def run_upload(rows: List[FileRow], session, concurrency: int = 4, dry_run: bool = False, check_s3_duplicate: bool = True) -> Tuple[pd.DataFrame, int, int]:
    results = []
    ok = fail = 0
    prog = st.progress(0)
    total = len(rows)

    def _one(r: FileRow) -> Tuple[str, bool, str]:
        if r.scope not in SCOPES or not r.s3_bucket or not r.s3_key:
            return (r.path.name, False, "Missing scope/bucket/key")
        if dry_run:
            return (r.path.name, True, f"DRY RUN → s3://{r.s3_bucket}/{r.s3_key}")
        # Duplicate check by S3 key
        if check_s3_duplicate and s3_key_exists(session, r.s3_bucket, r.s3_key):
            return (r.path.name, True, f"Skipped (already exists) s3://{r.s3_bucket}/{r.s3_key}")
        # Upload using project helper (SSE + retry)
        try:
            success = upload_file(session=session, bucket=r.s3_bucket, local_path=r.path, s3_key=r.s3_key)
            if success:
                return (r.path.name, True, f"Uploaded to s3://{r.s3_bucket}/{r.s3_key}")
            return (r.path.name, False, "Upload failed")
        except Exception as e:
            return (r.path.name, False, f"Error: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(_one, r) for r in rows]
        for i, f in enumerate(concurrent.futures.as_completed(futures), start=1):
            name, success, message = f.result()
            results.append({"file": name, "success": success, "message": message})
            ok += int(success)
            fail += (0 if success else 1)
            prog.progress(int(i / max(total, 1) * 100))

    return pd.DataFrame(results), ok, fail


# -----------------------------
# Streamlit UI
# -----------------------------

st.set_page_config(page_title="HMA Ingestion UI", page_icon="📤", layout="wide")
st.title("📤 HMA Ingestion – Streamlit UI (Local + S3, with duplicates check)")

with st.sidebar:
    st.header("Configuration")
    scope_mode = st.radio("Scope Mode (existing files)", ["Auto-detect", "mba", "policy"], index=0)
    input_dir = st.text_input("Input directory", value=DEFAULT_INPUT)
    include_exts = st.multiselect("Include types", ["pdf", "csv", "json", "txt", "png", "jpg", "jpeg"], default=DEFAULT_INCLUDE)
    exclude_exts = st.multiselect("Exclude types", ["pdf", "csv", "json", "txt", "png", "jpg", "jpeg"], default=[])
    concurrency = st.slider("Concurrency", 1, 32, 8)
    dry_run_existing = st.checkbox("Dry run (existing files)", value=True)
    check_dup_existing = st.checkbox("Skip if S3 key exists (existing files)", value=True)

    st.divider()
    st.subheader("AWS")
    aws_profile = st.text_input("AWS profile (blank = none)", value=settings.aws_profile or "")
    region = st.text_input("Region", value=settings.aws_default_region)

browse_tab, upload_tab = st.tabs(["🔎 Browse & Upload Existing", "🆕 Upload New Files"]) 

# -------- Tab 1: Browse & upload existing files --------
with browse_tab:
    col1, col2 = st.columns([2,1])
    with col1:
        st.subheader("1) Discover files")
        if st.button("Scan Folder"):
            base = Path(input_dir).resolve()
            try:
                includes = parse_extensions(
                    ",".join(include_exts)
                ) if include_exts else None
                excludes = parse_extensions(
                    ",".join(exclude_exts)
                ) if exclude_exts else None
                files = discover_files(base, includes, excludes)
                rows = build_rows(files, scope_mode)
                df = pd.DataFrame([
                    {"file": r.path.name, "scope": r.scope, "bucket": r.s3_bucket, "s3_key": r.s3_key, "size_kb": round(r.size_bytes/1024,2), "full_path": str(r.path)}
                    for r in rows if (r.scope in SCOPES)
                ])
                if df.empty:
                    st.info("No matching files found.")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.session_state["rows_existing"] = rows
            except Exception as e:
                st.error(f"Scan failed: {e}")
    with col2:
        st.subheader("2) Upload")
        if "rows_existing" not in st.session_state:
            st.caption("Scan first to populate the table.")
        else:
            rows: List[FileRow] = st.session_state["rows_existing"]
            total = sum(1 for r in rows if r.scope in SCOPES)
            st.write(f"Eligible files: **{total}**")
            if st.button("🚀 Start Upload (existing)"):
                elig = [r for r in rows if (r.scope in SCOPES and r.s3_bucket and r.s3_key)]
                if not elig:
                    st.warning("No eligible files.")
                else:
                    with st.spinner("Preparing AWS session..."):
                        session = make_session(aws_profile, region)
                    with st.spinner("Uploading..."):
                        df, ok, fail = run_upload(elig, session, concurrency=concurrency, dry_run=dry_run_existing, check_s3_duplicate=check_dup_existing)
                    st.success(f"Done. OK={ok}, Failed={fail}")
                    st.dataframe(df, use_container_width=True, hide_index=True)

# -------- Tab 2: Upload NEW files (browser → local → S3) --------
with upload_tab:
    st.subheader("Upload NEW PDFs/CSVs → Save locally AND Upload to S3")
    st.caption("A copy will be saved under the input directory, then the file is uploaded to S3. Local name collisions get a (1), (2), … suffix.")

    new_files = st.file_uploader("Choose files", type=["pdf", "csv"], accept_multiple_files=True)
    target_scope = st.radio("Target scope for new files", ["mba", "policy"], index=0)
    dry_run_new = st.checkbox("Dry run for new uploads (no S3)", value=False)
    check_dup_new = st.checkbox("Skip if S3 key exists (new files)", value=True)

    if st.button("⬆️ Save & Upload New Files"):
        if not new_files:
            st.warning("No files selected.")
        else:
            with st.spinner("Preparing AWS session..."):
                session = make_session(aws_profile, region)

            results = []
            ok = fail = 0

            base = Path(input_dir).resolve()
            for uf in new_files:
                name = uf.name
                ext = Path(name).suffix.lower()
                subdir = "pdf" if ext == ".pdf" else ("csv" if ext == ".csv" else "other")

                # 1) Save a local copy under ./data/<scope>/<pdf|csv>/
                local_dir = base / target_scope / subdir
                local_dir.mkdir(parents=True, exist_ok=True)
                local_path = ensure_unique_path(local_dir / name)
                with open(local_path, "wb") as f:
                    f.write(uf.getbuffer())

                # 2) Compute S3 bucket/key using your logic
                bucket, key = infer_bucket_and_key(target_scope, local_path)

                # 3) Upload to S3 (unless dry-run). Check dup by key if enabled.
                if dry_run_new:
                    results.append({"file": name, "saved": str(local_path), "success": True, "message": f"DRY RUN → s3://{bucket}/{key}"})
                    ok += 1
                    continue

                try:
                    if check_dup_new and s3_key_exists(session, bucket, key):
                        results.append({
                            "file": name,
                            "saved": str(local_path),
                            "success": True,
                            "message": f"Skipped (already exists) s3://{bucket}/{key}",
                        })
                        ok += 1
                        continue

                    success = upload_file(session=session, bucket=bucket, local_path=local_path, s3_key=key)
                    if success:
                        results.append({"file": name, "saved": str(local_path), "success": True, "message": f"Uploaded to s3://{bucket}/{key}"})
                        ok += 1
                    else:
                        results.append({"file": name, "saved": str(local_path), "success": False, "message": "Upload failed"})
                        fail += 1
                except Exception as e:
                    results.append({"file": name, "saved": str(local_path), "success": False, "message": f"Error: {e}"})
                    fail += 1

            st.success(f"Completed. OK={ok}, Failed={fail}")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

st.divider()
with st.expander("Troubleshooting"):
    st.markdown(
        """
- **ProfileNotFound**: Leave the *AWS profile* field **blank** or enter a valid named profile from `~/.aws/credentials`. This UI now sanitizes blanks to avoid passing an empty profile.
- If uploads appear to do nothing, check **Dry run** toggles.
- Ensure buckets exist and the region matches. Permissions needed: `s3:PutObject`, `s3:ListBucket`, `s3:HeadObject`.
- Detailed logs still flow through your project's logging config to `logs/app.log`.
        """
    )
