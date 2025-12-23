"""
Streamlit application for analyzing dbt freshness configuration.

Usage:
    streamlit run streamlit_freshness_app.py
"""

import warnings
# Suppress all warnings before any other imports
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore')

import streamlit as st
import pandas as pd
import requests
from log_freshness import DBTFreshnessLogger
import json
import ast
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import sys
import os

# Wrapper to suppress warnings during plotly chart rendering
@contextmanager
def suppress_warnings():
    """Context manager to suppress all warnings during chart rendering"""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # Also suppress stderr temporarily to catch Plotly warnings
        old_stderr = sys.stderr
        sys.stderr = open(os.devnull, 'w')
        try:
            yield
        finally:
            sys.stderr.close()
            sys.stderr = old_stderr

# Wrapper function for plotly_chart that suppresses warnings
def plotly_chart_no_warnings(fig, **kwargs):
    """Wrapper around st.plotly_chart that suppresses deprecation warnings"""
    with suppress_warnings():
        st.plotly_chart(fig, **kwargs)

# dbt Cloud run status codes
RUN_STATUS_CODES = {
    1: 'queued',
    2: 'starting',
    3: 'running',
    10: 'success',
    20: 'error',
    30: 'cancelled',
}

# Common packages to exclude (not part of main project)
EXCLUDED_PACKAGES = [
    'dbt_project_evaluator',
    'dbt_artifacts',
    'dbt_utils',
    'dbt_expectations',
    'codegen',
    'audit_helper',
    'dbt_meta_testing',
    'elementary',
    're_data'
]


def filter_to_main_project(df: pd.DataFrame, package_column: str = 'packageName') -> pd.DataFrame:
    """
    Filter dataframe to only include models from the main project.
    Excludes common dbt packages that users don't control.
    
    Args:
        df: DataFrame with package information
        package_column: Name of the column containing package names
    
    Returns:
        Filtered DataFrame with only main project models
    """
    if package_column not in df.columns:
        return df
    
    # Filter out excluded packages
    mask = ~df[package_column].isin(EXCLUDED_PACKAGES)
    filtered_df = df[mask].copy()
    
    return filtered_df


def determine_job_type(triggers: dict) -> str:
    """
    Determine job type from triggers.
    
    Args:
        triggers: Job triggers dictionary from dbt Cloud API
    
    Returns:
        'ci', 'merge', 'scheduled', or 'other'
    """
    # Has schedule trigger
    if triggers.get('schedule'):
        return 'scheduled'
    
    # Has webhook trigger
    if triggers.get('github_webhook') or triggers.get('on_merge'):
        # Check if custom branch only (CI) or not (merge)
        if triggers.get('custom_branch_only', False):
            return 'ci'
        else:
            return 'merge'
    
    # Everything else
    return 'other'


def filter_jobs_by_type(jobs: list, job_types: list) -> list:
    """
    Filter jobs by their type (ci, merge, scheduled, other).
    
    Args:
        jobs: List of job dictionaries from dbt Cloud API
        job_types: List of job types to include ('ci', 'merge', 'scheduled', 'other')
    
    Returns:
        Filtered list of jobs
    """
    filtered = []
    
    for job in jobs:
        triggers = job.get('triggers', {})
        job_type = determine_job_type(triggers)
        
        if job_type in job_types:
            filtered.append(job)
    
    return filtered


def check_job_has_sao(job_data: dict) -> bool:
    """
    Check if a job has State-Aware Orchestration (SAO) enabled.
    
    Args:
        job_data: Job dictionary from dbt Cloud API (from run's 'job' related object)
    
    Returns:
        True if SAO is enabled, False otherwise
    """
    if not job_data:
        return False
    
    cost_optimization_features = job_data.get('cost_optimization_features', [])
    return 'state_aware_orchestration' in cost_optimization_features


def filter_runs_by_sao(runs: list) -> tuple:
    """
    Filter runs to only include those from jobs with SAO enabled.
    
    Args:
        runs: List of run dictionaries from dbt Cloud API (with 'job' in include_related)
    
    Returns:
        Tuple of (sao_runs, non_sao_runs)
    """
    sao_runs = []
    non_sao_runs = []
    
    for run in runs:
        job_data = run.get('job')
        if check_job_has_sao(job_data):
            sao_runs.append(run)
        else:
            non_sao_runs.append(run)
    
    return sao_runs, non_sao_runs


def get_status_name(status):
    """Convert status code to readable name."""
    if isinstance(status, int):
        return RUN_STATUS_CODES.get(status, f'unknown({status})')
    return status


def get_all_runs_by_date(api_base: str, api_key: str, account_id: str, 
                          start_datetime: datetime, end_datetime: datetime,
                          environment_id: str = None, status: List[int] = None, limit: int = 500,
                          progress_callback=None):
    """
    Fetch runs across ALL jobs in a date range using API-native date filtering.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        start_datetime: Start of date range
        end_datetime: End of date range
        environment_id: Optional environment ID to filter
        status: Optional list of status codes to filter by
        limit: Max total runs to fetch
        progress_callback: Optional callback(message, count) for progress updates
    
    Returns:
        List of run objects (from all jobs, including deleted jobs)
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    API_MAX_LIMIT = 100
    all_runs = []
    seen_run_ids = set()
    
    # If no status filter specified, default to success only
    if status is None:
        status = [10]
    
    # Format datetimes for API (ISO 8601 with Z timezone)
    start_iso = start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_iso = end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    for status_code in status:
        offset = 0
        page_num = 0
        
        while len(all_runs) < limit:
            page_limit = min(API_MAX_LIMIT, limit - len(all_runs))
            page_num += 1
            
            if progress_callback:
                progress_callback(f"Fetching runs (page {page_num}, found {len(all_runs)} so far)...", len(all_runs))
            
            params = {
                'limit': page_limit,
                'offset': offset,
                'order_by': '-id',
                'include_related': '["job","trigger","environment","repository","project"]',
                'status': status_code,
                'created_at__range': f'["{start_iso}","{end_iso}"]'  # SERVER-SIDE date filtering!
            }
            
            # Add environment filter if provided
            if environment_id:
                params['environment_id'] = environment_id
            
            try:
                # DEBUG: Log the actual request being made
                if page_num == 1 and progress_callback:
                    progress_callback(f"DEBUG: API URL: {url}", 0)
                    progress_callback(f"DEBUG: created_at__range param: {params.get('created_at__range')}", 0)
                
                response = requests.get(url, headers=headers, params=params)
                
                # DEBUG: Log the actual URL that was called
                if page_num == 1 and progress_callback:
                    progress_callback(f"DEBUG: Actual URL: {response.url}", 0)
                
                response.raise_for_status()
                
                data = response.json()
                page_runs = data.get('data', [])
                
                # DEBUG: Log first page results
                if page_num == 1 and progress_callback:
                    if page_runs:
                        first_run = page_runs[0]
                        last_run = page_runs[-1]
                        progress_callback(f"DEBUG: Got {len(page_runs)} runs", 0)
                        progress_callback(f"  Newest: Run {first_run.get('id')} created {first_run.get('created_at')}", 0)
                        progress_callback(f"  Oldest: Run {last_run.get('id')} created {last_run.get('created_at')}", 0)
                        progress_callback(f"  Expected range: {start_iso} to {end_iso}", 0)
                        
                        # Check if dates are in expected range
                        first_created = first_run.get('created_at', '')
                        if first_created and (start_iso not in first_created[:10] and end_iso[:10] not in first_created[:10]):
                            progress_callback(f"⚠️  WARNING: Runs are OUTSIDE expected date range! API filtering may not be working.", 0)
                    else:
                        progress_callback(f"DEBUG: API returned 0 runs for date range {start_iso} to {end_iso}", 0)
                
                if not page_runs:
                    break  # No more runs in date range
                
                # Deduplicate and add runs
                for run in page_runs:
                    run_id = run.get('id')
                    if run_id and run_id not in seen_run_ids:
                        seen_run_ids.add(run_id)
                        all_runs.append(run)
                        
                        # Stop if we've hit our limit
                        if len(all_runs) >= limit:
                            break
                
                # If we've hit our limit, stop
                if len(all_runs) >= limit:
                    if progress_callback:
                        progress_callback(f"Collected {len(all_runs)} runs (limit reached)", len(all_runs))
                    break
                
                # If we got fewer runs than requested, we've reached the end
                if len(page_runs) < page_limit:
                    break
                
                offset += len(page_runs)
                
            except requests.exceptions.HTTPError as e:
                if progress_callback:
                    progress_callback(f"Error fetching runs: {e}", len(all_runs))
                break
    
    return all_runs


def get_all_jobs_with_metadata(api_base: str, api_key: str, account_id: str, progress_callback=None):
    """
    Fetch ALL jobs including active and inactive/deleted ones.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        progress_callback: Optional callback(message, count) for progress updates
    
    Returns:
        Dictionary mapping job_id -> job metadata (name, is_active, etc.)
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/jobs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    all_jobs = {}
    offset = 0
    limit = 100
    page_num = 0
    
    while True:
        page_num += 1
        
        if progress_callback:
            progress_callback(f"Fetching job metadata (page {page_num}, {len(all_jobs)} jobs so far)...", len(all_jobs))
        
        params = {
            'limit': limit,
            'offset': offset,
            'order_by': '-id',
            'state': 'all'  # Include deleted/archived jobs
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            page_jobs = data.get('data', [])
            
            if not page_jobs:
                break
            
            for job in page_jobs:
                job_id = job.get('id')
                if job_id:
                    all_jobs[job_id] = {
                        'id': job_id,
                        'name': job.get('name', 'Unknown'),
                        'is_active': job.get('state', 1) == 1,  # state=1 is active, state=2 is deleted
                        'environment_id': job.get('environment_id'),
                        'triggers': job.get('triggers', {})
                    }
            
            # If we got fewer jobs than limit, we've reached the end
            if len(page_jobs) < limit:
                break
            
            offset += len(page_jobs)
            
        except requests.exceptions.HTTPError as e:
            if progress_callback:
                progress_callback(f"Error fetching jobs: {e}", len(all_jobs))
            break
    
    return all_jobs


def get_job_runs(api_base: str, api_key: str, account_id: str, job_id: str, 
                 environment_id: str = None, limit: int = 20, status: List[int] = None):
    """
    Fetch recent runs for a specific job.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        job_id: Job definition ID
        environment_id: Optional environment ID (Note: not used when querying by job_id as it's redundant)
        limit: Max number of runs to fetch (per status if multiple). Will paginate if > 100.
        status: Optional list of status codes to filter by (10=success, 20=error, 30=cancelled)
                Note: API accepts one status per request, so we make multiple calls if needed
    
    Returns:
        List of run objects
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    # API has a max limit of 100 per request
    API_MAX_LIMIT = 100
    
    # If no status filter or only one status, make API call(s) with pagination if needed
    if not status or len(status) == 1:
        all_runs = []
        runs_to_fetch = limit
        offset = 0
        
        # Paginate if limit exceeds API max
        while runs_to_fetch > 0:
            page_limit = min(runs_to_fetch, API_MAX_LIMIT)
            
            params = {
                'limit': page_limit,
                'offset': offset,
                'order_by': '-id',
                'job_definition_id': job_id,
                'include_related': '["job","trigger","environment","repository","project"]',
            }
            
            if status:
                params['status'] = status[0]
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            page_runs = data.get('data', [])
            
            if not page_runs:
                break  # No more runs available
            
            all_runs.extend(page_runs)
            
            # If we got fewer runs than requested, we've reached the end
            if len(page_runs) < page_limit:
                break
            
            runs_to_fetch -= len(page_runs)
            offset += len(page_runs)
        
        return all_runs
    
    # Multiple statuses: make separate API calls (with pagination) and combine results
    all_runs = []
    seen_run_ids = set()
    errors = []
    
    for status_code in status:
        runs_to_fetch = limit
        offset = 0
        
        # Paginate if limit exceeds API max
        while runs_to_fetch > 0:
            page_limit = min(runs_to_fetch, API_MAX_LIMIT)
            
            params = {
                'limit': page_limit,
                'offset': offset,
                'order_by': '-id',
                'job_definition_id': job_id,
                'include_related': '["job","trigger","environment","repository","project"]',
                'status': status_code
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                page_runs = data.get('data', [])
                
                if not page_runs:
                    break  # No more runs available for this status
                
                # Avoid duplicates (shouldn't happen, but just in case)
                for run in page_runs:
                    run_id = run.get('id')
                    if run_id not in seen_run_ids:
                        seen_run_ids.add(run_id)
                        all_runs.append(run)
                
                # If we got fewer runs than requested, we've reached the end
                if len(page_runs) < page_limit:
                    break
                
                runs_to_fetch -= len(page_runs)
                offset += len(page_runs)
                
            except requests.exceptions.HTTPError as e:
                status_name = {10: 'Success', 20: 'Error', 30: 'Cancelled'}.get(status_code, str(status_code))
                # Print the full URL for debugging
                print(f"⚠️ Warning: Failed to fetch {status_name} runs")
                print(f"   URL: {response.url if 'response' in locals() else 'N/A'}")
                print(f"   Error: {str(e)}")
                if hasattr(e.response, 'text'):
                    print(f"   Response: {e.response.text[:500]}")
                error_msg = f"Failed to fetch {status_name} runs (status code {status_code})"
                errors.append(error_msg)
                break  # Stop paginating this status on error
            except Exception as e:
                status_name = {10: 'Success', 20: 'Error', 30: 'Cancelled'}.get(status_code, str(status_code))
                error_msg = f"Failed to fetch {status_name} runs: {str(e)}"
                errors.append(error_msg)
                print(f"Warning: {error_msg}")
                break  # Stop paginating this status on error
    
    # If we got errors but no runs, raise the first error
    if errors and not all_runs:
        raise Exception(f"Failed to fetch runs. Errors: {'; '.join(errors)}")
    
    # If we got some runs but had errors, continue with what we got
    # (errors will be visible in console but won't break the analysis)
    
    # Sort by ID descending (most recent first)
    all_runs.sort(key=lambda x: x.get('id', 0), reverse=True)
    
    # Limit to requested number
    return all_runs[:limit]


def analyze_run_statuses(api_base: str, api_key: str, account_id: str, job_id: str, 
                         start_date: datetime = None, end_date: datetime = None, limit: int = 100):
    """
    Analyze run statuses for a job over a date range.
    
    Returns a dataframe with run status information.
    """
    # Fetch runs
    runs = get_job_runs(api_base, api_key, account_id, job_id, environment_id=None, limit=limit)
    
    # Filter by date if provided
    if start_date or end_date:
        filtered_runs = []
        for run in runs:
            created_at_str = run.get('created_at')
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    # Remove timezone for comparison
                    created_at = created_at.replace(tzinfo=None)
                    
                    if start_date and created_at < start_date:
                        continue
                    if end_date and created_at > end_date:
                        continue
                    
                    filtered_runs.append(run)
                except:
                    pass
        runs = filtered_runs
    
    # Analyze each run
    all_model_statuses = []
    
    for run in runs:
        run_id = run.get('id')
        run_created = run.get('created_at')
        
        # Fetch run status data
        logger = DBTFreshnessLogger(api_base, api_key, account_id, str(run_id))
        try:
            result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
            
            if result and isinstance(result, dict) and 'run_status_data' in result:
                run_status_data = result['run_status_data']
                
                if run_status_data and 'models' in run_status_data:
                    for model in run_status_data['models']:
                        model['run_id'] = run_id
                        model['run_created_at'] = run_created
                        all_model_statuses.append(model)
        except Exception as e:
            st.warning(f"Could not process run {run_id}: {str(e)}")
            continue
    
    if all_model_statuses:
        return pd.DataFrame(all_model_statuses)
    return pd.DataFrame()


def calculate_summary_stats(results):
    """Calculate summary statistics for freshness usage."""
    if not results:
        return None
    
    df = pd.DataFrame(results)
    
    # Extract package from unique_id if not already present
    if 'package' not in df.columns and 'unique_id' in df.columns:
        df['package'] = df['unique_id'].apply(lambda x: x.split('.')[1] if isinstance(x, str) and len(x.split('.')) > 1 else 'unknown')
    
    # Overall stats
    total_items = len(df)
    items_with_freshness = len(df[df['is_freshness_configured'] == True])
    items_without_freshness = total_items - items_with_freshness
    
    # By resource type
    resource_stats = []
    for resource_type in sorted(df['resource_type'].unique()):
        subset = df[df['resource_type'] == resource_type]
        total = len(subset)
        with_freshness = len(subset[subset['is_freshness_configured'] == True])
        pct = (with_freshness / total * 100) if total > 0 else 0
        
        resource_stats.append({
            'Resource Type': resource_type,
            'Total Count': total,
            'With Freshness': with_freshness,
            'Without Freshness': total - with_freshness,
            '% With Freshness': f'{pct:.1f}%'
        })
    
    # By package and resource type
    package_resource_stats = []
    if 'package' in df.columns:
        # Sort packages by count (descending) to show main project first
        package_counts = df['package'].value_counts()
        for package in package_counts.index:
            for resource_type in sorted(df['resource_type'].unique()):
                subset = df[(df['package'] == package) & (df['resource_type'] == resource_type)]
                if len(subset) > 0:  # Only include if there are items
                    total = len(subset)
                    with_freshness = len(subset[subset['is_freshness_configured'] == True])
                    pct = (with_freshness / total * 100) if total > 0 else 0
                    
                    package_resource_stats.append({
                        'Package': package,
            'Resource Type': resource_type,
            'Total Count': total,
            'With Freshness': with_freshness,
            'Without Freshness': total - with_freshness,
            '% With Freshness': f'{pct:.1f}%'
        })
    
    summary = {
        'overall': {
            'total': total_items,
            'with_freshness': items_with_freshness,
            'without_freshness': items_without_freshness,
            'pct_with_freshness': (items_with_freshness / total_items * 100) if total_items > 0 else 0
        },
        'by_resource': pd.DataFrame(resource_stats),
        'by_package_resource': pd.DataFrame(package_resource_stats) if package_resource_stats else None
    }
    
    return summary


def main():
    st.set_page_config(
        page_title="dbt Fusion SAO Status Analyzer",
        page_icon="🔍",
        layout="wide"
    )
    
    # Initialize session state for configuration
    if 'config' not in st.session_state:
        st.session_state.config = {
            'api_base': 'https://cloud.getdbt.com',
            'api_key': '',
            'account_id': '',
            'project_id': '',
            'environment_id': '',
            'configured': False
        }
    
    st.title("🔍 dbt Fusion SAO Status Analyzer")
    
    # Show configuration sidebar
    show_configuration_sidebar()
    
    # Create tabs for different pages
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "⚙️ Configuration",
        "🗑️ Pre-SAO Waste Analysis",
        "🔀 Job Overlap Analysis",
        "📋 Model Details", 
        "📈 Historical Trends",
        "💰 Cost Analysis"
    ])
    
    with tab1:
        show_configuration_page()
    
    with tab2:
        show_pre_sao_waste_analysis()
    
    with tab3:
        show_job_overlap_analysis()
    
    with tab4:
        show_freshness_analysis()
    
    with tab5:
        show_run_status_analysis()
    
    with tab6:
        show_cost_analysis()


def show_configuration_sidebar():
    """Show minimal configuration status in sidebar."""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        if st.session_state.config['configured']:
            st.success("✅ Configured")
            st.caption(f"Account: {st.session_state.config['account_id']}")
            if st.session_state.config.get('environment_id'):
                st.caption(f"Environment: {st.session_state.config['environment_id']}")
            
            if st.button("🔄 Reconfigure", width='stretch'):
                st.session_state.config['configured'] = False
                st.rerun()
        else:
            st.warning("⚠️ Not Configured")
            st.caption("Go to Configuration tab to set up")


def show_configuration_page():
    """Show configuration page for setting up credentials and common settings."""
    st.header("⚙️ Configuration")
    st.markdown("Set up your dbt Cloud credentials and environment settings. These will be used across all analysis pages.")
    
    st.divider()
    
    # Configuration first (at the top)
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔐 dbt Cloud Credentials")
        
        api_base = st.text_input(
            "dbt Cloud URL",
            value=st.session_state.config['api_base'],
            help="Base URL for dbt Cloud instance (e.g., https://cloud.getdbt.com or https://vu491.us1.dbt.com - no trailing slash)",
            key="config_api_base"
        )
        
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config['api_key'],
            type="password",
            help="dbt Cloud API token (keep this secret!)",
            key="config_api_key"
        )
        
        account_id = st.text_input(
            "Account ID",
            value=st.session_state.config['account_id'],
            help="Your dbt Cloud account ID",
            key="config_account_id"
        )
    
    with col2:
        st.subheader("🎯 Environment Settings")
        
        environment_id = st.text_input(
            "Environment ID (Optional)",
            value=st.session_state.config.get('environment_id', ''),
            help="dbt Cloud environment ID for environment-wide analysis",
            key="config_environment_id"
        )
        
        project_id = st.text_input(
            "Project ID (Optional)",
            value=st.session_state.config.get('project_id', ''),
            help="dbt Cloud project ID for filtering",
            key="config_project_id"
        )
    
    st.divider()
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("💾 Save Configuration", type="primary", width='stretch'):
            if not api_key or not account_id:
                st.error("❌ API Key and Account ID are required")
            else:
                st.session_state.config = {
                    'api_base': api_base.rstrip('/'),  # Strip trailing slash to avoid double slashes in URLs
                    'api_key': api_key,
                    'account_id': account_id,
                    'project_id': project_id,
                    'environment_id': environment_id,
                    'configured': True
                }
                st.success("✅ Configuration saved!")
                st.rerun()
    
    with col2:
        if st.button("🔄 Clear Configuration", width='stretch'):
            st.session_state.config = {
                'api_base': 'https://cloud.getdbt.com',
                'api_key': '',
                'account_id': '',
                'project_id': '',
                'environment_id': '',
                'configured': False
            }
            st.success("✅ Configuration cleared!")
            st.rerun()
    
    # Show current status
    if st.session_state.config['configured']:
        st.success("✅ Configuration is saved and ready to use")
        st.info("💡 **Tip**: Environment ID is used for Environment Overview and Job Overlap Analysis. Other tabs let you specify job/run details.")
    else:
        st.warning("⚠️ **Please configure your credentials to start analyzing**")
        st.info("ℹ️ Fill in the required fields and click 'Save Configuration' to get started")

    # Tab descriptions at the bottom
    st.divider()
    st.subheader("📖 About the Analysis Tabs")
    
    with st.expander("🎯 Environment Overview - Main Dashboard"):
        st.markdown("""
        **Your primary source of truth** for environment-wide health and performance.
        - Real-time status from dbt Cloud GraphQL API
        - Key Metrics: Reuse rate, freshness coverage, SLO compliance
        - Automatic Insights and recommendations
        
        **Requires**: Environment ID
        """)
    
    with st.expander("📋 Model Details - Configuration Deep Dive"):
        st.markdown("""
        **Deep dive into individual model configurations** from job manifest.
        - Model-by-model freshness configuration
        - Filter by job type (ci, merge, scheduled, other)
        - Export detailed reports
        
        **Flexible**: Use latest run from environment or specify job ID
        """)
    
    with st.expander("📈 Historical Trends - Performance Over Time"):
        st.markdown("""
        **Analyze performance patterns** across multiple job runs.
        - ⚡ Parallel processing for fast analysis
        - Reuse rate trending over time
        - Filter by job and job type
        
        **Flexible**: Analyze all runs in environment or filter to specific jobs
        """)
    
    with st.expander("💰 Cost Analysis - Financial Impact"):
        st.markdown("""
        **Quantify the financial impact** of your optimization efforts.
        - Cost estimation based on warehouse size
        - Savings from model reuse
        - ROI analysis
        
        **Requires**: Job runs to analyze
        """)
    
    with st.expander("🔀 Job Overlap Analysis - Pre-SAO Optimization"):
        st.markdown("""
        **Identify models being run by multiple jobs** (unnecessary duplication).
        - Job inventory and overlap detection
        - Waste calculation
        - Filter by job types
        
        **Requires**: Environment ID
        **Use case**: Pre-SAO environments to find and eliminate waste
        """)


def show_freshness_analysis():
    """Show detailed freshness configuration analysis."""
    st.header("📋 Model Details")
    st.markdown("Deep dive into individual model configurations and freshness settings from job manifest")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Source selection
    st.subheader("📋 Analysis Source")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        source_mode = st.selectbox(
            "Source",
            ["Environment (Latest)", "Specific Job ID", "Specific Run ID"],
            help="How to select the run to analyze",
            key="freshness_source_mode"
        )
    
    with col2:
        if source_mode == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="dbt Cloud job ID", key="freshness_job_id")
        elif source_mode == "Specific Run ID":
            run_id_input = st.text_input("Run ID", help="dbt Cloud run ID", key="freshness_run_id")
        else:
            job_id_input = None
            run_id_input = None
    
    with col3:
        job_types_filter = st.multiselect(
            "Filter Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter to specific job types (applies to Environment mode)",
            key="freshness_job_types"
        )
    
    col4, col5 = st.columns(2)
    
    with col4:
        run_status_filter = st.multiselect(
            "Run Status Filter",
            options=["Success", "Error", "Cancelled"],
            default=["Success"],
            help="Filter runs by their completion status",
            key="freshness_run_status"
        )
    
    analyze_button = st.button("🔍 Analyze Freshness", type="primary", key="freshness_analyze")
    
    # Main content area
    if not analyze_button:
        st.info("👈 Configure your settings in the sidebar and click 'Analyze Freshness' to begin")
        
        # Show example output
        st.subheader("📊 Example Output")
        st.markdown("""
        This tool will help you:
        - 📋 View all models and sources with their freshness configuration
        - 📈 See summary statistics on freshness adoption
        - 🔎 Identify which configs come from SQL vs YAML
        - 📊 Track freshness coverage across resource types
        """)
        
        return
    
    # Validation
    if source_mode == "Specific Run ID" and not run_id_input:
        st.error("❌ Please provide a Run ID")
        return
    elif source_mode == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID")
        return
    elif source_mode == "Environment (Latest)" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab")
        return
    
    # Process the analysis
    try:
        with st.spinner("🔄 Processing..."):
            run_id = None
            
            # Handle different source modes
            if source_mode == "Environment (Latest)":
                # Get ACTIVE jobs only in the environment with pagination
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                
                jobs = []
                offset = 0
                while True:
                    params = {'environment_id': config['environment_id'], 'limit': 100, 'offset': offset, 'state': 1}  # Active jobs only
                    jobs_response = requests.get(jobs_url, headers=headers, params=params)
                    jobs_response.raise_for_status()
                    page_jobs = jobs_response.json().get('data', [])
                    if not page_jobs:
                        break
                    jobs.extend(page_jobs)
                    if len(page_jobs) < 100:
                        break
                    offset += 100
                
                # Filter by job type
                jobs = filter_jobs_by_type(jobs, job_types_filter)
                
                if not jobs:
                    st.error(f"❌ No jobs found matching job types: {', '.join(job_types_filter)}")
                    return
                
                # Convert status filter to status codes
                status_codes = []
                if "Success" in run_status_filter:
                    status_codes.append(10)
                if "Error" in run_status_filter:
                    status_codes.append(20)
                if "Cancelled" in run_status_filter:
                    status_codes.append(30)
                
                if not status_codes:
                    st.error("❌ Please select at least one run status to filter by")
                    return
                
                # Get latest run matching status filter from filtered jobs
                latest_run = None
                for job in jobs:
                    runs = get_job_runs(
                        config['api_base'],
                        config['api_key'],
                        config['account_id'],
                        str(job['id']),
                        config.get('environment_id'),
                        limit=1,
                        status=status_codes
                    )
                    
                    if runs:
                        if not latest_run or runs[0]['id'] > latest_run['id']:
                            latest_run = runs[0]
                
                if not latest_run:
                    st.error(f"❌ No runs found matching status [{', '.join(run_status_filter)}] for job types: {', '.join(job_types_filter)}")
                    return
                
                run_id = latest_run['id']
                run_status_humanized = RUN_STATUS_CODES.get(latest_run.get('status'), 'unknown')
                st.info(f"📋 Analyzing latest run from environment: {run_id} (Job: {latest_run.get('job_definition_id')}, Status: {run_status_humanized})")
            
            elif source_mode == "Specific Job ID":
                # Convert status filter to status codes
                status_codes = []
                if "Success" in run_status_filter:
                    status_codes.append(10)
                if "Error" in run_status_filter:
                    status_codes.append(20)
                if "Cancelled" in run_status_filter:
                    status_codes.append(30)
                
                if not status_codes:
                    st.error("❌ Please select at least one run status to filter by")
                    return
                
                # Get latest run from specific job
                runs = get_job_runs(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    job_id_input,
                    config.get('environment_id'),
                    limit=1,
                    status=status_codes
                )
                
                if not runs:
                    st.error(f"❌ No runs found for job {job_id_input} with status: {', '.join(run_status_filter)}")
                    return
                
                run_id = runs[0]['id']
                run_status_humanized = RUN_STATUS_CODES.get(runs[0].get('status'), 'unknown')
                st.info(f"📋 Analyzing latest run from job {job_id_input}: {run_id} (Status: {run_status_humanized})")
            
            elif source_mode == "Specific Run ID":
                run_id = int(run_id_input)
                st.info(f"📋 Analyzing run: {run_id}")
            
            # Now analyze the selected run
            with st.status("Analyzing freshness configuration...") as status:
                status.update(label="Fetching manifest...")
                logger = DBTFreshnessLogger(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(run_id)
                )
                
                status.update(label="Processing nodes and sources...")
                results = logger.process_and_log(output_format='json', write_to_db=False)
                
                status.update(label="Analysis complete!", state="complete")
            
            # Display results
            st.success(f"✅ Successfully analyzed {len(results)} items from run {run_id}")
            
            # Calculate summary statistics
            summary = calculate_summary_stats(results)
            
            # Identify main project and show its summary
            df_temp = pd.DataFrame(results)
            if 'package_name' not in df_temp.columns and 'unique_id' in df_temp.columns:
                df_temp['package_name'] = df_temp['unique_id'].apply(lambda x: x.split('.')[1] if isinstance(x, str) and len(x.split('.')) > 1 else 'unknown')
            
            # Find main project (package with most items)
            if 'package_name' in df_temp.columns:
                package_counts = df_temp['package_name'].value_counts()
                if len(package_counts) > 0:
                    main_project = package_counts.index[0]
                    main_df = df_temp[df_temp['package_name'] == main_project]
                    
                    # Calculate metrics for main project
                    main_models = main_df[main_df['resource_type'] == 'model']
                    main_sources = main_df[main_df['resource_type'] == 'source']
                    
                    # Use is_freshness_configured which checks warn_after, error_after, AND build_after
                    main_models_with_freshness = len(main_models[main_models['is_freshness_configured'] == True])
                    main_models_total = len(main_models)
                    main_models_pct = (main_models_with_freshness / main_models_total * 100) if main_models_total > 0 else 0
                    
                    main_sources_with_freshness = len(main_sources[main_sources['is_freshness_configured'] == True])
                    main_sources_total = len(main_sources)
                    main_sources_pct = (main_sources_with_freshness / main_sources_total * 100) if main_sources_total > 0 else 0
                    
                    # Display main project summary
                    st.header(f"📦 Main Project Summary: {main_project}")
                    st.markdown("*These metrics focus on your main project (the package with the most models)*")
                    
                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                    
                    with col1:
                        st.metric("Total Items", len(main_df))
                    
                    with col2:
                        st.metric("Models", main_models_total)
                    
                    with col3:
                        st.metric(
                            "Models w/ Freshness",
                            f"{main_models_pct:.1f}%",
                            delta=f"{main_models_with_freshness}/{main_models_total}"
                        )
                    
                    with col4:
                        st.metric("Sources", main_sources_total)
                    
                    with col5:
                        st.metric(
                            "Sources w/ Freshness",
                            f"{main_sources_pct:.1f}%",
                            delta=f"{main_sources_with_freshness}/{main_sources_total}"
                        )
                    
                    with col6:
                        main_total_with_freshness = len(main_df[main_df['is_freshness_configured'] == True])
                        main_total_pct = (main_total_with_freshness / len(main_df) * 100) if len(main_df) > 0 else 0
                        st.metric("Overall Coverage", f"{main_total_pct:.1f}%")
                    
                    st.divider()
            
            # Show overall summary
            st.header("📊 Summary Statistics (Entire Project including packages)")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Items",
                    summary['overall']['total']
                )
            
            with col2:
                st.metric(
                    "With Freshness",
                    summary['overall']['with_freshness'],
                    delta=f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            with col3:
                st.metric(
                    "Without Freshness",
                    summary['overall']['without_freshness']
                )
            
            with col4:
                st.metric(
                    "Coverage",
                    f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            # Summary tables
            st.subheader("📈 Freshness Coverage by Resource Type")
            st.dataframe(
                summary['by_resource'],
                width="stretch",
                hide_index=True
            )
            
            # Package-level breakdown
            if summary.get('by_package_resource') is not None and not summary['by_package_resource'].empty:
                st.subheader("📦 Freshness Coverage by Package & Resource Type")
                st.markdown("*Packages are sorted by size (largest first = main project)*")
                st.dataframe(
                    summary['by_package_resource'],
                width="stretch",
                hide_index=True
            )
            
            # Detailed results
            st.divider()
            st.header("📋 Detailed Results")
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Extract package from unique_id if not already present
            if 'package_name' not in df.columns and 'unique_id' in df.columns:
                df['package_name'] = df['unique_id'].apply(lambda x: x.split('.')[1] if isinstance(x, str) and len(x.split('.')) > 1 else 'unknown')
            
            # Model Distribution by Build After Configuration
            st.divider()
            st.subheader("📈 Model Distribution by Build After Configuration")
            
            # Filter models with freshness config
            models_with_config = df[df['build_after_count'].notna()].copy()
            
            if len(models_with_config) > 0:
                # Create a combined label for build_after
                models_with_config['build_after_label'] = models_with_config.apply(
                    lambda row: f"{int(row['build_after_count'])} {row['build_after_period']}(s)" 
                    if pd.notna(row['build_after_count']) and pd.notna(row['build_after_period'])
                    else 'Unknown',
                    axis=1
                )
                
                # Count by build_after configuration
                config_counts = models_with_config['build_after_label'].value_counts().reset_index()
                config_counts.columns = ['Build After Config', 'Count']
                
                # Create bar chart
                fig = px.bar(
                    config_counts,
                    x='Build After Config',
                    y='Count',
                    title='Model Count by Build After Configuration',
                    text='Count',
                    color='Count',
                    color_continuous_scale='Blues'
                )
                
                fig.update_traces(textposition='outside')
                fig.update_layout(
                    xaxis_title="Build After Configuration",
                    yaxis_title="Number of Models",
                    showlegend=False,
                    height=400
                )
                
                plotly_chart_no_warnings(fig, use_container_width=True)
                
                # Show breakdown table
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(config_counts, width='stretch', hide_index=True)
                
                with col2:
                    # Pie chart
                    fig_pie = px.pie(
                        config_counts,
                        names='Build After Config',
                        values='Count',
                        title='Build After Configuration Distribution'
                    )
                    plotly_chart_no_warnings(fig_pie, use_container_width=True)
            else:
                st.info("No models have freshness build_after configuration")
            
            st.divider()
            
            # Add filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Get unique projects/packages
                if 'package_name' in df.columns:
                    unique_projects = sorted(df['package_name'].dropna().unique())
                    project_filter = st.multiselect(
                        "Filter by Project/Package",
                        options=unique_projects,
                        default=unique_projects,
                        help="Select which projects/packages to include"
                    )
                else:
                    project_filter = None
            
            with col2:
                resource_filter = st.multiselect(
                    "Filter by Resource Type",
                    options=sorted(df['resource_type'].unique()),
                    default=df['resource_type'].unique()
                )
            
            with col3:
                has_freshness = st.selectbox(
                    "Has Freshness Config",
                    options=["All", "Yes", "No"]
                )
            
            # Add grouping option
            group_by_project = False
            if 'package_name' in df.columns:
                group_by_project = st.checkbox(
                    "📦 Group by Project/Package",
                    value=False,
                    help="Group models by their project/package"
                )
            
            # Apply filters
            filtered_df = df[df['resource_type'].isin(resource_filter)]
            
            # Apply project filter
            if project_filter is not None and 'package_name' in df.columns:
                filtered_df = filtered_df[filtered_df['package_name'].isin(project_filter)]
            
            if has_freshness == "Yes":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == True]
            elif has_freshness == "No":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == False]
            
            # Format the dataframe for display
            display_df = filtered_df.copy()
            
            # Reorder columns for better display (include package_name right after resource_type)
            display_columns = ['name', 'resource_type', 'package_name', 'is_freshness_configured',
                'warn_after_count', 'warn_after_period',
                'error_after_count', 'error_after_period',
                'build_after_count', 'build_after_period',
                'updates_on', 'unique_id']
            display_df = display_df[[col for col in display_columns if col in display_df.columns]]
            
            # Rename columns for better display
            column_rename = {
                'name': 'Name',
                'resource_type': 'Resource Type',
                'package_name': 'Project/Package',
                'is_freshness_configured': 'Has Freshness',
                'warn_after_count': 'Warn Count',
                'warn_after_period': 'Warn Period',
                'error_after_count': 'Error Count',
                'error_after_period': 'Error Period',
                'build_after_count': 'Build Count',
                'build_after_period': 'Build Period',
                'updates_on': 'Updates On',
                'unique_id': 'Unique ID'
            }
            display_df = display_df.rename(columns=column_rename)
            
            # Display with or without grouping
            if group_by_project and 'Project/Package' in display_df.columns:
                st.markdown("### Results Grouped by Project/Package")
                
                # Group by project and display each group
                for project in sorted(display_df['Project/Package'].dropna().unique()):
                    project_df = display_df[display_df['Project/Package'] == project].copy()
                    
                    with st.expander(f"📦 **{project}** ({len(project_df)} items)", expanded=True):
                        # Show project-specific stats
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Items", len(project_df))
                        with col2:
                            has_freshness_count = project_df['Has Freshness'].sum() if 'Has Freshness' in project_df.columns else 0
                            st.metric("With Freshness", has_freshness_count)
                        with col3:
                            freshness_pct = (has_freshness_count / len(project_df) * 100) if len(project_df) > 0 else 0
                            st.metric("Coverage", f"{freshness_pct:.1f}%")
                        
                        # Show the dataframe
                        st.dataframe(
                            project_df,
                            width="stretch",
                            hide_index=True
                        )
            else:
                st.dataframe(
                    display_df,
                    width="stretch",
                    hide_index=True
                )
            
            st.info(f"Showing {len(filtered_df)} of {len(df)} items")
            
            # Download buttons
            st.subheader("💾 Download Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Download full results as JSON
                json_str = json.dumps(results, indent=2, default=str)
                st.download_button(
                    label="📥 Download as JSON",
                    data=json_str,
                    file_name=f"freshness_analysis_run_{run_id}.json",
                    mime="application/json"
                )
            
            with col2:
                # Download as CSV
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"freshness_analysis_run_{run_id}.csv",
                    mime="text/csv"
                )
            
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def fetch_sources_json(api_base: str, api_key: str, account_id: str, run_id: int, step_index: int = None):
    """
    Fetch sources.json artifact from dbt Cloud.
    This contains actual source freshness check results with max_loaded_at timestamps.
    
    Returns None if sources.json doesn't exist (run didn't check freshness).
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        run_id: Run ID
        step_index: Optional step index to fetch from specific step
    
    Returns:
        dict: sources.json data, or None if not found
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/sources.json'
    headers = {'Authorization': f'Token {api_key}'}
    
    params = {}
    if step_index is not None:
        params['step'] = step_index
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None  # sources.json doesn't exist for this run
        raise


def link_models_to_sources(manifest: dict, model_results: list, sources_freshness: dict, staleness_threshold_seconds: int = 86400) -> list:
    """
    Link each model to its upstream source freshness status and CASCADE waste classification down the DAG.
    
    ⚠️ CRITICAL FIX: Case-insensitive status checks (API returns "Error", not "error")
    
    Cascading Logic (3-tier classification):
    - PURE WASTE: If ALL upstream (sources OR models) are pure waste → 100% avoidable
    - PARTIAL WASTE: If ANY waste exists in upstream (pure OR partial) → partially avoidable  
    - JUSTIFIED: If ALL upstream are justified → needed to run
    - If stg_orders is waste (stale source) → dim_orders is waste → fct_orders is waste
    - This captures the full impact of stale sources through the entire lineage
    
    Args:
        manifest: manifest.json data
        model_results: List of model execution results from run_results.json
        sources_freshness: Parsed sources.json with freshness check results
        staleness_threshold_seconds: Threshold in seconds for considering sources stale (default: 24 hours)
        
    Returns:
        Enhanced model_results with source_freshness_classification field
    """
    # Build source freshness lookup: unique_id -> freshness data
    source_lookup = {}
    if sources_freshness:
        for result in sources_freshness.get('results', []):
            source_lookup[result['unique_id']] = {
                'max_loaded_at': result.get('max_loaded_at'),
                'max_loaded_at_time_ago_in_s': result.get('max_loaded_at_time_ago_in_s'),
                'status': result.get('status'),  # pass/warn/error (may be capitalized!)
                'snapshotted_at': result.get('snapshotted_at')
            }
    
    # Build lookup of models that ran in this execution
    models_in_run = {model['unique_id']: model for model in model_results}
    
    # PHASE 1: Classify models with direct source dependencies
    for model in model_results:
        unique_id = model['unique_id']
        
        # Get model dependencies from manifest
        node = manifest.get('nodes', {}).get(unique_id, {})
        depends_on_nodes = node.get('depends_on', {}).get('nodes', [])
        
        # Split dependencies: sources vs models
        source_deps = [dep for dep in depends_on_nodes if dep.startswith('source.')]
        model_deps = [dep for dep in depends_on_nodes if dep.startswith('model.')]
        
        model['upstream_model_deps'] = model_deps  # Store for phase 2
        
        # Check if model has direct source dependencies
        if not source_deps:
            # No direct sources - will check model dependencies in phase 2
            model['source_freshness_classification'] = 'no_direct_sources'
            model['upstream_sources'] = []
        elif not sources_freshness:
            # No sources.json available - classify as JUSTIFIED (can't prove waste)
            # This happens when jobs don't run `dbt source freshness`
            model['source_freshness_classification'] = 'justified'
            model['waste_reason'] = 'no_source_freshness_data'
            model['upstream_sources'] = source_deps
        else:
            # Check freshness of upstream sources
            sources_with_data = []
            
            for source_id in source_deps:
                if source_id in source_lookup:
                    sources_with_data.append({
                        'id': source_id,
                        'freshness': source_lookup[source_id]
                    })
            
            # Classification logic
            if len(sources_with_data) == 0:
                # No freshness data for these specific sources (but sources.json exists for the run)
                # Mark as JUSTIFIED (can't prove waste without data)
                model['source_freshness_classification'] = 'justified'
                model['waste_reason'] = 'sources_not_in_freshness_check'
            elif all(
                # ⚠️ CRITICAL FIX: Case-insensitive status check
                # API returns "Error"/"Warn"/"Pass", not lowercase!
                ((s['freshness'].get('status') or '').lower() in ['error', 'warn']) or
                ((s['freshness'].get('status') or '').lower() == 'pass' and 
                 s['freshness']['max_loaded_at_time_ago_in_s'] > staleness_threshold_seconds)
                for s in sources_with_data
            ):
                # ALL sources are stale (unchanged) - PURE WASTE
                model['source_freshness_classification'] = 'pure_waste'
                model['waste_reason'] = 'all_direct_sources_stale'
            else:
                # At least one source was recently updated - justified
                model['source_freshness_classification'] = 'justified'
                model['waste_reason'] = 'some_direct_sources_fresh'
            
            model['upstream_sources'] = sources_with_data
    
    # PHASE 2: CASCADE waste classification down the DAG
    # Models without direct sources inherit classification from upstream models
    max_iterations = 10  # Prevent infinite loops in circular dependencies
    changed = True
    iteration = 0
    
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        
        for model in model_results:
            # Only process models that haven't been definitively classified yet
            current_classification = model.get('source_freshness_classification')
            if current_classification not in ['no_direct_sources', 'unknown']:
                continue
            
            model_deps = model.get('upstream_model_deps', [])
            
            if not model_deps:
                # No dependencies at all - leave as unknown
                if current_classification != 'unknown':
                    model['source_freshness_classification'] = 'unknown'
                    model['waste_reason'] = 'no_dependencies'
                    changed = True
                continue
            
            # Check classification of upstream models that ran in this execution
            upstream_classifications = []
            for dep_id in model_deps:
                if dep_id in models_in_run:
                    dep_class = models_in_run[dep_id].get('source_freshness_classification')
                    if dep_class and dep_class not in ['no_direct_sources']:  # Only count if classified
                        upstream_classifications.append(dep_class)
            
            if not upstream_classifications:
                # No upstream models have been classified yet - will try next iteration
                continue
            
            # Cascade logic (3-tier classification):
            # - If ALL upstream models are pure_waste → pure_waste (100% avoidable)
            # - If ANY waste (pure OR partial) exists → partial_waste (partially avoidable)
            # - If NO waste (all justified/unknown) → justified
            has_pure_waste = any(c == 'pure_waste' for c in upstream_classifications)
            has_partial_waste = any(c == 'partial_waste' for c in upstream_classifications)
            has_any_waste = has_pure_waste or has_partial_waste
            has_justified = any(c == 'justified' for c in upstream_classifications)
            has_unknown = any(c == 'unknown' for c in upstream_classifications)
            
            if all(c == 'pure_waste' for c in upstream_classifications):
                # ALL upstream is pure waste → this is also pure waste (100% avoidable)
                if current_classification != 'pure_waste':
                    model['source_freshness_classification'] = 'pure_waste'
                    model['waste_reason'] = 'cascaded_from_upstream_models'
                    changed = True
            elif has_any_waste and not has_unknown:
                # Has some waste (pure or partial), may have justified too → partial waste
                if current_classification != 'partial_waste':
                    model['source_freshness_classification'] = 'partial_waste'
                    model['waste_reason'] = 'mixed_upstream_dependencies'
                    changed = True
            elif has_justified and not has_any_waste and not has_unknown:
                # Only justified upstream, no waste → justified
                if current_classification != 'justified':
                    model['source_freshness_classification'] = 'justified'
                    model['waste_reason'] = 'justified_upstream_model'
                    changed = True
            elif has_unknown:
                # Has unknown → stay unknown
                if current_classification != 'unknown':
                    model['source_freshness_classification'] = 'unknown'
                    model['waste_reason'] = 'unknown_upstream_model'
                    changed = True
    
    if iteration > 1:
        print(f"  🔄 Cascaded waste classification through {iteration} levels of the DAG")
    
    # PHASE 3: Final cleanup - any remaining 'no_direct_sources' should be 'unknown'
    for model in model_results:
        if model.get('source_freshness_classification') == 'no_direct_sources':
            model['source_freshness_classification'] = 'unknown'
            model['waste_reason'] = 'unresolved_dependencies'
    
    return model_results


def process_single_run_lightweight(api_base: str, api_key: str, account_id: str, run_id: int, 
                                    run_created: str, job_id: str = None, job_name: str = None, 
                                    run_status: int = None, staleness_threshold_seconds: int = 86400):
    """
    Lightweight version: Only fetches run_results and minimal manifest data for waste analysis.
    Also fetches sources.json for source freshness classification.
    
    Uses STEP-BASED artifact fetching to ensure we get results from dbt run/build steps,
    NOT from dbt compile steps (which would show all models as "success" without actual execution).
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        run_id: Run ID to process
        run_created: Run created timestamp
        job_id: Job definition ID (optional)
        job_name: Job name (optional)
        run_status: Run invocation status code (optional)
    """
    try:
        headers = {'Authorization': f'Token {api_key}'}
        
        # STEP 1: Get run details with steps to find dbt run/build steps
        run_details_url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/'
        response = requests.get(run_details_url, headers=headers, params={'include_related': '["run_steps"]'})
        response.raise_for_status()
        run_details = response.json().get('data', {})
        run_steps = run_details.get('run_steps', [])
        
        # STEP 2: Find relevant steps (dbt run or dbt build - NOT test, compile, etc.)
        # For Pre-SAO Waste Analysis, we only want run/build steps (not test)
        relevant_step_ids = []
        
        # DEBUG: Log all steps we see
        if len(run_steps) > 0:
            print(f"DEBUG Run {run_id}: Found {len(run_steps)} total steps:")
            for step in run_steps:
                step_name = step.get('name', '')
                step_index = step.get('index')
                print(f"  Step {step_index}: {step_name}")
        
        for step in run_steps:
            step_name = step.get('name', '').lower()
            step_id = step.get('id')
            step_index = step.get('index')  # Use index for artifact API!
            # Match common step name patterns for dbt run/build/test commands
            # Using space after command name (e.g., "dbt run ") filters out "dbt run-operation"
            is_invoke_dbt = "invoke dbt" in step_name
            has_run = "dbt run " in step_name or "dbt run`" in step_name
            has_build = "dbt build " in step_name or "dbt build`" in step_name
            has_test = "dbt test " in step_name or "dbt test`" in step_name
            is_excluded = ("dbt source" in step_name or "dbt compile" in step_name or 
                          "dbt docs" in step_name or "dbt deps" in step_name)
            
            # For Pre-SAO Waste, exclude test commands (we only want run/build)
            if is_invoke_dbt and (has_run or has_build) and not is_excluded:
                relevant_step_ids.append(step_index)  # Store step INDEX, not ID!
                print(f"  ✅ MATCHED: {step_name} (step index {step_index})")
            elif is_invoke_dbt:
                print(f"  ❌ SKIPPED: {step_name}")
        
        if not relevant_step_ids:
            # No dbt run/build steps found - this job might only have compile steps
            print(f"⚠️ Run {run_id}: No relevant steps found!")
            return {
                'run_id': run_id, 
                'success': True, 
                'models': [],
                'job_id': job_id,
                'job_name': job_name,
                'run_status': run_status,
                'note': 'No dbt run/build steps found in this run'
            }
        else:
            print(f"✅ Run {run_id}: Found {len(relevant_step_ids)} relevant step(s)")
        
        # STEP 3: Fetch run_results.json from each relevant step and aggregate
        all_results = []
        seen_unique_ids = set()
        
        for step_index in relevant_step_ids:  # Note: variable name is step_ids but contains indices
            run_results_url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/run_results.json'
            params = {'step': step_index}
            
            try:
                print(f"  📥 Fetching run_results.json for step index {step_index}...")
                response = requests.get(run_results_url, headers=headers, params=params)
                response.raise_for_status()
                step_results = response.json()
                
                results_count = len(step_results.get('results', []))
                print(f"  ✅ Got {results_count} results from step index {step_index}")
                
                for result in step_results.get('results', []):
                    unique_id = result.get('unique_id')
                    if unique_id and unique_id not in seen_unique_ids:
                        all_results.append(result)
                        seen_unique_ids.add(unique_id)
                
                print(f"  📊 Total unique results so far: {len(all_results)}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"  ⚠️  No run_results.json found for step index {step_index} (404)")
                    continue
                print(f"  ❌ HTTP Error fetching step index {step_index}: {e}")
                raise
            except Exception as e:
                print(f"  ❌ Unexpected error fetching step index {step_index}: {e}")
                raise
        
        if not all_results:
            print(f"⚠️ Run {run_id}: No results found in run_results.json from any step!")
            return {
                'run_id': run_id, 
                'success': True, 
                'models': [],
                'job_id': job_id,
                'job_name': job_name,
                'run_status': run_status,
                'note': 'No run_results found in dbt run/build steps'
            }
        
        print(f"✅ Run {run_id}: Found {len(all_results)} total results, now filtering to models...")
        
        # STEP 3.5: Try to fetch sources.json for source freshness analysis
        sources_freshness = None
        
        # First, look for dedicated `dbt source freshness` steps
        source_freshness_step_ids = []
        for step in run_steps:
            step_name = step.get('name', '').lower()
            step_index = step.get('index')
            is_invoke_dbt = "invoke dbt" in step_name
            has_source_freshness = "dbt source freshness" in step_name or "source freshness" in step_name
            
            if is_invoke_dbt and has_source_freshness:
                source_freshness_step_ids.append(step_index)
                print(f"  🔍 Found source freshness step: {step.get('name')} (index {step_index})")
        
        # Try source freshness steps first
        for step_index in source_freshness_step_ids:
            try:
                sources_freshness = fetch_sources_json(api_base, api_key, account_id, run_id, step_index)
                if sources_freshness:
                    print(f"  ✅ Found sources.json in source freshness step {step_index}")
                    break
            except Exception as e:
                continue
        
        # Fallback: try without step parameter (for 0-step runs)
        if not sources_freshness and len(run_steps) == 0:
            print(f"  📥 Trying sources.json without step parameter (0-step run)...")
            try:
                sources_freshness = fetch_sources_json(api_base, api_key, account_id, run_id, step_index=None)
                if sources_freshness:
                    print(f"  ✅ Found sources.json at run level (no step filtering)")
            except Exception as e:
                print(f"  ℹ️  sources.json not available at run level: {e}")
        
        if not sources_freshness:
            print(f"  ℹ️  No sources.json found - source freshness classification unavailable")
        
        # STEP 4: Fetch manifest for materialization lookup
        manifest_url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json'
        response = requests.get(manifest_url, headers=headers)
        response.raise_for_status()
        manifest = response.json()
        
        # STEP 5: Process results
        models = []
        
        # Debug: Show what types of results we have
        result_types = {}
        for result in all_results:
            unique_id = result.get('unique_id', 'unknown')
            result_type = unique_id.split('.')[0] if '.' in unique_id else 'unknown'
            result_types[result_type] = result_types.get(result_type, 0) + 1
        print(f"  📊 Result types in run_results.json: {result_types}")
        
        for result in all_results:
            unique_id = result.get('unique_id')
            
            # Only process models (not sources, tests, etc.)
            if not unique_id or not unique_id.startswith('model.'):
                continue
            
            # Get node info
            node = manifest.get('nodes', {}).get(unique_id, {})
            resource_type = node.get('resource_type')
            
            if resource_type != 'model':
                continue
            
            # Get status and execution details
            status = result.get('status')
            execution_time = result.get('execution_time', 0)
            
            # Get timing
            timing = result.get('timing', [])
            started_at = None
            completed_at = None
            for t in timing:
                if t.get('name') == 'execute':
                    started_at = t.get('started_at')
                    completed_at = t.get('completed_at')
                    break
            
            # Get rows affected and materialization
            adapter_response = result.get('adapter_response', {})
            rows_affected = adapter_response.get('rows_affected')
            materialization = node.get('config', {}).get('materialized')
            
            model_data = {
                'unique_id': unique_id,
                'name': unique_id.split('.')[-1] if unique_id else 'unknown',
                'resource_type': resource_type,
                'status': status,
                'execution_time': execution_time,
                'started_at': started_at,
                'completed_at': completed_at,
                'rows_affected': rows_affected,
                'materialization': materialization,
                'adapter_response': adapter_response,
                'message': result.get('message'),
                'run_id': run_id,
                'run_created_at': run_created,
                'job_id': job_id,
                'job_name': job_name,
                'run_status': run_status
            }
            models.append(model_data)
        
        print(f"✅ Run {run_id}: Extracted {len(models)} models from {len(all_results)} results")
        
        # STEP 6: Link models to source freshness data
        if sources_freshness and models:
            print(f"  🔗 Linking {len(models)} models to source freshness data...")
            print(f"  Sources in sources.json: {len(sources_freshness.get('results', []))}")
            
            models = link_models_to_sources(manifest, models, sources_freshness, staleness_threshold_seconds)
            
            # Debug: Show classification breakdown
            classifications = {}
            for model in models:
                classification = model.get('source_freshness_classification', 'unknown')
                classifications[classification] = classifications.get(classification, 0) + 1
            
            print(f"  ✅ Classification breakdown: {classifications}")
        
        return {
            'run_id': run_id,
            'success': True,
            'models': models,
            'job_id': job_id,
            'job_name': job_name,
            'run_status': run_status,
            'has_source_freshness': sources_freshness is not None
        }
        
    except Exception as e:
        return {'run_id': run_id, 'success': False, 'error': str(e)}


def process_single_run(api_base: str, api_key: str, account_id: str, run_id: int, run_created: str, 
                       job_id: str = None, job_name: str = None, run_status: int = None,
                       project_id: str = None, environment_id: str = None):
    """
    Process a single run to extract model status data (includes freshness configs).
    
    This function is designed to be called in parallel for multiple runs.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        run_id: Run ID to process
        run_created: Run created timestamp
        job_id: Job definition ID (optional)
        job_name: Job name (optional)
        run_status: Run invocation status code (optional)
    """
    try:
        logger = DBTFreshnessLogger(api_base, api_key, account_id, str(run_id))
        result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
        
        if result and isinstance(result, dict) and 'run_status_data' in result:
            run_status_data = result['run_status_data']
            
            if run_status_data and 'models' in run_status_data:
                models = []
                for model in run_status_data['models']:
                    model['run_id'] = run_id
                    model['run_created_at'] = run_created
                    model['job_id'] = job_id
                    model['job_name'] = job_name
                    model['run_status'] = run_status
                    model['project_id'] = project_id
                    model['environment_id'] = environment_id
                    models.append(model)
                return {
                    'run_id': run_id, 
                    'success': True, 
                    'models': models,
                    'job_id': job_id,
                    'job_name': job_name,
                    'run_status': run_status
                }
        
        return {'run_id': run_id, 'success': False, 'error': 'No data in response'}
    except Exception as e:
        return {'run_id': run_id, 'success': False, 'error': str(e)}


def show_run_status_analysis():
    """Show historical trends analysis tab."""
    st.header("📈 Historical Trends")
    st.markdown("Analyze performance patterns and execution trends across multiple job runs over time")
    
    st.info("⚡ **Parallel Processing**: Analyzes up to 10 runs simultaneously for 5-10x faster results!")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Analysis parameters
    st.subheader("📋 Analysis Scope")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Default to last 7 days (reduced for performance)
        default_start = datetime.now() - timedelta(days=7)
        default_end = datetime.now()
        
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of date range"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=default_end,
            help="End of date range"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs",
            min_value=5,
            max_value=80,
            value=10,
            help="Maximum number of runs to analyze (from total found)"
        )
    
    with col4:
        job_source = st.selectbox(
            "Job Source",
            options=["All Jobs in Environment", "Specific Job ID"],
            help="Analyze all jobs or filter to one"
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if job_source == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="Specific dbt Cloud job ID")
        else:
            job_id_input = None
    
    with col2:
        job_types_filter = st.multiselect(
            "Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter by job type"
        )
    
    col3, col4 = st.columns(2)
    
    with col3:
        run_status_filter = st.multiselect(
            "Run Status",
            options=["Success", "Error", "Cancelled"],
            default=["Success"],
            help="Filter runs by completion status"
        )
    
    with col4:
        sao_only = st.checkbox(
            "SAO Jobs Only",
            value=True,
            help="Only analyze jobs with State-Aware Orchestration enabled (recommended for accurate reuse metrics)",
            key="run_sao_only"
        )
    
    col5, col6 = st.columns(2)
    
    with col5:
        include_deleted_jobs = st.checkbox(
            "Include Deleted/Archived Jobs",
            value=False,
            help="Include runs from jobs that have been deleted or archived. Essential for historical analysis!",
            key="run_include_deleted"
        )
    
    # Performance tip
    if include_deleted_jobs:
        st.info("💡 **Historical Mode**: Fetching runs by date range first, then enriching with job metadata. This captures runs from deleted jobs!")
    else:
        st.info("💡 **Performance Tip**: Runs are processed in parallel (up to 10 at a time) for much faster analysis! Feel free to analyze 20-50 runs.")
    
    analyze_button = st.button("📊 Analyze Run Statuses", type="primary", key="run_analyze")
    
    # Main content
    if not analyze_button:
        st.info("⬆️ Set parameters and click 'Analyze Run Statuses' to begin")
        return
    
    # Validation
    if job_source == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID when using 'Specific Job ID' mode")
        return
    elif job_source == "All Jobs in Environment" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab to use 'All Jobs in Environment' mode")
        return
    
    # Convert status filter to status codes
    status_codes = []
    if "Success" in run_status_filter:
        status_codes.append(10)
    if "Error" in run_status_filter:
        status_codes.append(20)
    if "Cancelled" in run_status_filter:
        status_codes.append(30)
    
    if not status_codes:
        st.error("❌ Please select at least one run status to filter by")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Create progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text(f"🔄 Fetching runs from {start_date} to {end_date}...")
        
        # Fetch runs based on job source
        runs = []
        all_jobs_dict = {}  # For job metadata enrichment
        
        if job_source == "All Jobs in Environment":
            
            if include_deleted_jobs:
                # HISTORICAL MODE: Fetch runs by date first, then enrich with job metadata
                # This captures runs from deleted/archived jobs!
                status_text.text(f"🔄 Fetching ALL runs in date range (including deleted jobs)...")
                
                def update_progress(msg, count):
                    status_text.text(msg)
                
                runs = get_all_runs_by_date(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    start_datetime,
                    end_datetime,
                    environment_id=config.get('environment_id'),
                    status=status_codes,
                    limit=max_runs * 3,  # Fetch extra to allow for filtering
                    progress_callback=update_progress
                )
                
                progress_bar.progress(5)
                
                # Get all jobs (including deleted ones) for metadata
                status_text.text(f"🔄 Fetching job metadata (including deleted jobs)...")
                all_jobs_dict = get_all_jobs_with_metadata(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    progress_callback=update_progress
                )
                
                progress_bar.progress(8)
                
                # Enrich runs with job metadata
                for run in runs:
                    job_def_id = run.get('job_definition_id')
                    if job_def_id and job_def_id in all_jobs_dict:
                        job_info = all_jobs_dict[job_def_id]
                        run['job_name'] = job_info['name']
                        run['job_is_active'] = job_info['is_active']
                        run['job_triggers'] = job_info['triggers']
                    else:
                        # Job truly deleted and not in API response
                        run['job_name'] = f"Deleted Job ({job_def_id})"
                        run['job_is_active'] = False
                        run['job_triggers'] = {}
                
                # Filter by job type if specified
                if job_types_filter:
                    filtered_runs = []
                    for run in runs:
                        job_type = determine_job_type(run.get('job_triggers', {}))
                        if job_type in job_types_filter:
                            filtered_runs.append(run)
                    
                    status_text.text(f"✅ Filtered to {len(filtered_runs)} runs matching job types: {', '.join(job_types_filter)}")
                    runs = filtered_runs
                
                progress_bar.progress(10)
                
            else:
                # STANDARD MODE: Fetch active jobs first, then get their runs
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                params = {'environment_id': config['environment_id'], 'limit': 100}
                
                jobs_response = requests.get(jobs_url, headers=headers, params=params)
                jobs_response.raise_for_status()
                jobs = jobs_response.json().get('data', [])
                
                # Filter by job type
                jobs = filter_jobs_by_type(jobs, job_types_filter)
                
                if not jobs:
                    st.error(f"❌ No active jobs found matching job types: {', '.join(job_types_filter)}. Try enabling 'Include Deleted/Archived Jobs' for historical analysis!")
                    progress_bar.empty()
                    status_text.empty()
                    return
                
                status_text.text(f"🔄 Found {len(jobs)} active jobs. Fetching runs...")
                
                # Get runs from all filtered jobs
                fetch_limit = min(200, max_runs * 3)
                all_runs = []
                for idx, job in enumerate(jobs):
                    job_runs = get_job_runs(
                        config['api_base'],
                        config['api_key'],
                        config['account_id'],
                        str(job['id']),
                        config.get('environment_id'),
                        limit=fetch_limit,
                        status=status_codes
                    )
                    all_runs.extend(job_runs)
                    
                    progress = int((idx + 1) / len(jobs) * 10)
                    progress_bar.progress(progress)
                
                runs = sorted(all_runs, key=lambda x: x.get('created_at', ''), reverse=True)
            
        else:  # Specific Job ID
            fetch_limit = min(200, max_runs * 3)
            runs = get_job_runs(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                job_id_input,
                config.get('environment_id'),
                limit=fetch_limit,
                status=status_codes
            )
        
        # Store count before date filtering
        runs_before_date_filter = len(runs)
        
        # Filter by date
        if start_datetime or end_datetime:
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if start_datetime and created_at < start_datetime:
                            continue
                        if end_datetime and created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
        
        if not runs:
            st.warning(f"No runs found in the specified date range ({start_date} to {end_date})")
            st.info(f"ℹ️ Found {runs_before_date_filter} total runs, but none matched the date range filter.")
            progress_bar.empty()
            status_text.empty()
            return
        
        # Store count after date filtering but before SAO filtering
        runs_after_date_filter = len(runs)
        
        # Filter by SAO if requested
        if sao_only:
            status_text.text(f"🔍 Filtering to SAO-enabled jobs only...")
            sao_runs, non_sao_runs = filter_runs_by_sao(runs)
            runs = sao_runs
            
            if not runs:
                st.warning(f"⚠️ No runs found from SAO-enabled jobs. Found {len(non_sao_runs)} runs from non-SAO jobs.")
                st.info("💡 **Tip**: Uncheck 'SAO Jobs Only' to analyze all jobs, or enable SAO on your jobs for better reuse metrics.")
                progress_bar.empty()
                status_text.empty()
                return
            
            if non_sao_runs:
                st.info(f"ℹ️ Filtered to {len(runs)} SAO-enabled runs (excluded {len(non_sao_runs)} non-SAO runs)")
        
        # Now limit to max_runs AFTER all filtering
        total_runs_found = len(runs)
        runs = runs[:max_runs]
        
        # Better status message
        if total_runs_found > max_runs:
            status_text.text(f"✅ Found {total_runs_found} runs in date range, analyzing {len(runs)} most recent (limited by slider)...")
        else:
            status_text.text(f"✅ Found {total_runs_found} runs in date range. Analyzing all in parallel...")
        progress_bar.progress(10)
        
        # Analyze runs in parallel using ThreadPoolExecutor
        all_model_statuses = []
        completed_runs = 0
        failed_runs = []
        
        # Determine optimal number of workers (max 10 concurrent requests)
        max_workers = min(10, len(runs))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all run processing tasks
            future_to_run = {
                executor.submit(
                    process_single_run,
                config['api_base'],
                config['api_key'],
                config['account_id'],
                    run.get('id'),
                    run.get('created_at'),
                    run.get('job_definition_id'),
                    run.get('job', {}).get('name') if run.get('job') else None,
                    run.get('status'),
                    run.get('project_id'),
                    run.get('environment_id')
                ): run.get('id') for run in runs
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_run):
                run_id = future_to_run[future]
                completed_runs += 1
                
                # Update progress
                progress = 10 + int((completed_runs / len(runs)) * 80)
                progress_bar.progress(progress)
                status_text.text(f"🔄 Processed {completed_runs}/{len(runs)} runs (in parallel)...")
                
                try:
                    result = future.result()
                    if result['success']:
                        all_model_statuses.extend(result['models'])
                    else:
                        failed_runs.append((run_id, result.get('error', 'Unknown error')))
                except Exception as e:
                    failed_runs.append((run_id, str(e)))
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Show warnings for failed runs
        if failed_runs:
            with st.expander(f"⚠️ {len(failed_runs)} run(s) failed to process", expanded=False):
                for run_id, error in failed_runs:
                    st.warning(f"Run {run_id}: {error}")
        
        # Convert to dataframe
        if all_model_statuses:
            df = pd.DataFrame(all_model_statuses)
        else:
            df = pd.DataFrame()
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        if df.empty:
            st.warning("No run data found for the specified date range and job")
            return
        
        st.success(f"✅ Analyzed {len(df)} model executions across {df['run_id'].nunique()} runs")
        
        # Diagnostic info
        with st.expander("🔍 Status Breakdown"):
            st.markdown("**Status values found:**")
            status_breakdown = df['status'].value_counts()
            for status, count in status_breakdown.items():
                pct = (count / len(df) * 100) if len(df) > 0 else 0
                st.text(f"  {status}: {count:,} ({pct:.1f}%)")
            
            st.markdown("""
            **How we detect model status:**  
            We use the **step-based approach** to get accurate status from `run_results.json`:
            
            1. **Identify relevant steps**: Filter to only `dbt run`, `dbt build`, and `dbt test` commands
            2. **Fetch run_results.json**: Get results from each relevant step
            3. **Aggregate results**: Combine data across all steps for accurate counts
            
            This eliminates guesswork and heuristics - we use the actual status from dbt's execution results!
            
            **Status values:**
            - `success`: Model executed successfully
            - `error`: Model execution failed  
            - `skipped`: Model was skipped (often due to deferral/reuse)
            """)
        
        # Summary Statistics
        st.header("📈 Summary Statistics")
        
        # Calculate status counts
        status_counts = df['status'].value_counts()
        total_executions = len(df)
        success_count = status_counts.get('success', 0)
        # Check for both 'skipped' and 'reused' status
        reused_count = status_counts.get('skipped', 0) + status_counts.get('reused', 0)
        error_count = status_counts.get('error', 0)
        
        # Calculate percentages
        success_pct = (success_count / total_executions * 100) if total_executions > 0 else 0
        reused_pct = (reused_count / total_executions * 100) if total_executions > 0 else 0
        error_pct = (error_count / total_executions * 100) if total_executions > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Executions", f"{total_executions:,}")
        
        with col2:
            st.metric("Success", f"{success_count:,}", delta=f"{success_pct:.1f}%")
        
        with col3:
            st.metric("Reused", f"{reused_count:,}", delta=f"{reused_pct:.1f}%")
        
        with col4:
            st.metric("Reuse Rate", f"{reused_pct:.1f}%")
        
        # TRENDING ANALYSIS
        st.divider()
        st.subheader("📈 Reuse Rate Trending")
        
        # Calculate reuse rate per run
        reuse_trending = df.groupby(['run_id', 'run_created_at']).apply(
            lambda x: pd.Series({
                'reuse_rate': (len(x[x['status'].isin(['reused', 'skipped'])]) / len(x) * 100) if len(x) > 0 else 0,
                'total_models': len(x),
                'reused_count': len(x[x['status'].isin(['reused', 'skipped'])]),
                'success_count': len(x[x['status'] == 'success']),
                'error_count': len(x[x['status'] == 'error'])
            })
        ).reset_index()
        
        reuse_trending = reuse_trending.sort_values('run_created_at')
        reuse_trending['run_created_at'] = pd.to_datetime(reuse_trending['run_created_at'])
        
        # Create trending chart
        fig_trending = go.Figure()
        
        # Reuse rate line
        fig_trending.add_trace(go.Scatter(
            x=reuse_trending['run_created_at'],
            y=reuse_trending['reuse_rate'],
            name='Reuse Rate %',
            mode='lines+markers',
            line=dict(color='#17a2b8', width=3),
            marker=dict(size=8)
        ))
        
        # Add 30% goal line
        fig_trending.add_hline(
            y=30,
            line_dash="dash",
            line_color="green",
            annotation_text="30% Goal",
            annotation_position="right"
        )
        
        fig_trending.update_layout(
            title="Reuse Rate Trend Over Time",
            xaxis_title="Date",
            yaxis_title="Reuse Rate (%)",
            hovermode='x unified',
            height=400
        )
        
        plotly_chart_no_warnings(fig_trending, use_container_width=True)
        
        # Show statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_reuse = reuse_trending['reuse_rate'].mean()
            st.metric("Average Reuse Rate", f"{avg_reuse:.1f}%")
        
        with col2:
            max_reuse = reuse_trending['reuse_rate'].max()
            st.metric("Peak Reuse Rate", f"{max_reuse:.1f}%")
        
        with col3:
            min_reuse = reuse_trending['reuse_rate'].min()
            st.metric("Lowest Reuse Rate", f"{min_reuse:.1f}%")
        
        with col4:
            # Calculate trend (positive/negative)
            if len(reuse_trending) >= 2:
                recent_avg = reuse_trending.tail(5)['reuse_rate'].mean()
                older_avg = reuse_trending.head(5)['reuse_rate'].mean()
                trend = recent_avg - older_avg
                st.metric("Trend", f"{trend:+.1f}%", delta=f"{'Improving' if trend > 0 else 'Declining'}")
            else:
                st.metric("Trend", "N/A")
        
        # Status breakdown by run
        st.divider()
        st.subheader("📊 Status Distribution by Run")
        
        # Group by run and status
        run_status_summary = df.groupby(['run_id', 'run_created_at', 'status']).size().reset_index(name='count')
        
        # Pivot for stacked bar chart
        pivot_df = run_status_summary.pivot_table(
            index=['run_id', 'run_created_at'],
            columns='status',
            values='count',
            fill_value=0
        ).reset_index()
        
        # Sort by date
        pivot_df['run_created_at'] = pd.to_datetime(pivot_df['run_created_at'])
        pivot_df = pivot_df.sort_values('run_created_at')
        
        # Create stacked bar chart
        fig = go.Figure()
        
        # Define colors for each status
        status_colors = {
            'success': '#22c55e',  # green
            'error': '#ef4444',    # red
            'reused': '#60a5fa',   # light blue
            'skipped': '#9ca3af'   # grey
        }
        
        # Add bars for each status
        for status in pivot_df.columns[2:]:  # Skip run_id and run_created_at
            fig.add_trace(go.Bar(
                name=status,
                x=pivot_df['run_created_at'],
                y=pivot_df[status],
                text=pivot_df[status],
                textposition='inside',
                marker_color=status_colors.get(status, '#6b7280')  # default to gray if status not found
            ))
        
        fig.update_layout(
            title="Model Execution Status by Run",
            xaxis_title="Run Date",
            yaxis_title="Number of Models",
            barmode='stack',
            hovermode='x unified',
            height=500
        )
        
        plotly_chart_no_warnings(fig, use_container_width=True)
        
        # Detailed breakdown table
        st.subheader("📋 Detailed Run Breakdown")
        
        st.markdown("""
        **Status values** are extracted from `run_results.json` using a step-based approach that filters to only `dbt run`, `dbt build`, and `dbt test` commands.
        """)
        
        # Create summary by run
        run_summary = df.groupby(['run_id', 'run_created_at']).agg({
            'status': lambda x: x.value_counts().to_dict(),
            'job_id': 'first',
            'job_name': 'first',
            'run_status': 'first',
            'project_id': 'first',
            'environment_id': 'first'
        }).reset_index()
        
        # Calculate execution time stats separately
        exec_time_stats = df.groupby(['run_id']).agg({
            'execution_time': ['mean', 'median']
        }).reset_index()
        exec_time_stats.columns = ['run_id', 'avg_exec_time', 'median_exec_time']
        
        # Merge execution time stats
        run_summary = run_summary.merge(exec_time_stats, on='run_id', how='left')
        
        # Map run_status codes to human-readable names
        run_summary['run_status_name'] = run_summary['run_status'].map(RUN_STATUS_CODES)
        
        # Expand status counts
        status_columns = []
        for status in df['status'].unique():
            run_summary[status] = run_summary['status'].apply(lambda x: x.get(status, 0))
            status_columns.append(status)
        
        # Calculate totals and percentages
        run_summary['total'] = run_summary[status_columns].sum(axis=1)
        
        # Calculate reuse rate if we have skipped or reused statuses
        has_reuse_column = False
        if 'skipped' in status_columns or 'reused' in status_columns:
            # Sum skipped and reused columns (handle if they don't exist)
            skipped_col = run_summary['skipped'] if 'skipped' in status_columns else 0
            reused_col_data = run_summary['reused'] if 'reused' in status_columns else 0
            total_reused = skipped_col + reused_col_data
            run_summary['reuse_rate_%'] = (total_reused / run_summary['total'] * 100).round(1)
            has_reuse_column = True
        
        # Format for display
        display_columns = ['run_created_at', 'run_id', 'project_id', 'environment_id', 'job_id', 'job_name', 'run_status_name', 'total'] + status_columns
        if has_reuse_column:
            display_columns.append('reuse_rate_%')
        
        # Add execution time to display columns
        display_columns.extend(['avg_exec_time', 'median_exec_time'])
        
        display_summary = run_summary[display_columns].copy()
        display_summary['run_created_at'] = pd.to_datetime(display_summary['run_created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Round execution times
        display_summary['avg_exec_time'] = display_summary['avg_exec_time'].round(3)
        display_summary['median_exec_time'] = display_summary['median_exec_time'].round(3)
        
        # Set column names - must match display_columns order
        new_column_names = ['Run Date', 'Run ID', 'Project ID', 'Environment ID', 'Job ID', 'Job Name', 'Run Status', 'Total'] + [s.title() for s in status_columns]
        if has_reuse_column:
            new_column_names.append('Reuse Rate %')
        new_column_names.extend(['Avg Time (s)', 'Median Time (s)'])
        display_summary.columns = new_column_names
        
        st.dataframe(display_summary, width='stretch', hide_index=True)
        
        # Download option
        st.subheader("💾 Download Data")
        csv = df.to_csv(index=False)
        
        # Generate filename based on job source
        if job_source == "All Jobs in Environment":
            filename_suffix = f"environment_{config.get('environment_id', 'all')}"
        else:
            filename_suffix = f"job_{job_id_input}"
        
        st.download_button(
            label="📥 Download Full Run Data as CSV",
            data=csv,
            file_name=f"run_status_analysis_{filename_suffix}_{start_date}_to_{end_date}.csv",
            mime="text/csv"
        )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def fetch_all_models_graphql(api_key: str, environment_id: int, page_size: int = 500):
    """
    Fetch all models from dbt Cloud using GraphQL API.
    Returns a list of model nodes with execution info and config.
    """
    url = 'https://metadata.cloud.getdbt.com/graphql'
    headers = {'Authorization': f'Bearer {api_key}'}
    
    query_body = '''
    query Environment($environmentId: BigInt!, $first: Int, $after: String) {
      environment(id: $environmentId) {
        applied {
          models(first: $first, after: $after) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
            }
            edges {
              node {
                config
                name
                packageName
                resourceType
                executionInfo {
                  lastRunGeneratedAt
                  lastRunStatus
                  lastSuccessJobDefinitionId
                  lastSuccessRunId
                  lastRunError
                  executeCompletedAt
                }
              }
            }
          }
        }
      }
    }
    '''
    
    all_nodes = []
    has_next_page = True
    cursor = None
    page_count = 0
    
    while has_next_page:
        page_count += 1
        
        variables = {
            "environmentId": environment_id,
            "first": page_size
        }
        
        if cursor:
            variables["after"] = cursor
        
        try:
            response = requests.post(
                url, 
                headers=headers, 
                json={"query": query_body, "variables": variables}
            )
            response.raise_for_status()
            response_data = response.json()
            
            if "errors" in response_data:
                st.error(f"GraphQL errors: {response_data['errors']}")
                break
            
            models_data = response_data.get("data", {}).get("environment", {}).get("applied", {}).get("models", {})
            
            if not models_data:
                break
            
            page_info = models_data.get("pageInfo", {})
            edges = models_data.get("edges", [])
            
            if not edges:
                break
            
            nodes = [edge['node'] for edge in edges]
            all_nodes.extend(nodes)
            
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            if has_next_page and not cursor:
                break
                
        except requests.exceptions.RequestException as e:
            st.error(f"Request error: {e}")
            break
        except KeyError as e:
            st.error(f"Error parsing response: {e}")
            break
    
    return all_nodes


def show_cost_analysis():
    """Show cost analysis and ROI tracking."""
    st.header("💰 Cost Analysis & ROI")
    st.markdown("Quantify the financial impact of your dbt optimization efforts and model reuse strategy")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Cost Configuration Section
    st.subheader("⚙️ Cost Configuration")
    
    with st.expander("📘 How Cost Calculation Works", expanded=False):
        st.markdown("""
        ### Cost Calculation Method
        
        **For models that execute (status = success)**:
        - `Cost = (Execution Time in seconds / 3600) × Warehouse Cost per Hour`
        
        **For reused models**:
        - **Actual Cost = $0** (they skip execution)
        - **Savings = Average execution time when model runs × Cost per Hour**
        
        ### Example
        ```
        Model: fct_orders
        - When it runs (status=success): Takes 300 seconds
        - Average run time: 300 seconds
        - Cost when it runs: (300/3600) × $4 = $0.33
        
        When reused:
        - Actual cost: $0.00
        - Savings: $0.33 (what we would have paid)
        ```
        
        ### Warehouse Costs (Typical Snowflake Pricing)
        Based on Snowflake on-demand pricing. Adjust based on your actual contract pricing.
        
        ### ROI Calculation
        - **ROI** = (Total Savings / Total Cost) × 100
        - Higher reuse rate = Higher ROI
        """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        warehouse_size = st.selectbox(
            "Warehouse Size",
            options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"],
            index=2,
            help="Select your typical warehouse size"
        )
    
    with col2:
        # Default costs per hour (Snowflake on-demand)
        default_costs = {
            "X-Small": 1.0,
            "Small": 2.0,
            "Medium": 4.0,
            "Large": 8.0,
            "X-Large": 16.0,
            "2X-Large": 32.0,
            "3X-Large": 64.0,
            "4X-Large": 128.0
        }
        
        cost_per_hour = st.number_input(
            "Cost per Hour ($)",
            min_value=0.0,
            value=float(default_costs[warehouse_size]),
            step=0.1,
            help="Cost per compute hour for your warehouse"
        )
    
    with col3:
        currency = st.selectbox(
            "Currency",
            options=["USD", "EUR", "GBP", "CAD", "AUD"],
            help="Display currency"
        )
    
    # Job source selection
    st.subheader("📋 Analysis Scope")
    
    col1, col2 = st.columns(2)
    
    with col1:
        job_source = st.selectbox(
            "Job Source",
            options=["All Jobs in Environment", "Specific Job ID"],
            help="Analyze all jobs or filter to one",
            key="cost_job_source"
        )
    
    with col2:
        job_types_filter = st.multiselect(
            "Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter by job type",
            key="cost_job_types"
        )
    
    col3, col4 = st.columns(2)
    
    with col3:
        if job_source == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="Specific dbt Cloud job ID", key="cost_job_id")
        else:
            job_id_input = None
            run_status_filter = st.multiselect(
                "Run Status",
                options=["Success", "Error", "Cancelled"],
                default=["Success"],
                help="Filter runs by completion status",
                key="cost_run_status"
            )
    
    with col4:
        # SAO filter option
        sao_only = st.checkbox(
            "SAO Jobs Only",
            value=True,
            help="Only analyze jobs with State-Aware Orchestration enabled (recommended for accurate cost/reuse metrics)",
            key="cost_sao_only"
        )
        if job_source == "Specific Job ID":
            run_status_filter = st.multiselect(
                "Run Status",
                options=["Success", "Error", "Cancelled"],
                default=["Success"],
                help="Filter runs by completion status",
                key="cost_run_status2"
            )
    
    # Include deleted jobs option
    include_deleted_jobs = st.checkbox(
        "Include Deleted/Archived Jobs",
        value=True,  # Default to True for cost analysis - we want to capture all costs
        help="Include runs from jobs that have been deleted or archived. Essential for accurate historical cost analysis!",
        key="cost_include_deleted"
    )
    
    if include_deleted_jobs:
        st.info("💡 **Historical Mode**: Fetching ALL runs by date range. This captures costs from deleted/archived jobs!")
    
    # Date range for analysis
    st.subheader("📅 Analysis Period")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        default_start = datetime.now() - timedelta(days=30)
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of analysis period"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            help="End of analysis period"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs",
            min_value=10,
            max_value=100,
            value=50,
            help="Number of runs to analyze"
        )
    
    analyze_button = st.button("💰 Calculate Costs & ROI", type="primary", key="cost_analyze")
    
    if not analyze_button:
        st.info("⬆️ Configure cost parameters and job source, then click 'Calculate Costs & ROI' to begin")
        
        # Show example output
        with st.expander("📊 What You'll See"):
            st.markdown("""
            ### Key Metrics
            - **Total Cost**: Total compute cost for the period
            - **Cost Savings**: Money saved from model reuse
            - **ROI**: Return on investment from freshness configs
            - **Cost per Run**: Average cost per job execution
            
            ### Visualizations
            - Cost trend over time
            - Cost by model (top 20 most expensive)
            - Savings from reuse over time
            - Cost breakdown by status (success vs reused)
            
            ### Detailed Analysis
            - Per-run cost breakdown
            - Model-level cost analysis
            - Reuse rate impact on costs
            """)
        return
    
    # Validation
    if job_source == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID when using 'Specific Job ID' mode")
        return
    elif job_source == "All Jobs in Environment" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab to use 'All Jobs in Environment' mode")
        return
    
    # Convert status filter to status codes
    status_codes = []
    if "Success" in run_status_filter:
        status_codes.append(10)
    if "Error" in run_status_filter:
        status_codes.append(20)
    if "Cancelled" in run_status_filter:
        status_codes.append(30)
    
    if not status_codes:
        st.error("❌ Please select at least one run status to filter by")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text(f"🔄 Fetching runs from {start_date} to {end_date}...")
        
        # Fetch runs based on job source
        runs = []
        all_jobs_dict = {}  # For job metadata enrichment
        
        if job_source == "All Jobs in Environment":
            
            if include_deleted_jobs:
                # HISTORICAL MODE: Fetch ALL runs by date first (includes deleted/archived jobs)
                status_text.text(f"🔄 Fetching ALL runs from {start_date} to {end_date} (including deleted jobs)...")
                
                def update_progress(msg, count):
                    status_text.text(f"🔄 {msg}")
                
                runs = get_all_runs_by_date(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    start_datetime,
                    end_datetime,
                    environment_id=config.get('environment_id'),
                    status=status_codes,
                    limit=max_runs * 3,  # Fetch extra to allow for filtering
                    progress_callback=update_progress
                )
                
                progress_bar.progress(5)
                
                # Get all jobs (including deleted ones) for metadata
                status_text.text(f"🔄 Fetching job metadata (including deleted jobs)...")
                all_jobs_dict = get_all_jobs_with_metadata(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    progress_callback=update_progress
                )
                
                progress_bar.progress(8)
                
                # Enrich runs with job metadata
                for run in runs:
                    job_def_id = run.get('job_definition_id')
                    if job_def_id and job_def_id in all_jobs_dict:
                        job_info = all_jobs_dict[job_def_id]
                        run['job_name'] = job_info['name']
                        run['job_is_active'] = job_info['is_active']
                        run['job_triggers'] = job_info['triggers']
                    else:
                        run['job_name'] = f'Job {job_def_id} (Deleted)'
                        run['job_is_active'] = False
                        run['job_triggers'] = {}
                
                # Filter by job type if specified
                if job_types_filter:
                    filtered_runs = []
                    for run in runs:
                        job_type = determine_job_type(run.get('job_triggers', {}))
                        if job_type in job_types_filter:
                            filtered_runs.append(run)
                    
                    status_text.text(f"✅ Filtered to {len(filtered_runs)} runs matching job types: {', '.join(job_types_filter)}")
                    runs = filtered_runs
                
                progress_bar.progress(10)
                
            else:
                # STANDARD MODE: Fetch active jobs first, then get their runs
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                
                # Paginate through all active jobs
                jobs = []
                offset = 0
                while True:
                    params = {'environment_id': config['environment_id'], 'limit': 100, 'offset': offset}
                    jobs_response = requests.get(jobs_url, headers=headers, params=params)
                    jobs_response.raise_for_status()
                    page_jobs = jobs_response.json().get('data', [])
                    if not page_jobs:
                        break
                    jobs.extend(page_jobs)
                    if len(page_jobs) < 100:
                        break
                    offset += 100
                
                # Filter by job type
                jobs = filter_jobs_by_type(jobs, job_types_filter)
                
                if not jobs:
                    st.error(f"❌ No active jobs found matching job types: {', '.join(job_types_filter)}. Try enabling 'Include Deleted/Archived Jobs' for historical analysis!")
                    progress_bar.empty()
                    status_text.empty()
                    return
                
                status_text.text(f"🔄 Found {len(jobs)} active jobs. Fetching runs...")
                
                # Get runs from all filtered jobs
                all_runs = []
                for idx, job in enumerate(jobs):
                    job_runs = get_job_runs(
                        config['api_base'],
                        config['api_key'],
                        config['account_id'],
                        str(job['id']),
                        config.get('environment_id'),
                        limit=max_runs,
                        status=status_codes
                    )
                    all_runs.extend(job_runs)
                    
                    progress = int((idx + 1) / len(jobs) * 10)
                    progress_bar.progress(progress)
                
                # Sort by date and limit
                all_runs = sorted(all_runs, key=lambda x: x.get('created_at', ''), reverse=True)[:max_runs]
                runs = all_runs
            
        else:  # Specific Job ID
            runs = get_job_runs(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                job_id_input,
                config.get('environment_id'),
                limit=max_runs,
                status=status_codes
            )
        
        # Filter by date
        if start_datetime or end_datetime:
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if start_datetime and created_at < start_datetime:
                            continue
                        if end_datetime and created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
        
        if not runs:
            st.warning("No runs found in the specified date range")
            progress_bar.empty()
            status_text.empty()
            return
        
        # Filter by SAO if requested
        original_run_count = len(runs)
        if sao_only:
            status_text.text(f"🔍 Filtering to SAO-enabled jobs only...")
            sao_runs, non_sao_runs = filter_runs_by_sao(runs)
            runs = sao_runs
            
            if not runs:
                st.warning(f"⚠️ No runs found from SAO-enabled jobs. Found {len(non_sao_runs)} runs from non-SAO jobs.")
                st.info("💡 **Tip**: Uncheck 'SAO Jobs Only' to analyze all jobs, or enable SAO on your jobs for accurate cost/reuse metrics.")
                progress_bar.empty()
                status_text.empty()
                return
            
            if non_sao_runs:
                st.info(f"ℹ️ Filtered to {len(runs)} SAO-enabled runs (excluded {len(non_sao_runs)} non-SAO runs)")
        
        status_text.text(f"✅ Found {len(runs)} runs. Analyzing costs in parallel...")
        progress_bar.progress(10)
        
        # Analyze runs in parallel
        all_model_statuses = []
        completed_runs = 0
        failed_runs = []
        max_workers = min(10, len(runs))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_run = {
                executor.submit(
                    process_single_run,
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    run.get('id'),
                    run.get('created_at'),
                    run.get('job_definition_id'),
                    run.get('job', {}).get('name') if run.get('job') else None,
                    run.get('status'),
                    run.get('project_id'),
                    run.get('environment_id')
                ): run.get('id') for run in runs
            }
            
            for future in as_completed(future_to_run):
                run_id = future_to_run[future]
                completed_runs += 1
                
                progress = 10 + int((completed_runs / len(runs)) * 80)
                progress_bar.progress(progress)
                status_text.text(f"🔄 Processed {completed_runs}/{len(runs)} runs...")
                
                try:
                    result = future.result()
                    if result['success']:
                        all_model_statuses.extend(result['models'])
                    else:
                        failed_runs.append((run_id, result.get('error', 'Unknown error')))
                except Exception as e:
                    failed_runs.append((run_id, str(e)))
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Show warnings for failed runs
        if failed_runs:
            with st.expander(f"⚠️ {len(failed_runs)} run(s) failed to process", expanded=False):
                for run_id, error in failed_runs:
                    st.caption(f"Run {run_id}: {error}")
        
        # Convert to dataframe
        if all_model_statuses:
            df = pd.DataFrame(all_model_statuses)
        else:
            df = pd.DataFrame()
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        if df.empty:
            st.warning("No cost data found for the specified date range")
            return
        
        # Calculate average execution time per model when it actually runs (status = success)
        success_times = df[df['status'] == 'success'].groupby('unique_id')['execution_time'].mean().to_dict()
        
        # For each row, get the expected execution time
        # If success: use actual time
        # If reused: use average success time for that model
        df['expected_execution_time'] = df.apply(
            lambda row: row['execution_time'] if row['status'] == 'success' 
            else success_times.get(row['unique_id'], row['execution_time']),
            axis=1
        )
        
        # Calculate costs
        df['execution_time_hours'] = df['execution_time'] / 3600
        df['expected_time_hours'] = df['expected_execution_time'] / 3600
        
        # Actual cost (only for models that ran)
        df['cost'] = df.apply(
            lambda row: row['execution_time_hours'] * cost_per_hour if row['status'] == 'success'
            else 0,
            axis=1
        )
        
        # What the cost would have been if model ran (using expected time for reused models)
        df['cost_if_not_reused'] = df['expected_time_hours'] * cost_per_hour
        
        # Savings = what we would have paid - what we actually paid
        df['savings'] = df['cost_if_not_reused'] - df['cost']
        
        # Convert run_created_at to datetime
        df['run_created_at'] = pd.to_datetime(df['run_created_at'])
        
        st.success(f"✅ Analyzed {len(df):,} model executions across {df['run_id'].nunique()} runs")
        
        # KEY METRICS
        st.divider()
        st.subheader("💰 Key Financial Metrics")
        
        total_cost = df['cost'].sum()
        total_savings = df['savings'].sum()
        total_cost_if_not_reused = df['cost_if_not_reused'].sum()
        avg_cost_per_run = total_cost / df['run_id'].nunique() if df['run_id'].nunique() > 0 else 0
        roi = (total_savings / total_cost * 100) if total_cost > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Compute Cost",
                f"{currency} ${total_cost:,.2f}",
                help="Actual cost paid for compute"
            )
        
        with col2:
            st.metric(
                "Cost Savings from Reuse",
                f"{currency} ${total_savings:,.2f}",
                delta=f"{(total_savings/total_cost_if_not_reused*100):.1f}% saved",
                help="Money saved by reusing cached models"
            )
        
        with col3:
            st.metric(
                "Avg Cost per Run",
                f"{currency} ${avg_cost_per_run:,.2f}",
                help="Average cost per job execution"
            )
        
        with col4:
            st.metric(
                "ROI from Reuse",
                f"{roi:.1f}%",
                help="Return on investment from model reuse"
            )
        
        # Cost without reuse
        st.info(f"💡 **Without reuse, you would have spent**: {currency} ${total_cost_if_not_reused:,.2f} (saved {currency} ${total_savings:,.2f}!)")
        
        # COST TREND OVER TIME
        st.divider()
        st.subheader("📈 Cost Trends Over Time")
        
        # Aggregate by run
        run_costs = df.groupby(['run_id', 'run_created_at']).agg({
            'cost': 'sum',
            'savings': 'sum',
            'cost_if_not_reused': 'sum'
        }).reset_index()
        
        run_costs = run_costs.sort_values('run_created_at')
        
        # Create figure with dual axis
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['cost'],
            name='Actual Cost',
            mode='lines+markers',
            line=dict(color='red', width=2),
            fill='tozeroy'
        ))
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['cost_if_not_reused'],
            name='Cost Without Reuse',
            mode='lines',
            line=dict(color='lightgray', width=2, dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['savings'],
            name='Savings',
            mode='lines+markers',
            line=dict(color='green', width=2),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title=f"Cost Trends: {warehouse_size} Warehouse @ ${cost_per_hour}/hour",
            xaxis_title="Date",
            yaxis_title=f"Cost ({currency})",
            yaxis2=dict(
                title=f"Savings ({currency})",
                overlaying='y',
                side='right'
            ),
            hovermode='x unified',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        plotly_chart_no_warnings(fig, use_container_width=True)
        
        # TOP 20 MOST EXPENSIVE MODELS
        st.divider()
        st.subheader("💸 Most Expensive Models")
        
        model_costs = df.groupby('unique_id').agg({
            'cost': 'sum',
            'execution_time': 'sum',
            'status': 'count'
        }).reset_index()
        
        model_costs.columns = ['Model', 'Total Cost', 'Total Time (s)', 'Executions']
        model_costs = model_costs.sort_values('Total Cost', ascending=False).head(20)
        
        fig_top = px.bar(
            model_costs,
            x='Total Cost',
            y='Model',
            orientation='h',
            title='Top 20 Most Expensive Models',
            labels={'Total Cost': f'Total Cost ({currency})', 'Model': 'Model'},
            color='Total Cost',
            color_continuous_scale='Reds'
        )
        
        fig_top.update_layout(height=600, showlegend=False)
        plotly_chart_no_warnings(fig_top, use_container_width=True)
        
        # DETAILED RUN BREAKDOWN
        st.divider()
        st.subheader("📋 Detailed Run Breakdown")
        
        run_summary = df.groupby(['run_id', 'run_created_at']).agg({
            'cost': 'sum',
            'savings': 'sum',
            'cost_if_not_reused': 'sum',
            'unique_id': 'count'
        }).reset_index()
        
        run_summary.columns = ['Run ID', 'Date', 'Actual Cost', 'Savings', 'Cost Without Reuse', 'Models']
        run_summary['Date'] = pd.to_datetime(run_summary['Date']).dt.strftime('%Y-%m-%d %H:%M')
        run_summary['Reuse %'] = ((run_summary['Savings'] / run_summary['Cost Without Reuse']) * 100).round(1)
        
        # Format currency columns
        for col in ['Actual Cost', 'Savings', 'Cost Without Reuse']:
            run_summary[col] = run_summary[col].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(
            run_summary,
            width='stretch',
            hide_index=True
        )
        
        # MODEL-LEVEL COST ANALYSIS
        st.divider()
        st.subheader("🔍 Model-Level Cost Analysis")
        
        model_detail = df.groupby('unique_id').agg({
            'cost': 'sum',
            'savings': 'sum',
            'execution_time': ['sum', 'mean', 'count'],
            'status': lambda x: x.value_counts().to_dict()
        }).reset_index()
        
        model_detail.columns = ['Model', 'Total Cost', 'Total Savings', 'Total Time (s)', 'Avg Time (s)', 'Executions', 'Status Breakdown']
        model_detail = model_detail.sort_values('Total Cost', ascending=False)
        
        # Format
        model_detail['Total Cost'] = model_detail['Total Cost'].apply(lambda x: f"${x:,.2f}")
        model_detail['Total Savings'] = model_detail['Total Savings'].apply(lambda x: f"${x:,.2f}")
        model_detail['Total Time (s)'] = model_detail['Total Time (s)'].round(2)
        model_detail['Avg Time (s)'] = model_detail['Avg Time (s)'].round(2)
        
        st.dataframe(
            model_detail,
            width='stretch',
            hide_index=True
        )
        
        # DOWNLOAD OPTION
        st.divider()
        st.subheader("💾 Download Cost Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Download run summary
            csv_runs = run_summary.to_csv(index=False)
            st.download_button(
                label="📥 Download Run Summary CSV",
                data=csv_runs,
                file_name=f"cost_analysis_runs_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Download model details
            csv_models = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Full Model Data CSV",
                data=csv_models,
                file_name=f"cost_analysis_models_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def show_pre_sao_waste_analysis():
    """Show Pre-SAO Waste Analysis - identify wasted compute before SAO implementation."""
    st.header("🗑️ Pre-SAO Waste Analysis")
    st.markdown("Identify wasted compute spend from models that ran but produced minimal or no changes")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Info expander
    with st.expander("💡 What is Pre-SAO Waste?", expanded=False):
        st.markdown("""
        ### The Problem
        Before State-Aware Orchestration (SAO), models run even when:
        - **No upstream data changed** (incremental models with 0 new rows)
        - **No changes in the result** (table models rebuilding identical data)
        - **Views** (these don't cost anything but show execution in logs)
        
        ### What We Analyze
        This analysis identifies:
        1. **View models**: Excluded from cost calculations (they don't drive warehouse costs)
        2. **Zero-change table models**: Full table refreshes with 0 rows changed
        3. **Zero/low-change incrementals**: Incremental models with 0 or very few new rows
        
        ### How We Detect This
        We parse `run_results.json` from each run to extract:
        - `adapter_response.rows_affected`: Number of rows changed/inserted
        - `materialization`: Model type (view, table, incremental, etc.)
        - `execution_time`: Time spent on execution
        
        ### Example
        ```
        Model: fct_orders (incremental)
        - Run 1: 53 rows inserted → Useful work
        - Run 2: 0 rows inserted → WASTE (should have been skipped)
        - Run 3: 2 rows inserted → Minimal change (could argue for skip)
        ```
        
        ### Impact with SAO
        With SAO enabled, these models would be automatically skipped, saving compute costs!
        
        ### How This Analysis Works
        1. **Fetches runs by date range**: Gets ALL successful runs in your specified date range
        2. **Includes historical jobs**: Captures data from jobs that no longer exist (deleted/archived)
        3. **Enriches with job metadata**: Shows job names and whether they're still active
        4. **Analyzes row changes**: Uses `adapter_response.rows_affected` for accurate waste detection
        
        ### Important Notes
        - **Historical data included**: This analysis captures runs from deleted/archived jobs
        - **Status Filter**: Only successful runs are analyzed (failed runs don't produce meaningful waste metrics)
        - **Job metadata**: Shows which jobs are still active vs. historical/deleted
        """)
    
    st.divider()
    
    # Configuration section
    st.subheader("⚙️ Analysis Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        warehouse_size = st.selectbox(
            "Warehouse Size",
            options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"],
            index=2,
            help="Select your typical warehouse size",
            key="waste_warehouse"
        )
    
    with col2:
        default_costs = {
            "X-Small": 1.0,
            "Small": 2.0,
            "Medium": 4.0,
            "Large": 8.0,
            "X-Large": 16.0,
            "2X-Large": 32.0,
            "3X-Large": 64.0,
            "4X-Large": 128.0
        }
        
        cost_per_hour = st.number_input(
            "Cost per Hour ($)",
            min_value=0.0,
            value=float(default_costs[warehouse_size]),
            step=0.1,
            help="Cost per compute hour for your warehouse",
            key="waste_cost"
        )
    
    with col3:
        min_rows_threshold = st.number_input(
            "Min Rows Threshold",
            min_value=0,
            value=10,
            step=1,
            help="Incremental models with fewer rows than this are considered 'low change'",
            key="waste_threshold"
        )
    
    col4, col5 = st.columns(2)
    
    with col4:
        num_threads = st.number_input(
            "Number of Threads",
            min_value=1,
            value=16,
            step=1,
            help="Number of parallel threads your warehouse uses (for calculating warehouse hours). Snowflake default is typically 8-16.",
            key="waste_threads"
        )
    
    with col5:
        credits_per_hour = st.number_input(
            "Credits per Hour",
            min_value=0.0,
            value=2.0,
            step=0.1,
            help="Snowflake credits consumed per hour for your warehouse size",
            key="waste_credits"
        )
    
    col6 = st.columns(1)[0]
    with col6:
        source_staleness_hours = st.number_input(
            "Source Staleness Threshold (hours)",
            min_value=1,
            value=24,
            step=1,
            help="Sources older than this are considered 'stale'. Models that ran when ALL sources were stale are pure waste.",
            key="waste_source_staleness"
        )
    
    # Job selection
    st.subheader("📋 Analysis Scope")
    
    col1, col2 = st.columns(2)
    
    with col1:
        job_source = st.selectbox(
            "Job Source",
            options=["All Jobs in Environment", "Specific Job ID"],
            help="Analyze all jobs or filter to one",
            key="waste_job_source"
        )
    
    with col2:
        if job_source == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="Specific dbt Cloud job ID", key="waste_job_id")
        else:
            job_id_input = None
            job_types_filter = st.multiselect(
                "Job Types",
                options=["ci", "merge", "scheduled", "other"],
                default=["ci", "merge", "scheduled", "other"],
                help="Filter by job type",
                key="waste_job_types"
            )
    
    # Additional options
    include_deleted_jobs = st.checkbox(
        "Include Deleted/Archived Jobs",
        value=True,  # Default to True for waste analysis since we often look at historical data
        help="Include runs from jobs that have been deleted or archived. Essential for historical analysis!",
        key="waste_include_deleted"
    )
    
    if include_deleted_jobs:
        st.info("💡 **Historical Mode**: Fetching runs by date range first. This captures runs from deleted/archived jobs!")
    
    # Date range
    st.subheader("📅 Analysis Period")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        default_start = datetime.now() - timedelta(days=30)
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of analysis period",
            key="waste_start_date"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            help="End of analysis period",
            key="waste_end_date"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs to Analyze",
            min_value=10,
            max_value=1000,
            value=200,
            step=50,
            help="Maximum total runs to fetch in the date range. Increase if you have many high-frequency jobs. Higher values = more data but slower.",
            key="waste_max_runs"
        )
    
    analyze_button = st.button("🔍 Analyze Waste", type="primary", key="waste_analyze")
    
    if not analyze_button:
        st.info("⬆️ Configure parameters and click 'Analyze Waste' to begin")
        
        with st.expander("📊 What You'll See"):
            st.markdown("""
            ### Key Metrics
            - **Total Wasted Cost**: Money spent on zero/low-change model runs
            - **Wasted Runs**: Count of model executions that produced no meaningful changes
            - **Potential SAO Savings**: Estimated savings if SAO was enabled
            
            ### Visualizations
            - Waste over time (cost and count)
            - Top wasters (which models waste most)
            - Waste by materialization type
            - Detailed model-level breakdown
            
            ### Actionable Insights
            - Which models should have SAO enabled first
            - Which jobs have the most waste
            - ROI estimate for SAO implementation
            """)
        return
    
    # Validation
    if job_source == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID when using 'Specific Job ID' mode")
        return
    elif job_source == "All Jobs in Environment" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab to use 'All Jobs in Environment' mode")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # NEW APPROACH: Fetch runs by date first, then enrich with job metadata
        progress_bar.progress(5)
        
        def update_progress(msg, count):
            """Helper to update progress during API calls"""
            status_text.text(f"🔄 {msg}")
        
        if job_source == "All Jobs in Environment":
            
            if include_deleted_jobs:
                # HISTORICAL MODE: Fetch ALL runs in the date range (includes historical/deleted jobs)
                update_progress(f"Fetching ALL runs from {start_date} to {end_date} (including deleted jobs)...", 0)
                
                runs = get_all_runs_by_date(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    start_datetime,
                    end_datetime,
                    environment_id=config.get('environment_id'),
                    status=[10],  # Success only for waste analysis
                    limit=max_runs,  # Use slider value directly - all runs have waste data
                    progress_callback=update_progress
                )
                
                status_text.text(f"✅ Found {len(runs)} successful runs in date range")
                
                # Warn if we hit the limit - there might be more runs beyond this
                if len(runs) >= max_runs:
                    st.warning(f"⚠️ Reached the maximum of {max_runs} runs. There may be more runs in this date range. Increase the 'Max Runs to Analyze' slider if needed.")
                
                progress_bar.progress(10)
                
                # Get all jobs (including inactive/deleted ones)
                all_jobs_dict = get_all_jobs_with_metadata(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    progress_callback=update_progress
                )
                
                status_text.text(f"✅ Found {len(all_jobs_dict)} total jobs (active + inactive)")
                progress_bar.progress(15)
                
                # Enrich runs with job metadata
                for run in runs:
                    job_def_id = run.get('job_definition_id')
                    if job_def_id and job_def_id in all_jobs_dict:
                        job_info = all_jobs_dict[job_def_id]
                        run['job_name'] = job_info['name']
                        run['job_is_active'] = job_info['is_active']
                        run['job_triggers'] = job_info['triggers']
                    else:
                        # Job not found (might be deleted and not in API response)
                        run['job_name'] = f'Job {job_def_id} (Deleted)'
                        run['job_is_active'] = False
                        run['job_triggers'] = {}
                
                # Filter by job type if specified
                if job_types_filter:
                    filtered_runs = []
                    for run in runs:
                        job_type = determine_job_type(run.get('job_triggers', {}))
                        if job_type in job_types_filter:
                            filtered_runs.append(run)
                    
                    status_text.text(f"✅ Filtered to {len(filtered_runs)} runs matching job types: {', '.join(job_types_filter)}")
                    runs = filtered_runs
            
            else:
                # STANDARD MODE: Fetch active jobs first, then get their runs
                update_progress(f"Fetching active jobs in environment...", 0)
                
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                params = {'environment_id': config['environment_id'], 'limit': 100}
                
                jobs_response = requests.get(jobs_url, headers=headers, params=params)
                jobs_response.raise_for_status()
                jobs = jobs_response.json().get('data', [])
                
                # Filter by job type
                if job_types_filter:
                    jobs = filter_jobs_by_type(jobs, job_types_filter)
                
                if not jobs:
                    st.error(f"❌ No active jobs found matching job types: {', '.join(job_types_filter)}. Try enabling 'Include Deleted/Archived Jobs' for historical analysis!")
                    progress_bar.empty()
                    status_text.empty()
                    return
                
                status_text.text(f"🔄 Found {len(jobs)} active jobs. Fetching runs...")
                progress_bar.progress(10)
                
                # Get runs from all filtered jobs
                runs = []
                for idx, job in enumerate(jobs):
                    job_runs = get_job_runs(
                        config['api_base'],
                        config['api_key'],
                        config['account_id'],
                        str(job['id']),
                        config.get('environment_id'),
                        limit=max_runs,
                        status=[10]  # Success only
                    )
                    # Enrich with job info
                    for run in job_runs:
                        run['job_name'] = job.get('name', 'Unknown')
                        run['job_is_active'] = True
                        run['job_triggers'] = job.get('triggers', {})
                    runs.extend(job_runs)
                    
                    progress = 10 + int((idx + 1) / len(jobs) * 5)
                    progress_bar.progress(progress)
                
                # Filter by date
                filtered_runs = []
                for run in runs:
                    created_at_str = run.get('created_at')
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                            created_at = created_at.replace(tzinfo=None)
                            if start_datetime <= created_at <= end_datetime:
                                filtered_runs.append(run)
                        except:
                            pass
                
                runs = filtered_runs
                status_text.text(f"✅ Found {len(runs)} runs in date range")
                progress_bar.progress(15)
        else:
            # Specific job ID mode
            runs = get_job_runs(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                job_id_input,
                config.get('environment_id'),
                limit=max_runs,
                status=[10]
            )
            
            # Filter by date
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if created_at < start_datetime or created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
            
            # Enrich with job metadata
            all_jobs_dict = get_all_jobs_with_metadata(
                config['api_base'],
                config['api_key'],
                config['account_id']
            )
            
            for run in runs:
                job_def_id = run.get('job_definition_id')
                if job_def_id and job_def_id in all_jobs_dict:
                    job_info = all_jobs_dict[job_def_id]
                    run['job_name'] = job_info['name']
                    run['job_is_active'] = job_info['is_active']
                else:
                    run['job_name'] = f'Job {job_def_id}'
                    run['job_is_active'] = False
        
        if not runs:
            progress_bar.empty()
            status_text.empty()
            
            st.warning(f"⚠️ No successful runs found in the specified date range ({start_date} to {end_date})")
            
            with st.expander("🔍 Diagnostic Information", expanded=True):
                if job_source == "All Jobs in Environment":
                    active_count = len([j for j in all_jobs_dict.values() if j['is_active']])
                    inactive_count = len(all_jobs_dict) - active_count
                    
                    st.markdown(f"""
                    **What was analyzed:**
                    - Environment ID: {config.get('environment_id')}
                    - Total jobs (active + inactive): {len(all_jobs_dict)} ({active_count} active, {inactive_count} inactive)
                    - Job types filter: {', '.join(job_types_filter) if job_types_filter else 'All types'}
                    - Date range: {start_date} to {end_date}
                    - Successful runs found: {len(runs)}
                    
                    **Possible reasons:**
                    1. **No successful runs in this date range** - Try expanding the date range
                    2. **Job type filter too restrictive** - Try selecting all job types
                    3. **Environment had no activity** - Verify the environment was active during this period
                    
                    **Suggestions:**
                    - ✅ Try selecting all job types (CI, Merge, Scheduled, Other)
                    - ✅ Expand the date range (currently analyzing last {(end_date - start_date).days} days)
                    - ✅ Try a different environment ID if you have multiple
                    - ✅ Use "Specific Job ID" mode if you know a job that was running
                    """)
                else:
                    st.markdown(f"""
                    **What was analyzed:**
                    - Job ID: {job_id_input}
                    - Date range: {start_date} to {end_date}
                    - Successful runs found: {len(runs)}
                    
                    **Possible reasons:**
                    1. **No successful runs in this date range** - Try expanding the date range
                    2. **Job didn't run during this period** - Verify the job was active
                    3. **Status filter** - Only successful runs (status=10) are analyzed
                    
                    **Suggestions:**
                    - ✅ Expand the date range
                    - ✅ Verify the job ID is correct
                    - ✅ Check if the job was active during this period
                    """)
            
            return
        
        status_text.text(f"✅ Found {len(runs)} runs. Analyzing for waste...")
        progress_bar.progress(10)
        
        # Analyze runs in parallel
        all_model_data = []
        completed_runs = 0
        failed_runs = []
        max_workers = min(10, len(runs))
        
        # Store run metadata for later enrichment
        run_metadata = {}
        for run in runs:
            run_metadata[run.get('id')] = {
                'job_name': run.get('job_name', 'Unknown'),
                'job_is_active': run.get('job_is_active', True)
            }
        
        # Convert staleness threshold from hours to seconds
        staleness_threshold_seconds = int(source_staleness_hours * 3600)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_run = {
                executor.submit(
                    process_single_run_lightweight,
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    run.get('id'),
                    run.get('created_at'),
                    run.get('job_definition_id'),
                    run.get('job_name', run.get('job', {}).get('name') if run.get('job') else None),
                    run.get('status'),
                    staleness_threshold_seconds  # Pass staleness threshold!
                ): run.get('id') for run in runs
            }
            
            for future in as_completed(future_to_run):
                run_id = future_to_run[future]
                completed_runs += 1
                
                progress = 10 + int((completed_runs / len(runs)) * 80)
                progress_bar.progress(progress)
                status_text.text(f"🔄 Processed {completed_runs}/{len(runs)} runs...")
                
                try:
                    result = future.result()
                    if result['success']:
                        all_model_data.extend(result['models'])
                    else:
                        failed_runs.append((run_id, result.get('error', 'Unknown error')))
                except Exception as e:
                    failed_runs.append((run_id, str(e)))
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Show warnings
        if failed_runs:
            with st.expander(f"⚠️ {len(failed_runs)} run(s) failed to process", expanded=False):
                for run_id, error in failed_runs:
                    st.caption(f"Run {run_id}: {error}")
        
        # Convert to dataframe
        if all_model_data:
            df = pd.DataFrame(all_model_data)
        else:
            df = pd.DataFrame()
        
        progress_bar.empty()
        status_text.empty()
        
        # Show processing summary
        runs_with_data = len(set(model['run_id'] for model in all_model_data)) if all_model_data else 0
        
        with st.expander("📊 Processing Summary", expanded=(df.empty)):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Runs Fetched", len(runs))
            with col2:
                st.metric("Runs with Model Data", runs_with_data)
            with col3:
                st.metric("Models Found", len(df) if not df.empty else 0)
            
            if runs_with_data == 0 and len(runs) > 0:
                st.warning("⚠️ **None of the runs had model execution data!**")
                st.markdown("""
                This is highly unusual and suggests:
                - The runs don't have `dbt run` or `dbt build` command steps
                - Step names don't match our pattern (check terminal output for step names)
                - All runs only had `dbt compile`, `dbt test`, or `dbt source freshness` steps
                
                **To debug:**
                1. Check your terminal/console for DEBUG output showing actual step names
                2. Pick one run ID from your date range and verify it in dbt Cloud UI
                3. Look for steps like "Invoke dbt build" or "Invoke dbt with \`dbt run\`"
                """)
        
        if df.empty:
            return
        
        # Process the data for waste analysis
        st.success(f"✅ Analyzed {len(df):,} model executions across {df['run_id'].nunique()} runs")
        
        # Show job statistics if we analyzed multiple jobs
        if job_source == "All Jobs in Environment" and all_jobs_dict:
            with st.expander("📊 Job Statistics", expanded=False):
                unique_job_ids = df['job_id'].unique()
                jobs_with_data = len(unique_job_ids)
                active_jobs_with_data = sum(1 for jid in unique_job_ids if all_jobs_dict.get(jid, {}).get('is_active', False))
                inactive_jobs_with_data = jobs_with_data - active_jobs_with_data
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Jobs Analyzed", jobs_with_data)
                with col2:
                    st.metric("Active Jobs", active_jobs_with_data, delta="Currently running")
                with col3:
                    st.metric("Inactive/Deleted Jobs", inactive_jobs_with_data, delta="Historical data")
                
                if inactive_jobs_with_data > 0:
                    st.info(f"💡 **Historical data included**: {inactive_jobs_with_data} job(s) no longer exist but had runs in this period")
                    
                    # Show which jobs are inactive
                    inactive_jobs = []
                    for jid in unique_job_ids:
                        job_info = all_jobs_dict.get(jid)
                        if job_info and not job_info.get('is_active', True):
                            run_count = len(df[df['job_id'] == jid])
                            inactive_jobs.append({
                                'Job ID': jid,
                                'Job Name': job_info['name'],
                                'Runs in Period': run_count
                            })
                    
                    if inactive_jobs:
                        st.markdown("**Inactive/Deleted Jobs:**")
                        inactive_df = pd.DataFrame(inactive_jobs)
                        st.dataframe(inactive_df, width="stretch", hide_index=True)
        
        # Convert run_created_at to datetime
        df['run_created_at'] = pd.to_datetime(df['run_created_at'])
        
        # Calculate execution cost
        df['execution_time_hours'] = df['execution_time'] / 3600
        df['cost'] = df['execution_time_hours'] * cost_per_hour
        
        # ============================================================
        # SOURCE FRESHNESS-BASED WASTE CLASSIFICATION (PRIMARY ROI)
        # ============================================================
        st.divider()
        st.header("🎯 Source Freshness-Based Waste Classification")
        st.markdown(f"""
        **Pure Waste** = Models that ran when ALL upstream sources were stale (unchanged for >{source_staleness_hours}hrs).  
        This represents the **strongest ROI for SAO** - these executions were 100% avoidable.
        
        **Classification Logic:**
        - 🔴 **Pure Waste**: ALL upstream sources were stale → 100% avoidable with SAO
        - 🟠 **Partial Waste**: Mixed dependencies (some stale, some fresh) → partially avoidable
        - 🟢 **Justified**: At least one source was recently updated → Model correctly ran
        - 🟡 **Unknown**: No source freshness data available (sources.json not found)
        """)
        
        # Calculate classification metrics
        if 'source_freshness_classification' in df.columns:
            classification_counts = df['source_freshness_classification'].value_counts()
            pure_waste_count = classification_counts.get('pure_waste', 0)
            partial_waste_count = classification_counts.get('partial_waste', 0)
            justified_count = classification_counts.get('justified', 0)
            unknown_count = classification_counts.get('unknown', 0)
            
            total = len(df)
            pure_waste_pct = (pure_waste_count / total * 100) if total > 0 else 0
            partial_waste_pct = (partial_waste_count / total * 100) if total > 0 else 0
            justified_pct = (justified_count / total * 100) if total > 0 else 0
            unknown_pct = (unknown_count / total * 100) if total > 0 else 0
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "🔴 Pure Waste", 
                    f"{pure_waste_count:,}", 
                    delta=f"{pure_waste_pct:.1f}%",
                    help="Models that ran when ALL upstream sources were stale (100% avoidable)"
                )
            
            with col2:
                st.metric(
                    "🟠 Partial Waste", 
                    f"{partial_waste_count:,}",
                    delta=f"{partial_waste_pct:.1f}%",
                    help="Models with mixed dependencies (some stale, some fresh)"
                )
            
            with col3:
                st.metric(
                    "🟢 Justified", 
                    f"{justified_count:,}",
                    delta=f"{justified_pct:.1f}%",
                    help="Models that ran with fresh upstream data"
                )
            
            with col4:
                st.metric(
                    "🟡 Unknown", 
                    f"{unknown_count:,}",
                    delta=f"{unknown_pct:.1f}%",
                    help="No source freshness data available"
                )
            
            with col5:
                # Calculate cost of pure + partial waste
                waste_df = df[df['source_freshness_classification'].isin(['pure_waste', 'partial_waste'])]
                total_waste_time_hours = waste_df['execution_time'].sum() / 3600
                total_waste_wh_hours = total_waste_time_hours / num_threads
                total_waste_cost = total_waste_wh_hours * cost_per_hour
                st.metric(
                    "💰 Total Waste Cost", 
                    f"${total_waste_cost:,.2f}",
                    help="Total cost of pure + partial waste"
                )
            
            # Waste classification pie chart
            st.subheader("📊 Classification Breakdown")
            
            classification_df = df.groupby('source_freshness_classification').agg({
                'execution_time': 'sum'
            }).reset_index()
            
            # Calculate cost for each classification
            classification_df['cost'] = (classification_df['execution_time'] / 3600 / num_threads) * cost_per_hour
            
            # Rename for display
            classification_map = {
                'pure_waste': '🔴 Pure Waste (100% avoidable)',
                'partial_waste': '🟠 Partial Waste (mixed dependencies)',
                'justified': '🟢 Justified (needed)',
                'unknown': '🟡 Unknown (no data)'
            }
            classification_df['category'] = classification_df['source_freshness_classification'].map(classification_map)
            
            fig = px.pie(
                classification_df,
                values='cost',
                names='category',
                title='Waste Classification by Source Freshness',
                color='category',
                color_discrete_map={
                    '🔴 Pure Waste (100% avoidable)': '#ff6b6b',
                    '🟠 Partial Waste (mixed dependencies)': '#ff922b',
                    '🟢 Justified (needed)': '#51cf66',
                    '🟡 Unknown (no data)': '#ffd43b'
                }
            )
            
            plotly_chart_no_warnings(fig, use_container_width=True)
            
            # ============================================================
            # BY RUN: Models Executed - Wasted vs Not Wasted Per Run
            # ============================================================
            st.subheader("📈 Models Built: Wasted vs Not Wasted by Run")
            st.caption("Shows which specific runs had waste - each bar represents one run")
            
            # Prepare per-run data
            if 'run_id' in df.columns and 'source_freshness_classification' in df.columns:
                # Group by run_id and classification
                run_df = df.groupby(['run_id', 'source_freshness_classification']).size().reset_index(name='model_count')
                
                # Pivot to get waste vs not waste columns
                run_pivot = run_df.pivot(
                    index='run_id',
                    columns='source_freshness_classification',
                    values='model_count'
                ).fillna(0.0).reset_index()
                
                # Ensure all columns exist
                for col in ['pure_waste', 'partial_waste', 'justified', 'unknown']:
                    if col not in run_pivot.columns:
                        run_pivot[col] = 0.0
                
                # Calculate totals
                run_pivot['wasted'] = run_pivot['pure_waste'] + run_pivot['partial_waste']
                run_pivot['not_wasted'] = run_pivot['justified'] + run_pivot['unknown']
                run_pivot['total'] = run_pivot['wasted'] + run_pivot['not_wasted']
                run_pivot['waste_pct'] = (run_pivot['wasted'] / run_pivot['total'] * 100).round(1)
                
                # Get run metadata (job name, created_at) for better labels
                run_metadata = df.groupby('run_id').agg({
                    'run_created_at': 'first',
                    'job_name': 'first'
                }).reset_index()
                
                # Merge metadata
                run_pivot = run_pivot.merge(run_metadata, on='run_id', how='left')
                
                # Sort by created_at (chronological)
                run_pivot = run_pivot.sort_values('run_created_at')
                
                # Create labels: "Run #1", "Run #2", etc.
                run_pivot['run_label'] = [f"Run #{i+1}" for i in range(len(run_pivot))]
                
                # Create stacked bar chart with 3 segments
                fig_run = go.Figure()
                
                # Add "Not Wasted" bar (green - bottom of stack)
                fig_run.add_trace(go.Bar(
                    name='Not Wasted (Justified + Unknown)',
                    x=run_pivot['run_label'],
                    y=run_pivot['not_wasted'],
                    marker_color='#51cf66',  # Green
                    customdata=run_pivot[['run_id', 'job_name', 'run_created_at']],
                    hovertemplate='<b>%{x}</b><br>' +
                                  'Run ID: %{customdata[0]}<br>' +
                                  'Job: %{customdata[1]}<br>' +
                                  'Created: %{customdata[2]}<br>' +
                                  'Not Wasted: %{y}<extra></extra>'
                ))
                
                # Add "Partial Waste" bar (orange - middle of stack)
                fig_run.add_trace(go.Bar(
                    name='Partial Waste (Mixed)',
                    x=run_pivot['run_label'],
                    y=run_pivot['partial_waste'],
                    marker_color='#ff922b',  # Orange
                    customdata=run_pivot[['run_id', 'job_name', 'run_created_at']],
                    hovertemplate='<b>%{x}</b><br>' +
                                  'Run ID: %{customdata[0]}<br>' +
                                  'Job: %{customdata[1]}<br>' +
                                  'Created: %{customdata[2]}<br>' +
                                  'Partial Waste: %{y}<extra></extra>'
                ))
                
                # Add "Pure Waste" bar (red - top of stack)
                fig_run.add_trace(go.Bar(
                    name='Pure Waste (100% Avoidable)',
                    x=run_pivot['run_label'],
                    y=run_pivot['pure_waste'],
                    marker_color='#ff6b6b',  # Red
                    customdata=run_pivot[['run_id', 'job_name', 'run_created_at']],
                    hovertemplate='<b>%{x}</b><br>' +
                                  'Run ID: %{customdata[0]}<br>' +
                                  'Job: %{customdata[1]}<br>' +
                                  'Created: %{customdata[2]}<br>' +
                                  'Pure Waste: %{y}<extra></extra>'
                ))
                
                fig_run.update_layout(
                    barmode='stack',
                    title=f'Models Built by Run (Total: {len(run_pivot)} runs)',
                    xaxis_title='Run',
                    yaxis_title='Number of Model Executions',
                    hovermode='x unified',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    xaxis={'tickangle': -45}  # Angle labels for readability
                )
                
                plotly_chart_no_warnings(fig_run, use_container_width=True)
                
                # Show summary stats
                total_pure_waste = run_pivot['pure_waste'].sum()
                total_partial_waste = run_pivot['partial_waste'].sum()
                total_wasted = total_pure_waste + total_partial_waste
                total_not_wasted = run_pivot['not_wasted'].sum()
                total_executions = total_wasted + total_not_wasted
                waste_percentage = (total_wasted / total_executions * 100) if total_executions > 0 else 0
                runs_with_waste = len(run_pivot[run_pivot['wasted'] > 0])
                
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total Runs", f"{len(run_pivot):,}")
                with col2:
                    st.metric("🔴 Pure Waste", f"{int(total_pure_waste):,}", help="100% avoidable")
                with col3:
                    st.metric("🟠 Partial Waste", f"{int(total_partial_waste):,}", help="Partially avoidable")
                with col4:
                    st.metric("🟢 Not Wasted", f"{int(total_not_wasted):,}", help="Justified")
                with col5:
                    st.metric("Waste %", f"{waste_percentage:.1f}%", delta=f"{runs_with_waste}/{len(run_pivot)} runs")
            else:
                st.info("Run data not available")
            
            # ============================================================
            # Pure waste details table
            # ============================================================
            st.subheader("🗑️ Pure Waste Model Details")
            
            # Get models that had any waste (pure or partial)
            waste_models_df = df[df['source_freshness_classification'].isin(['pure_waste', 'partial_waste'])].copy() if 'source_freshness_classification' in df.columns else pd.DataFrame()
            
            if not waste_models_df.empty:
                # Group by model and calculate stats
                model_summary = df.groupby('unique_id').agg({
                    'materialization': 'first',
                    'unique_id': 'count'  # Total runs
                }).rename(columns={'unique_id': 'total_runs'}).reset_index()
                
                # Count pure waste runs per model
                pure_waste_counts = df[df['source_freshness_classification'] == 'pure_waste'].groupby('unique_id').size().reset_index(name='pure_waste_runs')
                model_summary = model_summary.merge(pure_waste_counts, on='unique_id', how='left')
                model_summary['pure_waste_runs'] = model_summary['pure_waste_runs'].fillna(0.0).astype(int)
                
                # Count partial waste runs per model
                partial_waste_counts = df[df['source_freshness_classification'] == 'partial_waste'].groupby('unique_id').size().reset_index(name='partial_waste_runs')
                model_summary = model_summary.merge(partial_waste_counts, on='unique_id', how='left')
                model_summary['partial_waste_runs'] = model_summary['partial_waste_runs'].fillna(0.0).astype(int)
                
                # Count justified runs per model
                justified_counts = df[df['source_freshness_classification'] == 'justified'].groupby('unique_id').size().reset_index(name='justified_runs')
                model_summary = model_summary.merge(justified_counts, on='unique_id', how='left')
                model_summary['justified_runs'] = model_summary['justified_runs'].fillna(0.0).astype(int)
                
                # Count unknown runs per model
                unknown_counts = df[df['source_freshness_classification'] == 'unknown'].groupby('unique_id').size().reset_index(name='unknown_runs')
                model_summary = model_summary.merge(unknown_counts, on='unique_id', how='left')
                model_summary['unknown_runs'] = model_summary['unknown_runs'].fillna(0.0).astype(int)
                
                # Calculate total wasted time (pure + partial waste only)
                waste_time = df[df['source_freshness_classification'].isin(['pure_waste', 'partial_waste'])].groupby('unique_id')['execution_time'].sum().reset_index()
                model_summary = model_summary.merge(waste_time, on='unique_id', how='left')
                model_summary['execution_time'] = model_summary['execution_time'].fillna(0.0)
                
                # Calculate cost
                model_summary['cost'] = (model_summary['execution_time'] / 3600 / num_threads) * cost_per_hour
                
                # Filter to models with any waste and sort by cost
                model_summary = model_summary[
                    (model_summary['pure_waste_runs'] > 0) | (model_summary['partial_waste_runs'] > 0)
                ].sort_values('cost', ascending=False).head(30)
                
                st.markdown(f"**Top 30 Models with Waste** (pure or partial waste)")
                
                st.dataframe(
                    model_summary,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        'unique_id': 'Model',
                        'materialization': 'Type',
                        'total_runs': st.column_config.NumberColumn('Total Runs', format='%d'),
                        'pure_waste_runs': st.column_config.NumberColumn('🔴 Pure Waste', format='%d'),
                        'partial_waste_runs': st.column_config.NumberColumn('🟠 Partial Waste', format='%d'),
                        'justified_runs': st.column_config.NumberColumn('🟢 Justified', format='%d'),
                        'unknown_runs': st.column_config.NumberColumn('🟡 Unknown', format='%d'),
                        'execution_time': st.column_config.NumberColumn('Total Wasted Time (s)', format='%.1f'),
                        'cost': st.column_config.NumberColumn('Wasted Cost ($)', format='$%.2f')
                    }
                )
                
                # ROI Insight
                total_pure_cost = model_summary[model_summary['pure_waste_runs'] > 0]['cost'].sum()
                st.success(f"""
                **SAO Impact:** These models represent \\${model_summary['cost'].sum():,.2f} in total waste 
                (\\${total_pure_cost:,.2f} **pure waste** - 100% eliminable with SAO + source freshness checks).
                """)
            else:
                st.info("No waste detected!")
        else:
            st.info(f"""
            ℹ️ **Source freshness data unavailable**
            
            To enable this analysis, ensure your jobs run `dbt source freshness` commands. 
            This allows us to identify models that ran when their upstream sources were stale (>{source_staleness_hours}hrs old).
            
            **How to enable:**
            1. Add `dbt source freshness` step to your jobs
            2. Configure freshness thresholds in your sources YAML
            3. Re-run this analysis
            """)
        
        # ============================================================
        # ROW-BASED ANALYSIS (SUPPLEMENTARY)
        # ============================================================
        st.divider()
        st.subheader("📦 Row-Based Waste Analysis (Supplementary)")
        st.caption("Based on rows_affected: 'No New Data to Process' (0 rows) vs 'Processed New Data' (>0 rows)")
        
        # ============================================================
        # FRESHNESS STATUS SUMMARY (Matching Colleague's Pivot Table)
        # ============================================================
        st.divider()
        st.subheader("📊 Freshness Status Summary")
        st.caption("This matches the pivot table analysis format: 'No New Data to Process' (0 rows) vs 'Processed New Data' (>0 rows)")
        
        # Exclude views for this analysis (they don't have meaningful row counts)
        analysis_df = df[df['materialization'] != 'view'].copy()
        
        # Classify by Freshness Status (based on rows_affected)
        analysis_df['rows_affected_clean'] = pd.to_numeric(analysis_df['rows_affected'], errors='coerce').fillna(0.0)
        analysis_df['Freshness Status'] = analysis_df['rows_affected_clean'].apply(
            lambda x: 'No New Data to Process' if x == 0 else 'Processed New Data'
        )
        
        # Create summary pivot table
        summary = analysis_df.groupby('Freshness Status').agg({
            'unique_id': 'count',  # Number of model executions
            'execution_time': 'sum',  # Total execution time (seconds)
            'rows_affected_clean': 'sum'  # Total rows processed
        }).reset_index()
        
        summary.columns = ['Freshness Status', 'Number of Model Executions', 'Total Execution Time (s)', 'Total Rows Processed']
        
        # Add percentage
        total_executions = summary['Number of Model Executions'].sum()
        summary['Percent of Executions'] = (summary['Number of Model Executions'] / total_executions * 100).round(2)
        
        # Add time calculations
        summary['Sum in Minutes'] = (summary['Total Execution Time (s)'] / 60).round(2)
        summary['Execution Time Hours'] = (summary['Total Execution Time (s)'] / 3600).round(4)
        summary['Warehouse Hours (÷ threads)'] = (summary['Execution Time Hours'] / num_threads).round(4)
        summary['Credits Used'] = (summary['Warehouse Hours (÷ threads)'] * credits_per_hour).round(2)
        summary['Estimated Cost'] = (summary['Warehouse Hours (÷ threads)'] * cost_per_hour).round(2)
        
        # Reorder columns to match colleague's format
        summary = summary[[
            'Freshness Status',
            'Number of Model Executions',
            'Percent of Executions',
            'Total Execution Time (s)',
            'Sum in Minutes',
            'Execution Time Hours',
            'Warehouse Hours (÷ threads)',
            'Credits Used',
            'Estimated Cost'
        ]]
        
        # Add Grand Total row
        grand_total = pd.DataFrame([{
            'Freshness Status': 'Grand Total',
            'Number of Model Executions': summary['Number of Model Executions'].sum(),
            'Percent of Executions': 100.0,
            'Total Execution Time (s)': summary['Total Execution Time (s)'].sum(),
            'Sum in Minutes': summary['Sum in Minutes'].sum(),
            'Execution Time Hours': summary['Execution Time Hours'].sum(),
            'Warehouse Hours (÷ threads)': summary['Warehouse Hours (÷ threads)'].sum(),
            'Credits Used': summary['Credits Used'].sum(),
            'Estimated Cost': summary['Estimated Cost'].sum()
        }])
        
        summary_with_total = pd.concat([summary, grand_total], ignore_index=True)
        
        # Display the pivot table
        st.dataframe(
            summary_with_total,
            width="stretch",
            hide_index=True,
            column_config={
                'Freshness Status': st.column_config.TextColumn('Freshness Status', width='medium'),
                'Number of Model Executions': st.column_config.NumberColumn('# Model Executions', format='%d'),
                'Percent of Executions': st.column_config.NumberColumn('% of Executions', format='%.2f%%'),
                'Total Execution Time (s)': st.column_config.NumberColumn('Total Time (s)', format='%.2f'),
                'Sum in Minutes': st.column_config.NumberColumn('Time (min)', format='%.2f'),
                'Execution Time Hours': st.column_config.NumberColumn('Time (hours)', format='%.4f'),
                'Warehouse Hours (÷ threads)': st.column_config.NumberColumn(f'WH Hours (÷{num_threads})', format='%.4f'),
                'Credits Used': st.column_config.NumberColumn('Credits', format='%.2f'),
                'Estimated Cost': st.column_config.NumberColumn('Est. Cost ($)', format='$%.2f')
            }
        )
        
        # Key insight callout
        no_data_row = summary[summary['Freshness Status'] == 'No New Data to Process']
        if not no_data_row.empty:
            no_data_pct = no_data_row['Percent of Executions'].values[0]
            no_data_cost = no_data_row['Estimated Cost'].values[0]
            st.warning(f"⚠️ **{no_data_pct:.1f}%** of model executions processed **no new data** - this is ${no_data_cost:,.2f} in potential waste that SAO could eliminate!")
        
        # ============================================================
        # MODEL-LEVEL FRESHNESS BREAKDOWN
        # ============================================================
        with st.expander("📋 Model-Level Freshness Breakdown", expanded=False):
            st.caption("Average execution time and rows processed per model, grouped by Freshness Status")
            
            # Create model-level summary
            model_summary = analysis_df.groupby(['unique_id', 'Freshness Status']).agg({
                'execution_time': 'mean',
                'rows_affected_clean': 'mean'
            }).reset_index()
            
            model_summary.columns = ['Model', 'Freshness Status', 'AVERAGE of Model Execution Time (s)', 'AVERAGE of # of Records Processed']
            
            # Extract model name from unique_id
            model_summary['Model'] = model_summary['Model'].apply(lambda x: x.split('.')[-1] if '.' in str(x) else x)
            
            # Pivot to show both statuses side by side
            model_pivot = model_summary.pivot_table(
                index='Model',
                columns='Freshness Status',
                values=['AVERAGE of Model Execution Time (s)', 'AVERAGE of # of Records Processed'],
                aggfunc='first'
            ).round(2)
            
            # Flatten column names
            model_pivot.columns = [f'{val} - {status}' for val, status in model_pivot.columns]
            model_pivot = model_pivot.reset_index()
            
            st.dataframe(model_pivot, width="stretch", height=400)
        
        # Categorize waste
        df['waste_category'] = 'Not Waste'
        df['is_waste'] = False
        
        # 1. Exclude views (they don't cost anything)
        view_mask = df['materialization'] == 'view'
        df.loc[view_mask, 'waste_category'] = 'View (Excluded)'
        
        # 2. For non-views that executed (status = success)
        executed_mask = (df['status'] == 'success') & (~view_mask)
        
        # 3. Clean rows_affected for analysis
        df['rows_affected_clean'] = pd.to_numeric(df['rows_affected'], errors='coerce').fillna(0.0)
        
        # 4. Analyze patterns PER MODEL to categorize waste intelligently
        for unique_id in df[executed_mask]['unique_id'].unique():
            model_runs = df[(df['unique_id'] == unique_id) & executed_mask]
            
            if len(model_runs) == 0:
                continue
            
            # Count zero and non-zero runs
            zero_runs = (model_runs['rows_affected_clean'] == 0).sum()
            total_runs = len(model_runs)
            zero_pct = zero_runs / total_runs if total_runs > 0 else 0
            
            # Get materialization
            materialization = model_runs['materialization'].iloc[0] if len(model_runs) > 0 else None
            
            # Pattern Detection:
            if zero_runs == total_runs and total_runs >= 2:
                # ALWAYS ZERO: All runs have 0 changes (likely deprecated/unused table)
                df.loc[(df['unique_id'] == unique_id) & executed_mask, 'waste_category'] = 'Always Zero (Deprecated?)'
                df.loc[(df['unique_id'] == unique_id) & executed_mask, 'is_waste'] = True
                
            elif zero_pct >= 0.3 and zero_runs >= 2:
                # SPORADIC ZEROS: ≥30% of runs are zero (e.g., weekends, periodic patterns)
                zero_mask = (df['unique_id'] == unique_id) & executed_mask & (df['rows_affected_clean'] == 0)
                df.loc[zero_mask, 'waste_category'] = 'Sporadic Zeros (Periodic Pattern?)'
                df.loc[zero_mask, 'is_waste'] = True
                
            elif zero_runs > 0:
                # OCCASIONAL ZEROS: Some zero runs but not a pattern
                zero_mask = (df['unique_id'] == unique_id) & executed_mask & (df['rows_affected_clean'] == 0)
                df.loc[zero_mask, 'waste_category'] = 'Occasional Zero Changes'
                df.loc[zero_mask, 'is_waste'] = True
                
            # Check for low-change incrementals (even if they never hit zero)
            if materialization == 'incremental':
                low_change_mask = (df['unique_id'] == unique_id) & executed_mask & \
                                  (df['rows_affected_clean'] > 0) & \
                                  (df['rows_affected_clean'] <= min_rows_threshold)
                
                if low_change_mask.sum() > 0:
                    df.loc[low_change_mask, 'waste_category'] = f'Low Changes (<={min_rows_threshold} rows)'
                    df.loc[low_change_mask, 'is_waste'] = True
        
        # Calculate waste metrics
        waste_df = df[df['is_waste'] == True].copy()
        
        if waste_df.empty:
            st.info("🎉 No waste detected! All models that ran produced meaningful changes.")
            return
        
        # KEY METRICS
        st.divider()
        st.subheader("💸 Waste Metrics")
        
        total_cost = df[~view_mask]['cost'].sum()
        wasted_cost = waste_df['cost'].sum()
        wasted_model_executions = len(waste_df)
        total_model_executions = len(df[~view_mask])
        waste_pct = (wasted_cost / total_cost * 100) if total_cost > 0 else 0
        waste_exec_pct = (wasted_model_executions / total_model_executions * 100) if total_model_executions > 0 else 0
        avg_cost_per_wasted_execution = wasted_cost / wasted_model_executions if wasted_model_executions > 0 else 0
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Wasted Cost",
                f"${wasted_cost:,.2f}",
                delta=f"{waste_pct:.1f}% of total cost",
                help="Money spent on model executions with zero or minimal changes"
            )
        
        with col2:
            st.metric(
                "Wasted Executions",
                f"{wasted_model_executions:,}",
                delta=f"{waste_exec_pct:.1f}% of executions",
                help="Number of model executions that produced minimal/no changes"
            )
        
        with col3:
            st.metric(
                "Total Executions",
                f"{total_model_executions:,}",
                delta=f"{total_model_executions - wasted_model_executions:,} productive",
                delta_color="normal",
                help="Total model executions analyzed (excluding views)"
            )
        
        with col4:
            st.metric(
                "Avg. Cost per Waste",
                f"${avg_cost_per_wasted_execution:,.2f}",
                help="Average cost per wasted execution (when waste occurs)"
            )
        
        with col5:
            # Annualized savings estimate
            days_analyzed = (end_date - start_date).days if (end_date - start_date).days > 0 else 1
            daily_waste = wasted_cost / days_analyzed
            annual_savings = daily_waste * 365
            st.metric(
                "Annual Savings (Est.)",
                f"${annual_savings:,.0f}",
                help="Estimated annual savings if SAO eliminates this waste"
            )
        
        st.info(f"💡 **With SAO enabled**, these {wasted_model_executions:,} model executions would have been automatically skipped, saving ${wasted_cost:,.2f}!")
        
        # WASTE BREAKDOWN
        st.divider()
        st.subheader("📊 Waste Breakdown")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Waste by category
            st.markdown("**Waste by Category**")
            category_waste = waste_df.groupby('waste_category').agg({
                'cost': 'sum',
                'unique_id': 'count'
            }).reset_index()
            category_waste.columns = ['Category', 'Total Cost', 'Count']
            category_waste = category_waste.sort_values('Total Cost', ascending=False)
            
            fig = px.pie(
                category_waste,
                values='Total Cost',
                names='Category',
                title='Wasted Cost by Category',
                hole=0.4
            )
            plotly_chart_no_warnings(fig, use_container_width=True)
        
        with col2:
            # Waste by materialization
            st.markdown("**Waste by Model Type**")
            mat_waste = waste_df.groupby('materialization').agg({
                'cost': 'sum',
                'unique_id': 'count'
            }).reset_index()
            mat_waste.columns = ['Materialization', 'Total Cost', 'Count']
            mat_waste = mat_waste.sort_values('Total Cost', ascending=False)
            
            fig = px.bar(
                mat_waste,
                x='Materialization',
                y='Total Cost',
                title='Wasted Cost by Materialization Type',
                text='Total Cost'
            )
            fig.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
            plotly_chart_no_warnings(fig, use_container_width=True)
        
        # WASTE OVER TIME
        st.divider()
        st.subheader("📈 Waste Over Time")
        
        # Aggregate by run
        run_waste = df.groupby(['run_id', 'run_created_at', 'job_name']).agg({
            'cost': 'sum',
            'is_waste': lambda x: (x == True).sum()
        }).reset_index()
        run_waste.columns = ['run_id', 'run_created_at', 'job_name', 'total_cost', 'wasted_count']
        
        # Calculate wasted cost
        waste_by_run = waste_df.groupby(['run_id']).agg({
            'cost': 'sum'
        }).reset_index()
        waste_by_run.columns = ['run_id', 'wasted_cost']
        
        run_waste = run_waste.merge(waste_by_run, on='run_id', how='left')
        run_waste['wasted_cost'] = run_waste['wasted_cost'].fillna(0)
        run_waste = run_waste.sort_values('run_created_at')
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=run_waste['run_created_at'],
            y=run_waste['wasted_cost'],
            name='Wasted Cost',
            mode='lines+markers',
            line=dict(color='red', width=2),
            fill='tozeroy'
        ))
        
        fig.update_layout(
            title='Wasted Cost Over Time',
            xaxis_title='Run Date',
            yaxis_title='Wasted Cost ($)',
            hovermode='x unified'
        )
        
        plotly_chart_no_warnings(fig, use_container_width=True)
        
        # TOP WASTERS
        st.divider()
        st.subheader("🔥 Top Wasters")
        
        # Aggregate by model - need total run count and waste count
        model_waste = waste_df.groupby(['unique_id', 'name', 'materialization', 'waste_category']).agg({
            'cost': 'sum',
            'run_id': 'count',
            'rows_affected_clean': 'mean',
            'execution_time': 'mean'
        }).reset_index()
        model_waste.columns = ['unique_id', 'Model Name', 'Materialization', 'Waste Category', 
                               'Total Wasted Cost', 'Waste Count', 'Avg Rows Changed', 'Avg Execution Time (s)']
        
        # Get total run count per model (including non-waste runs)
        total_runs_per_model = df[~view_mask].groupby('unique_id')['run_id'].nunique().reset_index()
        total_runs_per_model.columns = ['unique_id', 'Run Count']
        
        # Merge to get run count
        model_waste = model_waste.merge(total_runs_per_model, on='unique_id', how='left')
        
        # Calculate waste percentage
        model_waste['Waste %'] = (model_waste['Waste Count'] / model_waste['Run Count'] * 100).round(1)
        
        # Reorder columns
        model_waste = model_waste[['unique_id', 'Model Name', 'Materialization', 'Waste Category', 
                                   'Total Wasted Cost', 'Waste Count', 'Run Count', 'Waste %',
                                   'Avg Rows Changed', 'Avg Execution Time (s)']]
        
        model_waste = model_waste.sort_values('Total Wasted Cost', ascending=False)
        
        # Show top 20
        top_wasters = model_waste.head(20).copy()
        
        fig = px.bar(
            top_wasters,
            x='Total Wasted Cost',
            y='Model Name',
            orientation='h',
            title='Top 20 Models by Wasted Cost',
            color='Waste Category',
            hover_data=['Materialization', 'Waste Count', 'Run Count', 'Waste %', 'Avg Rows Changed', 'Avg Execution Time (s)']
        )
        fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
        plotly_chart_no_warnings(fig, use_container_width=True)
        
        # DETAILED TABLE
        st.divider()
        st.subheader("📋 Detailed Waste Analysis")
        
        # Format for display
        display_waste = model_waste.copy()
        display_waste['Total Wasted Cost'] = display_waste['Total Wasted Cost'].apply(lambda x: f"${x:.2f}")
        display_waste['Waste %'] = display_waste['Waste %'].apply(lambda x: f"{x:.1f}%")
        display_waste['Avg Rows Changed'] = display_waste['Avg Rows Changed'].apply(lambda x: f"{x:.1f}")
        display_waste['Avg Execution Time (s)'] = display_waste['Avg Execution Time (s)'].apply(lambda x: f"{x:.2f}")
        
        # Drop unique_id for cleaner display
        display_waste = display_waste.drop('unique_id', axis=1)
        
        st.dataframe(
            display_waste,
            width="stretch",
            hide_index=True
        )
        
        st.caption(f"💡 **Waste %** shows how often each model wastes compute (Waste Count ÷ Run Count). Higher % = more consistent waste pattern.")
        
        # RECOMMENDATIONS
        st.divider()
        st.subheader("💡 Recommendations")
        
        st.markdown(f"""
        ### Immediate Actions
        1. **Enable SAO on these jobs**: Focus on jobs with the highest waste
        2. **Review {len(model_waste)} models**: These consistently run with minimal/no changes
        3. **Estimated Annual Savings**: ${annual_savings:,.0f} by implementing SAO
        
        ### Top Priority Models
        The following models are your biggest opportunities:
        """)
        
        priority_models = model_waste.head(5)
        for idx, row in priority_models.iterrows():
            st.markdown(f"- **{row['Model Name']}** ({row['Materialization']}): Wasted ${row['Total Wasted Cost']:.2f} across {int(row['Waste Count'])} runs")
        
        st.markdown("""
        ### Implementation Steps
        1. **Phase 1**: Enable SAO on scheduled jobs with highest waste
        2. **Phase 2**: Add freshness checks to models that rebuild frequently
        3. **Phase 3**: Extend to CI/merge jobs for full efficiency
        
        ### Expected Impact
        - ✅ Immediate cost reduction
        - ✅ Faster job completion times
        - ✅ Reduced warehouse utilization
        - ✅ More efficient data pipelines
        """)
        
    except Exception as e:
        st.error(f"❌ Error during waste analysis: {str(e)}")
        st.exception(e)


def show_job_overlap_analysis():
    """Show job overlap analysis to identify models run in multiple jobs."""
    st.header("🔀 Job Overlap Analysis")
    st.markdown("**Identify unnecessary model duplication** across jobs in your environment (pre-SAO optimization)")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Info about what this does
    with st.expander("ℹ️ What is Job Overlap Analysis?", expanded=False):
        st.markdown("""
        ### The Problem
        In environments **without State-Aware Orchestration (SAO)**, multiple jobs may independently run the same models, wasting compute resources.
        
        ### Example
        ```
        Job A: Runs models X, Y, Z
        Job B: Runs models Y, Z, W
        Job C: Runs models Z, W, Q
        
        Problem: Models Y, Z, W are executed multiple times!
        ```
        
        ### Solution
        This analysis helps you:
        1. **Identify overlapping models** across jobs
        2. **Quantify waste** (how many redundant executions)
        3. **Consolidate jobs** to eliminate duplication
        4. **Prioritize SAO adoption** to automatically handle this
        
        ### When to Use
        - Before implementing SAO
        - When consolidating jobs
        - When reducing compute costs
        - When auditing job efficiency
        """)
    
    st.divider()
    
    # Unified configuration section
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        environment_id_input = st.text_input(
            "Environment ID",
            value=config.get('environment_id', ''),
            help="Environment ID to analyze (required for SAO, optional for overlap)",
            key="combined_environment_id"
        )
    
    with col2:
        job_types_filter = st.multiselect(
            "Filter Job Types (for Overlap Analysis Only)",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Which job types to analyze for overlap",
            key="combined_job_types"
        )
    
    with col3:
        st.markdown("")
        st.markdown("")
        analyze_button = st.button("🔍 Analyze All", type="primary", key="combined_analyze", width="stretch")
    
    if not analyze_button:
        st.info("⬆️ Click 'Analyze All' to run SAO adoption and job overlap analysis")
        
        with st.expander("📊 What You'll See"):
            st.markdown("""
            ### SAO Adoption Analysis
            - Overall SAO adoption metrics and charts
            - Job type breakdown (CI, Merge, Scheduled, Other)
            - List of jobs without SAO enabled
            
            ### Job Overlap Analysis
            - Models run in multiple jobs (redundant execution)
            - Overlap metrics and visualizations
            - Recommendations for consolidation
            
            **Note**: SAO analysis requires an Environment ID. Job overlap can run with or without one.
            """)
        return
    
    # Run SAO Analysis if environment ID provided
    if environment_id_input:
        st.subheader("🔄 SAO Adoption Analysis")
        try:
            env_id = int(environment_id_input)
            
            with st.spinner("🔄 Fetching jobs and analyzing SAO..."):
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                
                # Paginate through ACTIVE jobs only (not deleted/archived)
                jobs = []
                offset = 0
                while True:
                    params = {'limit': 100, 'environment_id': env_id, 'state': 1, 'offset': offset}  # state=1 for active jobs only
                    jobs_response = requests.get(jobs_url, headers=headers, params=params)
                    jobs_response.raise_for_status()
                    page_jobs = jobs_response.json().get('data', [])
                    if not page_jobs:
                        break
                    jobs.extend(page_jobs)
                    if len(page_jobs) < 100:
                        break
                    offset += 100
                
                if jobs:
                    # Count SAO jobs
                    sao_jobs = [job for job in jobs if check_job_has_sao(job)]
                    total_jobs = len(jobs)
                    sao_count = len(sao_jobs)
                    sao_pct = (sao_count / total_jobs * 100) if total_jobs > 0 else 0
                    
                    # Metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Jobs", f"{total_jobs:,}")
                    with col2:
                        st.metric("SAO-Enabled", f"{sao_count:,}", delta=f"{sao_pct:.1f}%")
                    with col3:
                        st.metric("Non-SAO", f"{total_jobs - sao_count:,}")
                    
                    # Charts
                    chart_col1, chart_col2 = st.columns(2)
                    chart_data = pd.DataFrame({
                        'Category': ['SAO-Enabled', 'Non-SAO'],
                        'Count': [sao_count, total_jobs - sao_count]
                    })
                    
                    with chart_col1:
                        fig_pie = px.pie(
                            chart_data,
                            values='Count',
                            names='Category',
                            title='SAO Adoption',
                            color='Category',
                            color_discrete_map={'SAO-Enabled': '#10b981', 'Non-SAO': '#6b7280'},
                            hole=0.4
                        )
                        plotly_chart_no_warnings(fig_pie, use_container_width=True)
                    
                    with chart_col2:
                        # Job type breakdown
                        job_type_data = defaultdict(lambda: {'sao': 0, 'non_sao': 0})
                        for job in jobs:
                            job_type = determine_job_type(job.get('triggers', {}))
                            if check_job_has_sao(job):
                                job_type_data[job_type]['sao'] += 1
                            else:
                                job_type_data[job_type]['non_sao'] += 1
                        
                        breakdown_list = []
                        for jt, counts in job_type_data.items():
                            total = counts['sao'] + counts['non_sao']
                            if total > 0:
                                breakdown_list.append({
                                    'Job Type': jt.upper(),
                                    'SAO': counts['sao'],
                                    'Non-SAO': counts['non_sao']
                                })
                        
                        if breakdown_list:
                            breakdown_df = pd.DataFrame(breakdown_list)
                            fig_bar = go.Figure()
                            fig_bar.add_trace(go.Bar(name='SAO', x=breakdown_df['Job Type'], y=breakdown_df['SAO'], marker_color='#10b981'))
                            fig_bar.add_trace(go.Bar(name='Non-SAO', x=breakdown_df['Job Type'], y=breakdown_df['Non-SAO'], marker_color='#6b7280'))
                            fig_bar.update_layout(title='SAO by Job Type', barmode='group', height=300)
                            plotly_chart_no_warnings(fig_bar, use_container_width=True)
                    
                    # Non-SAO jobs list
                    non_sao_jobs = [{'Job ID': j['id'], 'Job Name': j['name'], 'Type': determine_job_type(j.get('triggers', {})).upper()} 
                                   for j in jobs if not check_job_has_sao(j)]
                    
                    if non_sao_jobs:
                        with st.expander(f"💡 {len(non_sao_jobs)} Jobs Without SAO"):
                            st.dataframe(pd.DataFrame(non_sao_jobs), width="stretch", hide_index=True)
                    
                    if sao_pct < 50:
                        st.warning(f"⚠️ Only {sao_pct:.1f}% of jobs have SAO enabled. Consider enabling SAO to reduce compute costs and avoid model overlap.")
                    else:
                        st.success(f"✅ Good SAO adoption rate: {sao_pct:.1f}%")
                else:
                    st.warning("No jobs found for this environment")
                    
        except Exception as e:
            st.error(f"❌ Error analyzing SAO: {str(e)}")
    
    # Run Job Overlap Analysis
    st.divider()
    st.subheader("🔀 Job Overlap Analysis")
    
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Fetch all ACTIVE jobs only (not deleted/archived)
        status_text.text("🔄 Fetching active jobs...")
        url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
        headers = {'Authorization': f'Token {config["api_key"]}'}
        
        params = {
            'limit': 100,
            'state': 1  # Only active jobs (state=1), not deleted (state=2)
        }
        if environment_id_input:
            params['environment_id'] = environment_id_input
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        jobs = response.json().get('data', [])
        
        # Filter by job type
        jobs = filter_jobs_by_type(jobs, job_types_filter)
        
        if not jobs:
            st.warning(f"No jobs found matching the selected job types: {', '.join(job_types_filter)}")
            progress_bar.empty()
            status_text.empty()
            return
        
        st.success(f"✅ Found {len(jobs)} jobs to analyze (filtered by: {', '.join(job_types_filter)})")
        progress_bar.progress(10)
        
        # Analyze each job
        status_text.text(f"🔄 Analyzing jobs...")
        
        model_to_jobs = defaultdict(list)
        job_to_models = {}
        jobs_analyzed = 0
        jobs_skipped = 0
        jobs_running = 0
        jobs_never_succeeded = 0
        
        for idx, job in enumerate(jobs):
            job_id = job['id']
            job_name = job['name']
            
            progress = 10 + int((idx / len(jobs)) * 80)
            progress_bar.progress(progress)
            status_text.text(f"🔄 Analyzing job {idx + 1}/{len(jobs)}: {job_name[:50]}...")
            
            try:
                # First, check if job has any runs at all and their status
                check_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/'
                check_params = {
                    'job_definition_id': job_id,
                    'limit': 1,
                    'order_by': '-id'  # Get most recent run regardless of status
                }
                
                check_response = requests.get(check_url, headers=headers, params=check_params)
                check_response.raise_for_status()
                recent_runs = check_response.json().get('data', [])
                
                # Check if most recent run is currently running
                # If so, we'll fetch the latest successful run instead of skipping
                is_currently_running = recent_runs and recent_runs[0].get('status') in [1, 2, 3]  # Queued, Starting, Running
                if is_currently_running:
                    jobs_running += 1
                
                # Now get latest successful run
                run_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/'
                run_params = {
                    'job_definition_id': job_id,
                    'limit': 1,
                    'order_by': '-id',
                    'status': '10'  # Success
                }
                
                run_response = requests.get(run_url, headers=headers, params=run_params)
                run_response.raise_for_status()
                runs = run_response.json().get('data', [])
                
                if not runs:
                    # No successful runs found
                    jobs_never_succeeded += 1
                    jobs_skipped += 1
                    continue
                
                run_id = runs[0]['id']
                
                # Use the new step-based approach to get accurate model lists
                # Fetch run steps and filter to only run/build commands
                run_details_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/'
                run_details_params = {'include_related': '["run_steps"]'}
                
                run_details_response = requests.get(run_details_url, headers=headers, params=run_details_params)
                run_details_response.raise_for_status()
                
                run_data = run_details_response.json().get('data', {})
                run_steps = run_data.get('run_steps', [])
                
                # Filter to only 'dbt run', 'dbt build', and 'dbt test' commands
                relevant_steps = []
                for step in run_steps:
                    step_name = step.get('name', '')
                    step_index = step.get('index')
                    
                    if 'dbt run' in step_name or 'dbt build' in step_name or 'dbt test' in step_name:
                        relevant_steps.append(step_index)
                
                # Collect models from all relevant steps
                all_models = set()  # Use set to avoid duplicates
                
                if relevant_steps:
                    # Fetch run_results.json from each relevant step
                    for step_idx in relevant_steps:
                        artifacts_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/artifacts/run_results.json?step={step_idx}'
                        
                        try:
                            results_response = requests.get(artifacts_url, headers=headers)
                            if results_response.ok:
                                run_results = results_response.json()
                                
                                # Extract models from this step
                                for result in run_results.get('results', []):
                                    unique_id = result.get('unique_id', '')
                                    if unique_id.startswith('model.'):
                                        all_models.add(unique_id)
                        except Exception:
                            # If this step fails, continue to next step
                            continue
                else:
                    # Fallback: no relevant steps found, try default run_results.json
                    artifacts_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/artifacts/run_results.json'
                    
                    try:
                        results_response = requests.get(artifacts_url, headers=headers)
                        if results_response.ok:
                            run_results = results_response.json()
                            
                            for result in run_results.get('results', []):
                                unique_id = result.get('unique_id', '')
                                if unique_id.startswith('model.'):
                                    all_models.add(unique_id)
                    except Exception:
                        pass
                
                if not all_models:
                    jobs_skipped += 1
                    continue
                
                # Convert set to list
                models = list(all_models)
                
                # Store mappings
                job_to_models[job_name] = {
                    'job_id': job_id,
                    'run_id': run_id,
                    'models': models
                }
                
                for model in models:
                    model_to_jobs[model].append({
                        'job_name': job_name,
                        'job_id': job_id,
                        'run_id': run_id
                    })
                
                jobs_analyzed += 1
                
            except Exception as e:
                jobs_skipped += 1
                continue
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        # Find overlapping models
        overlapping_models = {
            model: jobs_list
            for model, jobs_list in model_to_jobs.items()
            if len(jobs_list) > 1
        }
        
        # Display results
        st.divider()
        st.subheader("📊 Analysis Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs Analyzed", f"{jobs_analyzed:,}")
            if jobs_running > 0:
                st.caption(f"ℹ️ {jobs_running} job(s) currently running - using latest successful run")
            if jobs_skipped > 0:
                skip_details = []
                if jobs_never_succeeded > 0:
                    skip_details.append(f"{jobs_never_succeeded} never succeeded")
                other_skipped = jobs_skipped - jobs_never_succeeded
                if other_skipped > 0:
                    skip_details.append(f"{other_skipped} other")
                
                if skip_details:
                    st.caption(f"⚠️ {jobs_skipped} jobs skipped: {', '.join(skip_details)}")
        
        with col2:
            st.metric("Total Unique Models", f"{len(model_to_jobs):,}")
        
        with col3:
            st.metric("Overlapping Models", f"{len(overlapping_models):,}")
        
        with col4:
            overlap_rate = (len(overlapping_models) / len(model_to_jobs) * 100) if len(model_to_jobs) > 0 else 0
            st.metric("Overlap Rate", f"{overlap_rate:.1f}%")
        
        # Show info about currently running and skipped jobs
        info_messages = []
        
        if jobs_running > 0:
            info_messages.append(f'- **{jobs_running} job(s) currently running** - Using their latest successful run for analysis')
        
        if jobs_skipped > 0:
            skip_info = []
            if jobs_never_succeeded > 0:
                skip_info.append(f'- **{jobs_never_succeeded} never succeeded** - No successful runs in history (may be new, disabled, or have errors)')
            other_skipped = jobs_skipped - jobs_never_succeeded
            if other_skipped > 0:
                skip_info.append(f'- **{other_skipped} other** - Missing artifacts or other issues')
            
            if skip_info:
                info_messages.append(f'**{jobs_skipped} job(s) were excluded:**')
                info_messages.extend(skip_info)
        
        if info_messages:
            st.divider()
            st.info('ℹ️ ' + '\n'.join(info_messages))
        
        # Assessment
        if len(overlapping_models) == 0:
            st.success("✅ **Excellent!** No models are being run in multiple jobs. Your job structure is optimized!")
            
            # Show table of all jobs analyzed even when no overlap
            st.divider()
            st.subheader("📊 Jobs Analyzed")
            
            # Create summary of all jobs
            jobs_summary = []
            for job_name, job_data in job_to_models.items():
                jobs_summary.append({
                    'Job Name': job_name,
                    'Job ID': job_data['job_id'],
                    'Run ID': job_data['run_id'],
                    'Model Count': len(job_data['models'])
                })
            
            jobs_summary_df = pd.DataFrame(jobs_summary)
            jobs_summary_df = jobs_summary_df.sort_values('Model Count', ascending=False)
            
            st.dataframe(
                jobs_summary_df,
                width='stretch',
                hide_index=True
            )
            
            st.caption(f"✓ Analyzed {len(jobs_summary_df)} jobs with a total of {len(model_to_jobs)} unique models")
            
        elif overlap_rate < 10:
            st.info(f"✓ **Good** - Only {overlap_rate:.1f}% of models have overlap. Minor optimization opportunity.")
        elif overlap_rate < 25:
            st.warning(f"⚠️ **Moderate Waste** - {overlap_rate:.1f}% of models are duplicated. Consider job consolidation.")
        else:
            st.error(f"❌ **High Waste** - {overlap_rate:.1f}% of models are duplicated! Significant optimization opportunity.")
        
        if len(overlapping_models) > 0:
            # Most duplicated models
            st.divider()
            st.subheader("🔝 Most Duplicated Models")
            
            # Sort by number of jobs
            sorted_overlaps = sorted(
                overlapping_models.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )[:20]  # Top 20
            
            # Create dataframe for chart
            overlap_data = []
            for model_id, jobs_list in sorted_overlaps:
                model_name = model_id.split('.')[-1]
                overlap_data.append({
                    'Model': model_name,
                    'Full ID': model_id,
                    'Job Count': len(jobs_list),
                    'Jobs': ', '.join([j['job_name'] for j in jobs_list])
                })
            
            overlap_df = pd.DataFrame(overlap_data)
            
            # Bar chart
            fig_overlap = px.bar(
                overlap_df.head(20),
                x='Job Count',
                y='Model',
                orientation='h',
                title='Top 20 Models by Number of Jobs Running Them',
                text='Job Count',
                color='Job Count',
                color_continuous_scale='Reds'
            )
            
            fig_overlap.update_layout(height=600, showlegend=False)
            plotly_chart_no_warnings(fig_overlap, use_container_width=True)
            
            # Detailed table
            st.divider()
            st.subheader("📋 Overlap Details")
            
            # Expandable sections for each overlapping model
            for model_id, jobs_list in sorted_overlaps[:10]:  # Show top 10 in detail
                model_name = model_id.split('.')[-1]
                
                with st.expander(f"🔸 {model_name} (in {len(jobs_list)} jobs)"):
                    st.code(model_id, language=None)
                    st.markdown("**Found in these jobs:**")
                    
                    for job_info in jobs_list:
                        st.markdown(f"- **{job_info['job_name']}** (Job ID: {job_info['job_id']}, Run ID: {job_info['run_id']})")
                    
                    st.warning(f"💡 **Recommendation**: This model is executed {len(jobs_list)} times. Consider consolidating to a single job or implementing SAO.")
            
            # Full overlap table
            st.divider()
            st.subheader("📊 Complete Overlap Report")
            
            st.dataframe(
                overlap_df[['Model', 'Job Count', 'Jobs']],
                width='stretch',
                hide_index=True
            )
            
            # Waste calculation
            st.divider()
            st.subheader("💸 Waste Calculation")
            
            total_redundant_executions = sum(len(jobs) - 1 for jobs in overlapping_models.values())
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric(
                    "Redundant Executions",
                    f"{total_redundant_executions:,}",
                    help="Number of unnecessary model executions (executions beyond the first)"
                )
                
                st.caption("These executions could be eliminated through job consolidation or SAO")
            
            with col2:
                if jobs_analyzed > 0:
                    avg_redundancy = total_redundant_executions / jobs_analyzed
                    st.metric(
                        "Avg Redundancy per Job",
                        f"{avg_redundancy:.1f}",
                        help="Average number of duplicate models per job"
                    )
            
            # Recommendations
            st.divider()
            st.subheader("💡 Recommendations")
            
            if overlap_rate > 50:
                st.markdown("""
                ### 🚨 High Priority Actions
                1. **Implement State-Aware Orchestration (SAO)** - Automates model reuse across jobs
                2. **Consolidate jobs** - Merge jobs with heavy overlap
                3. **Review job design** - Consider if all jobs are necessary
                """)
            elif overlap_rate > 25:
                st.markdown("""
                ### ⚠️ Medium Priority Actions
                1. **Plan for SAO adoption** - Will automatically eliminate this waste
                2. **Consolidate high-overlap jobs** - Focus on top duplicated models
                3. **Document job purposes** - Ensure each job has a clear, unique purpose
                """)
            else:
                st.markdown("""
                ### ✓ Low Priority Actions
                1. **Monitor over time** - Track if overlap increases
                2. **Consider SAO** for automatic optimization
                3. **Document current structure** - Good baseline for future changes
                """)
            
            # Job-to-job overlap matrix (for smaller sets)
            if jobs_analyzed <= 20:
                st.divider()
                st.subheader("🔀 Job-to-Job Overlap Matrix")
                
                # Create matrix showing overlap between jobs
                job_names = list(job_to_models.keys())
                overlap_matrix = []
                
                for job1 in job_names:
                    row = {'Job': job1}
                    models1 = set(job_to_models[job1]['models'])
                    
                    for job2 in job_names:
                        if job1 == job2:
                            row[job2] = len(models1)
                        else:
                            models2 = set(job_to_models[job2]['models'])
                            overlap = len(models1.intersection(models2))
                            row[job2] = overlap  # Use 0 instead of empty string for consistent type
                    
                    overlap_matrix.append(row)
                
                matrix_df = pd.DataFrame(overlap_matrix)
                st.dataframe(matrix_df, width='stretch', hide_index=True)
                st.caption("Numbers show how many models are shared between jobs. Diagonal shows total models in each job.")
        
        # Download section
        st.divider()
        st.subheader("💾 Download Analysis Results")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Model to jobs mapping
            model_to_jobs_json = json.dumps(dict(model_to_jobs), indent=2, default=str)
            st.download_button(
                label="📥 Model-to-Jobs Mapping",
                data=model_to_jobs_json,
                file_name=f"model_to_jobs_{config['account_id']}.json",
                mime="application/json"
            )
        
        with col2:
            # Job to models mapping
            job_to_models_json = json.dumps(job_to_models, indent=2, default=str)
            st.download_button(
                label="📥 Job-to-Models Mapping",
                data=job_to_models_json,
                file_name=f"job_to_models_{config['account_id']}.json",
                mime="application/json"
            )
        
        with col3:
            # Overlapping models only
            if overlapping_models:
                overlapping_json = json.dumps(overlapping_models, indent=2, default=str)
                st.download_button(
                    label="📥 Overlapping Models Report",
                    data=overlapping_json,
                    file_name=f"overlapping_models_{config['account_id']}.json",
                    mime="application/json"
                )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and permissions")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

