"""
This script replicates the log_freshness dbt macro functionality in Python.
It fetches dbt Cloud run artifacts and extracts freshness configuration from nodes and sources.

Usage:
    python log_freshness.py

Environment variables required:
    - DBT_API_KEY: dbt Cloud API token
    - DBT_ACCOUNT_ID: dbt Cloud account ID
    - DBT_RUN_ID: dbt Cloud run ID to analyze
    - DBT_URL (optional): dbt Cloud URL (defaults to https://cloud.getdbt.com)
    - DBT_CONNECTION_STRING (optional): Database connection string for logging results
"""

import requests
import os
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional


class DBTFreshnessLogger:
    def __init__(self, api_base: str, api_key: str, account_id: str, run_id: str):
        """
        Initialize the freshness logger.
        
        Args:
            api_base: dbt Cloud base URL
            api_key: dbt Cloud API token
            account_id: dbt Cloud account ID
            run_id: Run ID to analyze
        """
        self.api_base = api_base
        self.api_key = api_key
        self.account_id = account_id
        self.run_id = run_id
        self.headers = {'Authorization': f'Token {api_key}'}
        
    def fetch_manifest(self) -> Dict[str, Any]:
        """Fetch manifest.json artifact from dbt Cloud."""
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/artifacts/manifest.json'
        print(f'Fetching manifest from: {url}')
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def fetch_run_results(self, step: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch run_results.json artifact from dbt Cloud.
        
        Args:
            step: Optional step number to fetch results from a specific step
        
        Returns:
            Dictionary containing run results
        """
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/artifacts/run_results.json'
        if step is not None:
            url += f'?step={step}'
        print(f'Fetching run results from: {url}')
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def fetch_run_steps(self) -> List[Dict[str, Any]]:
        """
        Fetch run steps from dbt Cloud API and filter to only dbt run/build commands.
        
        Returns:
            List of relevant run steps (only those with 'dbt run' or 'dbt build')
        """
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/'
        params = {'include_related': '["run_steps"]'}
        
        print(f'Fetching run steps from: {url}')
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        all_steps = data.get('data', {}).get('run_steps', [])
        
        # Filter to only 'dbt run' and 'dbt build' commands
        relevant_steps = []
        for step in all_steps:
            step_name = step.get('name', '')
            step_name_lower = step_name.lower()
            step_index = step.get('index')
            
            # Check if this is a run, build, or test command
            # Using space/backtick after command name (e.g., "dbt run " or "dbt run`")
            # This automatically filters out "dbt run-operation" (which has a dash, not a space)
            has_run = 'dbt run ' in step_name_lower or 'dbt run`' in step_name_lower
            has_build = 'dbt build ' in step_name_lower or 'dbt build`' in step_name_lower
            has_test = 'dbt test ' in step_name_lower or 'dbt test`' in step_name_lower
            is_excluded = ('dbt source' in step_name_lower or 
                          'dbt compile' in step_name_lower or 
                          'dbt docs' in step_name_lower or 
                          'dbt deps' in step_name_lower)
            
            is_relevant = (has_run or has_build or has_test) and not is_excluded
            
            if is_relevant:
                relevant_steps.append({
                    'index': step_index,
                    'name': step_name,
                    'status': step.get('status'),
                    'status_humanized': step.get('status_humanized')
                })
                print(f'  ✓ Found relevant step {step_index}: {step_name}')
            else:
                print(f'  ⊘ Skipping step {step_index}: {step_name}')
        
        print(f'Found {len(relevant_steps)} relevant run/build steps out of {len(all_steps)} total steps')
        return relevant_steps
    
    def aggregate_run_results_from_steps(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch run_results.json from all relevant steps and aggregate the results.
        This eliminates guesswork by only looking at actual dbt run/build commands.
        
        Args:
            manifest: The manifest.json data
        
        Returns:
            Dictionary with aggregated run status information
        """
        # Get relevant run steps (only run/build commands)
        try:
            run_steps = self.fetch_run_steps()
        except Exception as e:
            print(f'⚠️  Warning: Could not fetch run steps: {e}')
            print(f'  Falling back to default run_results.json')
            # Fallback to fetching without step parameter
            run_results = self.fetch_run_results()
            return self._process_single_run_results(manifest, run_results)
        
        if not run_steps:
            print('⚠️  No relevant run/build steps found, fetching default run_results.json')
            run_results = self.fetch_run_results()
            return self._process_single_run_results(manifest, run_results)
        
        # Fetch run_results.json from each relevant step
        all_results = []
        for step in run_steps:
            step_index = step['index']
            try:
                print(f'\nFetching run_results.json for step {step_index}: {step["name"]}')
                run_results = self.fetch_run_results(step=step_index)
                all_results.append({
                    'step_index': step_index,
                    'step_name': step['name'],
                    'run_results': run_results
                })
            except Exception as e:
                print(f'  ⚠️  Warning: Could not fetch run_results for step {step_index}: {e}')
                continue
        
        # Aggregate results across all steps
        return self._aggregate_results(manifest, all_results)
    
    def _aggregate_results(self, manifest: Dict[str, Any], step_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate run results from multiple steps.
        
        Args:
            manifest: The manifest.json data
            step_results: List of step results with run_results.json data
        
        Returns:
            Dictionary with aggregated status information
        """
        # Resource types to exclude
        excluded_types = ['source', 'analysis', 'operation', 'seed', 'snapshot', 'test']
        
        # Track all models and their statuses across steps
        model_statuses = {}  # unique_id -> {status, execution_time, etc.}
        
        for step_data in step_results:
            step_index = step_data['step_index']
            run_results = step_data['run_results']
            
            print(f'\nProcessing results from step {step_index}:')
            
            for result in run_results.get('results', []):
                unique_id = result.get('unique_id')
                
                # Look up resource type
                resource_type = None
                if unique_id in manifest.get('nodes', {}):
                    resource_type = manifest['nodes'][unique_id].get('resource_type')
                
                # Skip excluded types
                if not resource_type or resource_type in excluded_types:
                    continue
                
                # Only include models
                if resource_type != 'model':
                    continue
                
                # Get status directly from run_results (no heuristics!)
                status = result.get('status')
                execution_time = result.get('execution_time', 0)
                
                # Get timing details
                timing = result.get('timing', [])
                started_at = None
                completed_at = None
                
                if timing:
                    for t in timing:
                        if t.get('name') == 'execute':
                            started_at = t.get('started_at')
                            completed_at = t.get('completed_at')
                            break
                
                # Get adapter response for row count information
                adapter_response = result.get('adapter_response', {})
                rows_affected = adapter_response.get('rows_affected')
                message = result.get('message', '')
                
                # Get materialization type from manifest
                materialization = None
                if unique_id in manifest.get('nodes', {}):
                    node = manifest['nodes'][unique_id]
                    materialization = node.get('config', {}).get('materialized')
                
                # Store or update model data
                if unique_id not in model_statuses:
                    model_statuses[unique_id] = {
                        'unique_id': unique_id,
                        'name': unique_id.split('.')[-1] if unique_id else 'unknown',
                        'resource_type': resource_type,
                        'status': status,
                        'execution_time': execution_time,
                        'started_at': started_at,
                        'completed_at': completed_at,
                        'rows_affected': rows_affected,
                        'message': message,
                        'materialization': materialization,
                        'adapter_response': adapter_response,
                        'steps': [step_index]
                    }
                else:
                    # Model appeared in multiple steps - track it
                    model_statuses[unique_id]['steps'].append(step_index)
                    # Keep the most severe status (error > success > reused)
                    if status == 'error':
                        model_statuses[unique_id]['status'] = 'error'
                    elif status == 'success' and model_statuses[unique_id]['status'] != 'error':
                        model_statuses[unique_id]['status'] = 'success'
                        # Update row count info for successful runs
                        model_statuses[unique_id]['rows_affected'] = rows_affected
                        model_statuses[unique_id]['message'] = message
                        model_statuses[unique_id]['adapter_response'] = adapter_response
        
        # Convert to list
        models_list = list(model_statuses.values())
        
        # Print summary diagnostics
        total_models = len(models_list)
        status_counts = {}
        for model in models_list:
            status_counts[model['status']] = status_counts.get(model['status'], 0) + 1
        
        print(f'\n📊 Aggregated Run Status Summary:')
        print(f'  Total unique models: {total_models}')
        for status, count in sorted(status_counts.items()):
            pct = (count / total_models * 100) if total_models > 0 else 0
            print(f'  {status}: {count} ({pct:.1f}%)')
        
        if total_models > 0:
            execution_times = [m['execution_time'] for m in models_list]
            avg_time = sum(execution_times) / len(execution_times)
            sorted_times = sorted(execution_times)
            median_time = sorted_times[len(sorted_times) // 2] if sorted_times else 0
            print(f'  Avg execution time: {avg_time:.3f}s')
            print(f'  Median execution time: {median_time:.3f}s')
        
        return {
            'run_id': self.run_id,
            'models': models_list
        }
    
    def _process_single_run_results(self, manifest: Dict[str, Any], run_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process run results from a single run_results.json file (fallback method).
        No log parsing, no heuristics - just what's in run_results.json.
        
        Args:
            manifest: The manifest.json data
            run_results: The run_results.json data
        
        Returns:
            Dictionary with run status information
        """
        excluded_types = ['source', 'analysis', 'operation', 'seed', 'snapshot', 'test']
        
        models_list = []
        
        for result in run_results.get('results', []):
            unique_id = result.get('unique_id')
            
            # Look up resource type
            resource_type = None
            if unique_id in manifest.get('nodes', {}):
                resource_type = manifest['nodes'][unique_id].get('resource_type')
            
            # Skip excluded types
            if not resource_type or resource_type in excluded_types:
                continue
            
            # Only include models
            if resource_type != 'model':
                continue
            
            # Get status directly from run_results
            status = result.get('status')
            execution_time = result.get('execution_time', 0)
            
            # Get timing details
            timing = result.get('timing', [])
            started_at = None
            completed_at = None
            
            if timing:
                for t in timing:
                    if t.get('name') == 'execute':
                        started_at = t.get('started_at')
                        completed_at = t.get('completed_at')
                        break
            
            # Get adapter response for row count information
            adapter_response = result.get('adapter_response', {})
            rows_affected = adapter_response.get('rows_affected')
            message = result.get('message', '')
            
            # Get materialization type from manifest
            materialization = None
            if unique_id in manifest.get('nodes', {}):
                node = manifest['nodes'][unique_id]
                materialization = node.get('config', {}).get('materialized')
            
            models_list.append({
                'unique_id': unique_id,
                'name': unique_id.split('.')[-1] if unique_id else 'unknown',
                'resource_type': resource_type,
                'status': status,
                'execution_time': execution_time,
                'started_at': started_at,
                'completed_at': completed_at,
                'rows_affected': rows_affected,
                'message': message,
                'materialization': materialization,
                'adapter_response': adapter_response
            })
        
        # Print summary
        total_models = len(models_list)
        status_counts = {}
        for model in models_list:
            status_counts[model['status']] = status_counts.get(model['status'], 0) + 1
        
        print(f'\n📊 Run Status Summary:')
        print(f'  Total models: {total_models}')
        for status, count in sorted(status_counts.items()):
            pct = (count / total_models * 100) if total_models > 0 else 0
            print(f'  {status}: {count} ({pct:.1f}%)')
        
        return {
            'run_id': self.run_id,
            'models': models_list
        }
    
    def extract_freshness_fields(self, freshness_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract individual freshness fields from config.
        
        Args:
            freshness_config: The freshness configuration object
            
        Returns:
            Dictionary with individual freshness fields
        """
        if not freshness_config:
            return {
                'warn_after_count': None,
                'warn_after_period': None,
                'error_after_count': None,
                'error_after_period': None,
                'build_after_count': None,
                'build_after_period': None,
                'updates_on': None
            }
        
        # Extract warn_after
        warn_after = freshness_config.get('warn_after', {}) or {}
        warn_after_count = warn_after.get('count') if isinstance(warn_after, dict) else None
        warn_after_period = warn_after.get('period') if isinstance(warn_after, dict) else None
        
        # Extract error_after
        error_after = freshness_config.get('error_after', {}) or {}
        error_after_count = error_after.get('count') if isinstance(error_after, dict) else None
        error_after_period = error_after.get('period') if isinstance(error_after, dict) else None
        
        # Extract build_after
        build_after = freshness_config.get('build_after', {}) or {}
        build_after_count = build_after.get('count') if isinstance(build_after, dict) else None
        build_after_period = build_after.get('period') if isinstance(build_after, dict) else None
        
        # Extract updates_on
        updates_on = freshness_config.get('updates_on')
        
        return {
            'warn_after_count': warn_after_count,
            'warn_after_period': warn_after_period,
            'error_after_count': error_after_count,
            'error_after_period': error_after_period,
            'build_after_count': build_after_count,
            'build_after_period': build_after_period,
            'updates_on': updates_on
        }
    
    def process_nodes(self, manifest: Dict[str, Any], run_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process nodes from run results to extract freshness information.
        
        Args:
            manifest: The manifest.json data
            run_results: The run_results.json data
            
        Returns:
            List of processed node data
        """
        rows = []
        
        # Resource types to exclude from analysis
        excluded_types = ['source', 'analysis', 'operation', 'seed', 'snapshot', 'test']
        
        # Process all nodes from manifest (not just executed ones)
        for unique_id, node in manifest.get('nodes', {}).items():
            # Skip excluded resource types
            resource_type = node.get('resource_type')
            if resource_type in excluded_types:
                continue
            
            # Try multiple locations for freshness config
            # First try node.config.freshness (most common)
            config = node.get('config', {})
            freshness_config = config.get('freshness')
            
            # If not found, try node.freshness (alternative location)
            if not freshness_config:
                freshness_config = node.get('freshness')
            
            # Check if freshness is configured
            is_freshness_configured = freshness_config is not None and freshness_config != {}
            
            # Extract individual freshness fields
            freshness_fields = self.extract_freshness_fields(freshness_config)
            
            row = {
                'unique_id': node.get('unique_id'),
                'resource_type': node.get('resource_type'),
                'name': node.get('name'),
                'is_freshness_configured': is_freshness_configured,
                'freshness_config': freshness_config
            }
            
            # Add individual freshness fields
            row.update(freshness_fields)
            
            rows.append(row)
        
        return rows
    
    def process_sources(self, manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process all sources from manifest to extract freshness information.
        
        Args:
            manifest: The manifest.json data
            
        Returns:
            List of processed source data
        """
        rows = []
        
        for unique_id, source in manifest.get('sources', {}).items():
            # Get freshness config
            freshness_config = source.get('freshness')
            
            # Check if freshness is actually set
            is_freshness_configured = False
            if freshness_config:
                error_after = freshness_config.get('error_after', {}) or {}
                warn_after = freshness_config.get('warn_after', {}) or {}
                
                # Consider it configured if either error_after or warn_after has a count
                if error_after.get('count') is not None or warn_after.get('count') is not None:
                    is_freshness_configured = True
                else:
                    freshness_config = None
            
            # Extract individual freshness fields
            freshness_fields = self.extract_freshness_fields(freshness_config)
            
            row = {
                'unique_id': source.get('unique_id'),
                'resource_type': source.get('resource_type', 'source'),
                'name': source.get('name'),
                'is_freshness_configured': is_freshness_configured,
                'freshness_config': freshness_config
            }
            
            # Add individual freshness fields
            row.update(freshness_fields)
            
            rows.append(row)
        
        return rows
    
    def log_to_database(self, rows: List[Dict[str, Any]], connection_string: Optional[str] = None):
        """
        Log results to database (placeholder - implement based on your DB).
        
        Args:
            rows: Data to log
            connection_string: Database connection string
        """
        # This is a placeholder - you would implement actual DB connection here
        # For example, using SQLAlchemy, psycopg2, snowflake-connector-python, etc.
        
        if not connection_string:
            print("No database connection string provided - skipping database insert")
            return
        
        print(f"Would insert {len(rows)} rows to database")
        # TODO: Implement actual database insertion based on your connection type
    
    def process_run_statuses(self, manifest: Dict[str, Any], run_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process run results to extract model execution statuses.
        
        This method now uses the new step-based approach:
        1. Fetches run steps and filters to only 'dbt run' and 'dbt build' commands
        2. Fetches run_results.json from each relevant step
        3. Aggregates results across all steps
        
        This eliminates all guesswork and heuristics!
        
        Args:
            manifest: The manifest.json data
            run_results: Optional run_results.json data (for backwards compatibility, but ignored)
            
        Returns:
            Dictionary with run status information
        """
        print('=' * 80)
        print('PROCESSING RUN STATUSES (NEW STEP-BASED APPROACH)')
        print('=' * 80)
        
        # Use the new aggregation method
        status_data = self.aggregate_run_results_from_steps(manifest)
        
        return status_data
    
    def process_and_log(self, output_format: str = 'json', write_to_db: bool = False, include_run_statuses: bool = False):
        """
        Main method to fetch artifacts, process data, and log results.
        
        Args:
            output_format: Output format ('json', 'csv', or 'dataframe')
            write_to_db: Whether to write results to database
            include_run_statuses: Whether to include run status analysis
            
        Returns:
            Dictionary with freshness data and optionally run status data
        """
        print('=' * 80)
        print('DBT FRESHNESS LOGGER')
        print('=' * 80)
        print(f'API Base: {self.api_base}')
        print(f'Account ID: {self.account_id}')
        print(f'Run ID: {self.run_id}')
        print('=' * 80)
        
        # Fetch artifacts
        print('\n[1/4] Fetching manifest...')
        manifest = self.fetch_manifest()
        print(f'✓ Manifest fetched successfully')
        
        print('\n[2/4] Fetching run results...')
        run_results = self.fetch_run_results()
        print(f'✓ Run results fetched successfully')
        
        # Process data
        print('\n[3/4] Processing nodes...')
        node_rows = self.process_nodes(manifest, run_results)
        print(f'✓ Processed {len(node_rows)} nodes')
        
        print('\n[4/4] Processing sources...')
        source_rows = self.process_sources(manifest)
        print(f'✓ Processed {len(source_rows)} sources')
        
        # Combine results
        all_rows = node_rows + source_rows
        
        print('\n' + '=' * 80)
        print(f'SUMMARY: Total of {len(all_rows)} items processed')
        print(f'  - Nodes: {len(node_rows)}')
        print(f'  - Sources: {len(source_rows)}')
        print('=' * 80)
        
        # Process run statuses if requested
        run_status_data = None
        if include_run_statuses:
            print('\n[BONUS] Processing run statuses...')
            run_status_data = self.process_run_statuses(manifest, run_results)
            print(f'✓ Processed {len(run_status_data["models"])} model runs')
        
        # Prepare response
        response = {
            'freshness_data': all_rows,
            'run_status_data': run_status_data
        }
        
        # Output results (just freshness for backward compatibility)
        if output_format == 'json':
            print('\n[OUTPUT] JSON Format:')
            print(json.dumps(all_rows, indent=2, default=str))
        elif output_format == 'csv':
            print('\n[OUTPUT] CSV Format:')
            self._output_csv(all_rows)
        elif output_format == 'dataframe':
            print('\n[OUTPUT] DataFrame Format:')
            self._output_dataframe(all_rows)
        
        # Write to database if requested
        if write_to_db:
            connection_string = os.getenv('DBT_CONNECTION_STRING')
            self.log_to_database(all_rows, connection_string)
        
        # Return freshness data for backward compatibility, or full response if run statuses included
        if include_run_statuses:
            return response
        return all_rows
    
    def _output_csv(self, rows: List[Dict[str, Any]]):
        """Output results in CSV format."""
        import csv
        import sys
        
        if not rows:
            print("No data to output")
            return
        
        fieldnames = ['unique_id', 'resource_type', 'name', 'is_freshness_configured',
                     'warn_after_count', 'warn_after_period', 
                     'error_after_count', 'error_after_period',
                     'build_after_count', 'build_after_period', 
                     'updates_on', 'freshness_config']
        
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in rows:
            # Convert dict to string for CSV
            csv_row = row.copy()
            csv_row['freshness_config'] = json.dumps(row.get('freshness_config'), default=str)
            writer.writerow(csv_row)
    
    def _output_dataframe(self, rows: List[Dict[str, Any]]):
        """Output results as pandas DataFrame."""
        try:
            import pandas as pd
            df = pd.DataFrame(rows)
            print(df.to_string())
            return df
        except ImportError:
            print("pandas not installed - cannot output as DataFrame")
            return None


def main():
    """Main function to run the freshness logger."""
    # Get configuration from environment variables
    api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com')
    api_key = os.environ['DBT_API_KEY']
    account_id = os.environ['DBT_ACCOUNT_ID']
    run_id = os.environ['DBT_RUN_ID']
    
    # Optional configuration
    output_format = os.getenv('OUTPUT_FORMAT', 'json')  # json, csv, or dataframe
    write_to_db = os.getenv('WRITE_TO_DB', 'false').lower() == 'true'
    
    # Create logger and process
    logger = DBTFreshnessLogger(api_base, api_key, account_id, run_id)
    results = logger.process_and_log(output_format=output_format, write_to_db=write_to_db)
    
    return results


if __name__ == "__main__":
    main()

