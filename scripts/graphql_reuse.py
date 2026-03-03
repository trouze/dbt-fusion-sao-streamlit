"""
GraphQL API script for fetching dbt Cloud model execution data and analyzing reuse rates.

NOTE: This functionality has been integrated into the Streamlit app!
      Run 'streamlit run app.py' and go to the
      "🔄 Model Reuse & SLO Analysis" tab for an interactive version.

This standalone script can still be used for command-line analysis.
"""

import os
import requests
import pandas as pd
import ast
import json

# Configuration - set via environment variables or modify here
API_KEY = os.getenv('DBT_CLOUD_API_KEY', '')
ENVIRONMENT_ID = int(os.getenv('DBT_CLOUD_ENVIRONMENT_ID', '0'))

url = 'https://metadata.cloud.getdbt.com/graphql'

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

def fetch_all_models(api_key: str, environment_id: int, page_size: int = 500):
    """
    Fetch all models from dbt Cloud GraphQL API with pagination.
    
    Args:
        api_key: dbt Cloud API key (user token or service account token)
        environment_id: dbt Cloud environment ID
        page_size: Number of models to fetch per page (default: 500)
    
    Returns:
        List of model nodes with execution info and config
    """
    headers = {'Authorization': f'Bearer {api_key}'}
    
    all_nodes = []
    has_next_page = True
    cursor = None
    page_count = 0
    
    while has_next_page:
        page_count += 1
        print(f"Fetching page {page_count}...")
        
        variables = {
            "environmentId": environment_id,
            "first": page_size
        }
        
        # Add cursor for subsequent pages
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
            
            # Check for GraphQL errors
            if "errors" in response_data:
                print(f"GraphQL errors: {response_data['errors']}")
                break
            
            models_data = response_data.get("data", {}).get("environment", {}).get("applied", {}).get("models", {})
            
            if not models_data:
                print("No models data found in response")
                break
            
            # Extract page info and edges
            page_info = models_data.get("pageInfo", {})
            edges = models_data.get("edges", [])
            
            if not edges:
                print("No more models found")
                break
            
            # Add nodes from this page
            nodes = [edge['node'] for edge in edges]
            all_nodes.extend(nodes)
            
            print(f"  Fetched {len(nodes)} models (total: {len(all_nodes)})")
            
            # Check if there are more pages
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            if has_next_page and not cursor:
                print("Warning: hasNextPage is True but no endCursor provided")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
        except KeyError as e:
            print(f"Error parsing response: {e}")
            break
    
    print(f"Completed! Fetched {len(all_nodes)} total models across {page_count} pages")
    return all_nodes


if __name__ == "__main__":
    # Check for required configuration
    if not API_KEY:
        print("Error: DBT_CLOUD_API_KEY environment variable not set")
        print("Set it with: export DBT_CLOUD_API_KEY='your_api_key_here'")
        exit(1)
    
    if ENVIRONMENT_ID == 0:
        print("Error: DBT_CLOUD_ENVIRONMENT_ID environment variable not set")
        print("Set it with: export DBT_CLOUD_ENVIRONMENT_ID='your_environment_id_here'")
        exit(1)
    
    print(f"Fetching models for environment {ENVIRONMENT_ID}...")
    all_nodes = fetch_all_models(API_KEY, ENVIRONMENT_ID)

    # Convert to DataFrame
    if all_nodes:
        df = pd.DataFrame(all_nodes)
        df['config'] = df['config'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else json.dumps(x))
        print(f"\nDataFrame shape: {df.shape}")
        print(df.head())
        
        # Calculate reuse statistics
        df['last_run_status'] = df['executionInfo'].apply(lambda x: x.get('lastRunStatus') if x else None)
        
        total_models = len(df)
        reused_models = len(df[df['last_run_status'] == 'reused'])
        reuse_pct = (reused_models / total_models * 100) if total_models > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"REUSE STATISTICS")
        print(f"{'='*60}")
        print(f"Total Models: {total_models:,}")
        print(f"Reused Models: {reused_models:,}")
        print(f"Reuse Percentage: {reuse_pct:.2f}%")
        print(f"{'='*60}\n")
        
        # Save to CSV
        output_file = f"model_reuse_analysis_env_{ENVIRONMENT_ID}.csv"
        df.to_csv(output_file, index=False)
        print(f"Data saved to: {output_file}")
        
    else:
        print("No data retrieved")
        df = pd.DataFrame()

    print("\n" + "="*60)
    print("NOTE: For more detailed analysis with visualizations,")
    print("use the Streamlit app: streamlit run app.py")
    print("Navigate to the '🔄 Model Reuse & SLO Analysis' tab")
    print("="*60)

