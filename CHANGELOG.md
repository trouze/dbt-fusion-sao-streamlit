# Changelog

## [2.10.0] - December 10, 2025

### Tab Reorganization 🔄

Reordered tabs to prioritize the most actionable analyses:
1. ⚙️ Configuration
2. 🗑️ Pre-SAO Waste Analysis (NEW - moved to #2)
3. 🔀 Job Overlap Analysis (moved to #3)
4. 📋 Model Details
5. 📈 Historical Trends
6. 💰 Cost Analysis

**Rationale**: Waste Analysis and Job Overlap are the most critical for SAO planning and should be easily accessible.

### Major Feature: Pre-SAO Waste Analysis 🗑️

Added comprehensive waste analysis to identify models that run but produce minimal or no changes - the primary use case for State-Aware Orchestration (SAO).

#### New Pre-SAO Waste Analysis Tab

**Enhanced Historical Job Support:**
- Now fetches runs by date range FIRST, then enriches with job metadata
- Captures data from deleted/archived jobs (not just current jobs)
- Shows which jobs are active vs. historical/deleted
- Job statistics breakdown with inactive job details

**Real-Time Progress Updates:**
- Progress callbacks during API pagination
- Shows page number and count during fetching
- Separate progress for runs and job metadata
- Better user feedback during long operations

**Waste Detection:**
- **View Models**: Automatically excluded from cost calculations (don't drive warehouse costs)
- **Zero-Change Models**: Table models that rebuild with 0 rows affected
- **Low-Change Incrementals**: Incremental models with very few new rows (configurable threshold)
- Parses `adapter_response.rows_affected` from `run_results.json` for accurate row counts

**Key Metrics:**
- Total Wasted Cost: Money spent on zero/low-change executions
- Wasted Executions: Count of wasteful model runs
- Average Waste per Run: Per-job waste tracking
- Annual Savings Estimate: Projected yearly savings with SAO

**Visualizations:**
- 📊 Waste by Category (pie chart): Zero changes vs. low changes
- 📊 Waste by Model Type (bar chart): Breakdown by materialization
- 📈 Waste Over Time (line chart): Trend analysis across runs
- 🔥 Top 20 Wasters (horizontal bar): Models with highest wasted cost

**Detailed Analysis:**
- Model-level breakdown with waste category, count, average rows changed
- Job-level aggregation showing waste per run
- Priority recommendations for SAO implementation
- ROI estimates based on historical waste patterns

**Configuration Options:**
- Warehouse size and cost per hour (for cost calculations)
- Minimum rows threshold (for "low change" detection)
- Job source (all jobs or specific job)
- Date range and max runs to analyze

#### Enhanced Data Collection (log_freshness.py)

**Extended Model Data:**
- Added `rows_affected` extraction from `adapter_response`
- Added `materialization` type (view, table, incremental, etc.)
- Added `adapter_response` full object for detailed analysis
- Added `message` field for additional context

**Multi-Step Aggregation:**
- Properly aggregates row counts across multiple dbt steps
- Preserves row change information for successful runs
- Handles both single and multi-step run processing

#### Use Cases

**Before SAO Implementation:**
- Quantify potential savings to justify SAO adoption
- Identify which jobs/models waste the most compute
- Prioritize SAO rollout based on ROI

**After Partial SAO:**
- Identify remaining non-SAO jobs with waste
- Track waste reduction over time
- Validate SAO effectiveness

**Continuous Optimization:**
- Monitor for new waste patterns
- Identify models that should have freshness checks
- Track annual cost impact

---

## [2.9.0] - November 28, 2025

### Major Feature: Enhanced SAO Adoption Analysis 🎯

Added comprehensive State-Aware Orchestration (SAO) adoption analysis to the **Environment Overview** tab to help organizations understand and optimize their SAO usage.

#### New SAO Adoption Section

**Overall Metrics & Visualizations:**
- Total Jobs, SAO-Enabled Jobs, and Non-SAO Jobs metrics
- Dual visualizations: Donut chart and bar chart showing adoption split
- Scheduled jobs-specific analysis with dedicated pie chart and metrics

**Job Type Breakdown (New!):**
- Grouped bar chart comparing SAO adoption across CI, Merge, Scheduled, and Other job types
- Summary table with counts and percentages by job type
- Smart alerts for low adoption in critical job types (CI and Merge jobs)

**Freshness Configuration Coverage (New!):**
- 4 key metrics tracking configuration patterns:
  - ✅ Both SAO & Freshness (optimal)
  - ⚠️ SAO without Freshness (may not reuse effectively)
  - 💡 Freshness without SAO (missing optimization)
  - ❌ Neither (basic configuration)
- Color-coded bar chart showing configuration distribution
- Expandable lists of jobs with problematic configurations
- Helps identify misconfigured jobs that won't benefit from SAO

**Top Opportunities Analysis (New!):**
- Intelligent ranking of non-SAO jobs by potential impact
- Impact Score calculation: `Recent Runs × Avg Duration (minutes)`
- Priority levels: High (CI/Merge), Medium (Scheduled), Low (Other)
- Top 10 table with color-coded text by priority (red/orange/gray)
- ROI calculator showing estimated time savings if SAO enabled
- Helps prioritize which jobs to enable SAO on first

**All Jobs List:**
- Expandable table showing all jobs with SAO status (✅/❌)
- Includes Job ID, Name, Type, and Environment ID
- Sorted by SAO status then job name

#### Visual Enhancements
- Consistent color scheme: Green (#10b981) for SAO-enabled, Gray (#6b7280) for non-SAO
- Horizontal legends positioned at top of charts
- All bar charts show value labels for quick reading
- Standardized chart heights (400px) for visual consistency
- Responsive layouts with `use_container_width=True`

### Model Details Tab Enhancements 📋

**Package Column Extraction (New!):**
- Automatically extracts package name from `unique_id` field
- Package column appears right after Resource Type
- Format: `model.{package}.{model_name}` → extracts package
- Works universally without hardcoding project names

**Enhanced Freshness Coverage Table (New!):**
- New table: "📦 Freshness Coverage by Package & Resource Type"
- Breaks down coverage by both package AND resource type
- **Packages automatically sorted by size** (main project appears first)
- Shows exactly which packages have freshness configs
- Expected: ~100% for main project, lower for imported packages
- Makes it easy to identify configuration gaps

**Project/Package Filtering:**
- Multiselect filter to show models from specific projects/packages
- Helps focus on main project vs. external dependencies
- Default: All projects selected
- Now works with automatically extracted package names

**Group by Project/Package:**
- Optional checkbox to group results by project
- Each project displayed in expandable section with:
  - Project name and item count
  - Project-specific metrics (Total Items, With Freshness, Coverage %)
  - Filtered table for that project
- All sections expanded by default for easy scanning
- Makes it easy to compare freshness coverage across projects

### Cost Analysis Tab Improvements 💰

**Removed:**
- Cost Distribution by Status chart (redundant - only showed that reused = $0)

### Bug Fixes & Polish 🔧

**Top Opportunities Table:**
- Changed from background highlighting to colored text with bold font
- Much more readable: Red (High), Orange (Medium), Gray (Low)
- Rounded decimal places from 6+ to 2 digits

**All Files:**
- Continued removal of outdated "log parsing" documentation
- Updated to reflect step-based `run_results.json` approach throughout

### Technical Notes

- SAO detection uses `cost_optimization_features` array check for `'state_aware_orchestration'`
- Freshness config detection analyzes job `execute_steps` for freshness-related commands
- Project filtering uses pandas value_counts for intelligent detection
- All new features work across all customer environments without configuration

---

## [2.8.6] - November 26, 2025

### Critical Bug Fix: API Limit Pagination 📊

#### Problem
API calls were failing with **"Limit must not exceed 100"** error when trying to fetch more than 100 runs:
```
Response: {"status": {"code": 400, "is_success": false, 
          "user_message": "Limit must not exceed 100", ...}
```

#### Root Cause
The code was calculating `fetch_limit = min(200, max_runs * 3)` to account for date filtering, which could result in requests like:
- User sets slider to 54 runs
- Code calculates: `54 * 3 = 162` runs
- API request: `limit=162` ❌ (exceeds max of 100)

#### Solution
Implemented **automatic pagination** when limit exceeds 100:
- Split large requests into multiple API calls
- Each call limited to max 100 runs
- Uses `offset` parameter to fetch subsequent pages
- Works for both single and multiple status filters

#### Code Changes
```python
# Before
params = {'limit': limit, ...}  # Could be > 100 ❌

# After
API_MAX_LIMIT = 100
while runs_to_fetch > 0:
    page_limit = min(runs_to_fetch, API_MAX_LIMIT)
    params = {'limit': page_limit, 'offset': offset, ...}
    # Fetch page, increment offset, repeat ✅
```

#### Also Fixed
- Enhanced error logging now shows API response body
- Made it clear that `environment_id` is not needed with `job_definition_id`

**Result:** ✅ Can now fetch up to 240 runs (80 * 3) across multiple API calls!

---

## [2.8.5] - November 26, 2025

### Critical Bug Fix: Trailing Slash in API Base URL 🔧

#### Problem
400 errors were occurring due to **double slashes** in API URLs:
```
https://vu491.us1.dbt.com//api/v2/...  ❌ (double slash)
                        ^^
```

#### Root Cause
When users entered the API base URL with a trailing slash:
- Input: `https://vu491.us1.dbt.com/`
- Code adds: `/api/v2/...`
- Result: `https://vu491.us1.dbt.com//api/v2/...` ❌

#### Solution
- **Auto-strip trailing slashes** when saving configuration
- Updated help text to show correct format examples
- Now handles URLs with or without trailing slashes

#### Code Change
```python
# Before
'api_base': api_base,  # Could have trailing slash

# After
'api_base': api_base.rstrip('/'),  # Always clean
```

#### Also Improved
- **Better error messages** when status API calls fail
- Surfaces errors to user instead of failing silently
- Shows which specific status failed and why

**Result:** ✅ Works with any URL format! No more double slash errors!

---

## [2.8.4] - November 26, 2025

### Critical Fix: Multiple Status API Calls 🚀

#### Fixed: Proper Multi-Status Filtering

**The Issue:**
- dbt Cloud API only accepts **one `status` value per request**
- Passing multiple statuses like `?status=10&status=20` causes 400 errors
- Previous fix (v2.8.3) fetched all runs and filtered client-side (inefficient)

**The Solution:**
- **Make separate API calls** for each status
- Combine results and remove duplicates
- Sort by most recent
- Much more efficient than fetching all runs!

**How It Works:**
```python
# For Success + Error:
# Call 1: GET /runs/?status=10&environment_id=672
# Call 2: GET /runs/?status=20&environment_id=672
# Combine results → return most recent runs
```

**Example:**
- User selects: Success (10) + Error (20)
- Makes 2 API calls: one for status=10, one for status=20
- Combines and returns up to `limit` most recent runs
- ✅ Fast, efficient, accurate!

**Also Fixed:**
- Now using `environment_id` parameter instead of `project_id`
- Both work, but `environment_id` is more semantically correct
- Updated all `get_job_runs()` calls throughout the app

**Affected Tabs:**
- ✅ Model Details
- ✅ Historical Trends
- ✅ Cost Analysis

**Performance:**
- Much faster than fetching all runs
- Only fetches what's needed
- Minimal API overhead (typically 1-3 calls)

---

## [2.8.3] - November 26, 2025

### Critical Bug Fix 🚨

#### Fixed: 400 Bad Request Error with Multiple Status Filters

**Error:**
```
400 Client Error: Bad Request for url: 
https://cloud.getdbt.com/api/v2/.../runs/?...&status=10&status=20
```

**Root Cause:**
- dbt Cloud API doesn't support multiple `status` query parameters in a single request
- When selecting multiple run statuses (e.g., Success + Error), the API call failed

**Solution (Temporary - improved in v2.8.4):**
- Changed from server-side to client-side filtering
- Fetch all runs without status filter
- Filter by status after receiving the data

**Result:** ✅ Multi-status filtering works (but improved in v2.8.4)

---

## [2.8.2] - November 26, 2025

### Bug Fixes & Improvements 🐛

#### Historical Trends Tab - Fixed Run Filtering Logic

**Problem:** 
- Runs were being limited by the slider BEFORE date filtering
- This caused confusing messages like "Found 12 runs" followed by "No runs found in date range"
- When increasing the slider, more runs would appear because more were fetched before the date filter

**Solution:**
- Now fetches 3x the slider limit (up to 200 runs) from API
- Applies date filtering first
- Counts total runs in date range
- Then limits to slider value
- Shows clear message: "Found X runs in date range, analyzing Y most recent (limited by slider)"

**Example Messages:**
- Before: `✅ Found 12 runs. Analyzing in parallel...` (confusing)
- After: `✅ Found 45 runs in date range, analyzing 12 most recent (limited by slider)...` (clear!)

#### Slider Max Value Increased
- **Historical Trends**: Max runs increased from 50 → 80
- Allows analysis of larger time periods
- Still benefits from parallel processing

#### Fixed Column Mismatch Error
**Error:** `ValueError: Length mismatch: Expected axis has 13 elements, new values have 10 elements`

**Cause:** Added job_id, job_name, run_status_name columns but didn't update column name mapping

**Fix:** Updated column names list to include:
- 'Job ID'
- 'Job Name'  
- 'Run Status'

Now the Detailed Run Breakdown table displays correctly with all columns!

### Technical Details

#### Fetch Strategy
```python
# Old: Limited before date filtering
limit=max_runs  # Could be 10

# New: Fetch more to account for filtering
fetch_limit = min(200, max_runs * 3)  # Could be 30-200
```

#### Filtering Flow
1. Fetch runs (3x slider or 200 max)
2. **Filter by date range** ← Now happens before limiting!
3. Filter by SAO (if enabled)
4. Count total
5. Limit to slider max
6. Show accurate message

#### Benefits
- ✅ No more confusing "found but not found" messages
- ✅ Date filtering works correctly regardless of slider value
- ✅ Clear visibility into how many runs exist vs. being analyzed
- ✅ Slider tooltip updated: "Maximum number of runs to analyze (from total found)"

---

## [2.8.1] - November 26, 2025

### UI & UX Enhancements 🎨

#### Status Distribution Chart - Better Colors
- **success**: Now green (#22c55e) instead of default
- **error**: Now red (#ef4444) for clear visibility
- **reused**: Now light blue (#60a5fa) for differentiation
- **skipped**: Now grey (#9ca3af) for clarity
- **Impact**: Much easier to interpret status distributions at a glance

#### Detailed Run Breakdown - Enhanced Context
**NEW: Job Information in Run Table**
- Added **Job ID** column
- Added **Job Name** column  
- Added **Run Status** column (success, error, cancelled)
- **Impact**: Better traceability from runs back to source jobs

#### Job Overlap Analysis - Smarter Handling
**NEW: Currently Running Jobs**
- Now **fetches latest successful run** instead of skipping
- Shows count of running jobs using successful run data
- **Before**: "⚠️ 2 jobs skipped: 1 currently running"
- **After**: "ℹ️ 1 job(s) currently running - using latest successful run"
- **Impact**: More complete analysis, no waiting for jobs to finish

**NEW: Jobs Summary Table**
- Now shows table of all analyzed jobs **even when no overlap found**
- Displays: Job Name, Job ID, Run ID, Model Count
- Sorted by model count (descending)
- **Impact**: Always see what was analyzed, not just problems

### Technical Implementation

#### `process_single_run()` Function
- Added `job_id`, `job_name`, `run_status` parameters
- Returns job context with model data
- Enables richer analysis across tabs

#### Job Overlap Logic
- Removed skip logic for currently running jobs
- Fetches latest successful run as fallback
- Only skips jobs with no successful run history

#### Status Colors Map
```python
status_colors = {
    'success': '#22c55e',  # green
    'error': '#ef4444',    # red
    'reused': '#60a5fa',   # light blue
    'skipped': '#9ca3af'   # grey
}
```

---

## [2.8.0] - November 26, 2025

### 🎯 Major Overhaul - Step-Based Run Analysis & Accurate Model Counting

#### Revolutionary New Approach: Step-Based Run Processing

**Problem Identified:**
- Jobs with multiple steps (e.g., `dbt build` + `dbt compile`) were causing inaccurate model counts
- The final `dbt compile` step would compile ALL models and show them as "success" with near-zero execution time
- Heuristic-based detection was creating false positives and confusion

**Solution Implemented:**
- **Intelligent Step Filtering**: Only analyze `dbt run` and `dbt build` commands
- **Multi-Step Aggregation**: Fetch and combine `run_results.json` from all relevant steps
- **Zero Guesswork**: No more heuristics - just accurate data from actual run/build steps

#### Technical Implementation (`log_freshness.py`)

##### New Methods
1. **`fetch_run_steps()`**
   - Fetches run steps from API with `include_related=["run_steps"]`
   - Filters to only steps containing "dbt run" or "dbt build"
   - Skips auxiliary commands (deps, compile, docs, source freshness, etc.)
   - Returns list of relevant step indices

2. **`fetch_run_results(step)`**
   - Enhanced to accept optional `step` parameter
   - Fetches `run_results.json?step={N}` for specific steps
   - Backwards compatible (no step = default behavior)

3. **`aggregate_run_results_from_steps()`**
   - Orchestrates the new step-based approach
   - Fetches run_results.json from all relevant steps
   - Aggregates model statuses across steps
   - Handles duplicates (model in multiple steps)
   - Falls back gracefully if no relevant steps found

4. **`_aggregate_results()`**
   - Merges results from multiple steps
   - Tracks which steps each model appeared in
   - Prioritizes error > success > reused for status resolution
   - Prints detailed diagnostics

5. **`_process_single_run_results()`**
   - Clean fallback method for single run_results.json
   - No log parsing, no heuristics
   - Used when step-based approach fails

##### Updated Methods
- **`process_run_statuses()`**: Now a thin wrapper that calls `aggregate_run_results_from_steps()`
- **Removed**: All log parsing logic, fuzzy detection heuristics, execution time guessing

#### Application Updates (`streamlit_freshness_app.py`)

##### Run Status Filtering 🎚️
**NEW: Filter runs by completion status across all tabs**

- **Filter Options**: Success, Error, Cancelled
- **Default**: Success only (most common use case)
- **Multi-select**: Analyze multiple statuses simultaneously
- **Applied To**: Model Details, Historical Trends, Cost Analysis

##### Tab-by-Tab Enhancements

**📋 Model Details**
- Added run status filtering (Success/Error/Cancelled)
- Uses new step-based approach for accurate counts
- Shows run status in info message
- Better error messages with status context

**📈 Historical Trends**
- Run status multi-select filter
- Step-based analysis for all runs
- Removed "Data Quality Notice" warning (no longer needed!)
- Accurate model counts regardless of job structure
- Status codes properly passed to API

**💰 Cost Analysis**
- Run status filtering support
- Accurate cost calculations using step-based data
- No more inflated model counts from compile steps
- True savings calculations

**🔀 Job Overlap Analysis**
- Updated to use step-based artifact fetching
- Only counts models from run/build steps
- Avoids duplicate counting from compile steps
- More accurate overlap detection

#### API Enhancements

**`get_job_runs()` Function**
- NEW `status` parameter: List of status codes to filter by
- Supports multiple statuses via API (10=success, 20=error, 30=cancelled)
- Backwards compatible (status=None = all statuses)
- Proper type hints with `List[int]`

#### What This Fixes

1. **✅ Accurate Model Counts**
   - Before: Job with 1 model executed showed 1736 "success" (from dbt compile)
   - After: Shows exactly 1 model executed, correct status breakdown

2. **✅ No More False Positives**
   - Eliminated all heuristic-based guessing
   - No execution time thresholds
   - No log parsing failures

3. **✅ Multi-Step Job Support**
   - Correctly handles jobs with multiple dbt commands
   - Aggregates data from all relevant steps
   - Ignores auxiliary commands (compile, docs, etc.)

4. **✅ Better User Experience**
   - Removed confusing "Data Quality Notice" warnings
   - Clear, accurate metrics
   - Trustworthy data for decision-making

5. **✅ Flexible Status Analysis**
   - Analyze successful runs, errors, or cancelled runs
   - Compare patterns across status types
   - Debug failed runs more effectively

#### Breaking Changes
- None! The changes are backwards compatible and transparent to users

#### Performance
- Minimal impact (1-2 additional API calls per run to fetch steps)
- Parallel processing still active in Historical Trends
- Smart fallback to default run_results.json if needed

#### Example: Step Filtering

**Skipped Steps:**
- `Clone git repository`
- `Create profile from connection`
- `Invoke dbt with dbt deps`
- `Invoke dbt with dbt source freshness`
- `Invoke dbt with Generation of docs`
- `Invoke dbt with dbt compile` ⚠️ (this was the culprit!)

**Analyzed Steps:**
- ✅ `Invoke dbt with dbt build --exclude package:dbt_project_evaluator`
- ✅ `Invoke dbt with dbt run --select tag:hourly`

#### Migration Notes
- No user action required
- Existing functionality unchanged
- Data will be more accurate automatically
- Previous inaccuracies will self-correct on next analysis

---

## [2.7.0] - November 25, 2025

### Major Features - SAO Detection & Filtering 🎯

#### State-Aware Orchestration (SAO) Filtering
- **NEW: Automatic SAO detection** from job configuration
- **NEW: "SAO Jobs Only" filter** in Historical Trends and Cost Analysis tabs
- **Smart filtering** excludes jobs without SAO for accurate reuse metrics
- **Default: Enabled** - filters to SAO-only by default (best practice)

#### Key Benefits
- ✅ **Accurate reuse metrics** - No more 0% reuse from non-SAO jobs
- ✅ **Meaningful cost savings** - Only shows real SAO financial impact
- ✅ **Better trends** - Track true SAO performance over time
- ✅ **Clear feedback** - Shows exactly what was filtered and why

#### Technical Implementation
- `check_job_has_sao()` - Detects SAO from `cost_optimization_features` field
- `filter_runs_by_sao()` - Separates SAO and non-SAO runs
- Integrated into existing filtering pipeline (date → job type → SAO)
- Zero performance impact (uses existing API data)

#### User Interface
- **Historical Trends**: "SAO Jobs Only" checkbox (default: checked)
- **Cost Analysis**: "SAO Jobs Only" checkbox (default: checked)
- **Info messages**: Shows count of filtered runs
- **Warnings**: Alerts if no SAO jobs found with helpful guidance

#### Use Cases Enabled
1. **Pure SAO environments** - Clean, accurate metrics
2. **Mixed environments** - Focus on SAO performance
3. **Migration analysis** - Compare before/after SAO adoption
4. **Troubleshooting** - Identify jobs missing SAO

### Bug Fixes & Improvements

#### Run Status Detection Enhancements
- **Improved fallback detection** for reused models
  - Models with execution_time < 0.1s automatically marked as "reused"
  - Only applies when log parsing succeeds but model not in logs
  - Addresses issue where all models showed as "success" incorrectly
- **Enhanced diagnostics** in log processing
  - Shows reuse detection statistics
  - Reports average and median execution times
  - Helps identify when detection fails
- **Data quality warnings** in UI
  - Automatically detects suspicious runs (95%+ success, median < 0.05s)
  - Warns users about potential log parsing failures
  - Explains that counts are still accurate

#### Job Overlap Analysis Improvements
- **Proactive filtering** of currently running jobs
  - Checks job status before attempting analysis
  - Prevents errors from jobs with no successful run
  - Status codes: 1=Queued, 2=Starting, 3=Running
- **Categorized skip tracking**
  - Separately counts: running, never succeeded, other
  - Detailed breakdown in metrics display
  - Clear explanation of why jobs excluded
- **Enhanced user feedback**
  - Info box showing skip categories and counts
  - Actionable guidance (e.g., "re-run later")
  - Updated documentation explaining exclusions

## [2.6.0] - November 25, 2025

### Major Enhancements - Environment-First Workflow 🎯

#### Configuration Page Reorganization
- **Removed Job ID from main configuration** - No longer required for core workflows
- **Reorganized layout**: Configuration inputs at top, tab descriptions below
- **Streamlined fields**: 
  - Credentials: API URL, API Key, Account ID
  - Environment Settings: Environment ID, Project ID (optional)
- **Improved help text**: Better guidance on what each field is for
- **Summary first, details below**: Configuration inputs take priority

#### 📋 Model Details - Environment-First Approach
- **NEW: Environment (Latest) mode** - Get latest run from environment automatically
  - Fetches all jobs in environment
  - Filters by selected job types
  - Uses latest successful run across filtered jobs
- **Flexible Source Selection**:
  - Environment (Latest): Analyze most recent run from environment
  - Specific Job ID: Get latest run from a particular job
  - Specific Run ID: Analyze a specific run directly
- **NEW: Job Type Filtering**
  - Filter to ci, merge, scheduled, or other job types
  - Default: scheduled jobs only
  - Applies to Environment mode

#### 📈 Historical Trends - Environment-Wide Analysis
- **NEW: All Jobs in Environment mode** - Analyze all jobs across time range
  - Fetches runs from all jobs in environment
  - Filters by job types (ci, merge, scheduled, other)
  - Aggregates data across entire environment
- **Flexible Analysis**:
  - All Jobs in Environment: Environment-wide trends
  - Specific Job ID: Single job analysis (legacy mode)
- **Job Type Filtering**: Filter which job types to analyze
- **Better performance tracking**: See trends across your entire environment

#### 🔀 Job Overlap Analysis - Job Type Filtering
- **NEW: Job type filtering**
  - Filter overlap analysis by job types
  - Default: scheduled jobs only
  - Focus on production jobs or analyze all types
- **Improved messaging**: Shows which job types were analyzed
- **Better targeting**: Focus on specific job categories

### Technical Improvements
- **NEW: Helper functions**:
  - `determine_job_type(triggers)`: Detect job type from trigger config
  - `filter_jobs_by_type(jobs, job_types)`: Filter job lists by type
- **Job Type Detection Logic**:
  - `scheduled`: Has schedule trigger
  - `ci`: Has webhook trigger + custom_branch_only=True
  - `merge`: Has webhook trigger + custom_branch_only=False
  - `other`: Everything else
- **Session state cleanup**: Removed `job_id` from default config
- **Error handling**: Better validation for environment vs job ID modes
- **API efficiency**: Smarter fetching based on source mode

### Breaking Changes
- **Job ID removed from Configuration page**: Use per-tab job specification instead
- **Environment ID now primary**: Required for Environment-wide features
- **Job type filtering**: Default changed to "scheduled" only (was "all")

### Migration Guide
- **Before**: Set Job ID in Configuration → use for all tabs
- **After**: Set Environment ID in Configuration → specify job/run per tab as needed
- **Benefit**: More flexible, supports environment-wide analysis, clearer workflow

## [2.5.0] - November 24, 2025

### New Features 🎉

#### 🔀 Job Overlap Analysis Tab (NEW!)
- **Pre-SAO Optimization**: Identify models being run by multiple jobs
- **Job Inventory**: Analyze all jobs in environment
- **Overlap Detection**: Find models executed in multiple jobs
- **Overlap Ranking**: Sort by most duplicated models (bar chart)
- **Waste Calculation**: Quantify redundant executions
- **Job-to-Job Matrix**: Visual overlap matrix (for smaller job sets)
- **Detailed Reports**: Expandable sections for each overlapping model
- **Recommendations**: Priority-based suggestions for consolidation
- **Export Options**: Download mappings and overlap reports as JSON

#### Key Capabilities
- Identifies unnecessary model duplication across jobs
- Helps prioritize job consolidation efforts
- Quantifies compute waste before SAO adoption
- Provides actionable recommendations based on overlap severity

#### Use Cases
- **Pre-SAO environments**: Find redundant model executions
- **Job consolidation**: Identify which jobs to merge
- **Cost optimization**: Eliminate unnecessary compute
- **Audit efficiency**: Validate job structure

## [2.4.1] - November 24, 2025

### Bug Fixes & Improvements

#### 💰 Cost Analysis - Corrected Calculation Logic
- **FIXED: Savings calculation now accurate**
  - For reused models, uses average execution time from successful runs
  - Previously used reused time (~0s) which was incorrect
  - Now correctly calculates: "What would this model have cost if it ran?"
- **Removed: Savings Breakdown by Status** (redundant visualization)
- **Improved: Cost Distribution** now includes execution counts and percentages
- **Updated: Documentation** to reflect correct calculation methodology

#### How It Works Now
```python
# When a model runs (status=success):
cost = (execution_time / 3600) × cost_per_hour

# When a model is reused:
average_success_time = mean(all successful runs for this model)
cost = $0 (didn't execute)
savings = (average_success_time / 3600) × cost_per_hour
```

## [2.4.0] - November 24, 2025

### New Features 🎉

#### 💰 Cost Analysis Tab (NEW!)
- **Cost Estimation**: Calculate compute costs based on execution time and warehouse type
- **Savings Calculation**: Show money saved from model reuse
- **ROI Analysis**: Measure return on investment from freshness configurations
- **Cost Trends**: Visualize cost patterns over time
- **Top Expensive Models**: Identify the 20 most costly models
- **Model-Level Analysis**: Detailed cost breakdown per model
- **Warehouse Configuration**: Support for 8 warehouse sizes with customizable costs
- **Multi-Currency**: Support for USD, EUR, GBP, CAD, AUD
- **Export Options**: Download run summaries and model-level cost data

#### 📈 Historical Trends Enhancements
- **Reuse Rate Trending**: Track reuse percentage over time with trend line
- **Goal Visualization**: 30% reuse goal line on charts
- **Trend Statistics**: Average, peak, and lowest reuse rates
- **Trend Direction**: Calculate if reuse is improving or declining
- **Better Metrics**: Recent vs older period comparison

### Features
- Added 5th tab: "💰 Cost Analysis"
- Parallel processing for cost analysis (same as Historical Trends)
- Interactive cost trend charts with dual axes (cost + savings)
- Warehouse cost defaults based on Snowflake pricing
- Configurable cost per hour for custom pricing
- Date range filtering for cost analysis
- Run-level and model-level cost breakdowns

### Technical Improvements
- Enhanced `show_run_status_analysis()` with reuse rate trending
- New `show_cost_analysis()` function with comprehensive financial analysis
- Cost calculations: `(execution_time / 3600) × cost_per_hour`
- Savings calculations: reused models cost $0
- ROI formula: `(savings / total_cost) × 100`

## [2.3.0] - November 24, 2025

### Major Reorganization 🎯
- **Reduced from 5 tabs to 4** for clearer navigation
- **Environment Overview is now the primary dashboard** (Tab 1)
- **Removed Summary Statistics tab** (functionality merged into Environment Overview)
- **Renamed tabs** for clarity:
  - "Model Reuse & SLO Analysis" → "Environment Overview"
  - "Freshness Details" → "Model Details"
  - "Run Status Details" → "Historical Trends"

### Package Filtering ✅
- **NEW: `filter_to_main_project()` function**
  - Filters out 8 common dbt packages (dbt_project_evaluator, dbt_artifacts, etc.)
  - Ensures consistent model counts across all tabs
  - Focus on models you actually control
- Applied to all tabs for consistency
- **Fixes model count discrepancies**:
  - Before: Environment Overview (1620) vs Others (~2548 with packages)
  - After: All tabs show consistent counts for main project only

### UX Improvements
- **Enhanced Configuration tab**
  - New expandable "About the Analysis Tabs" section
  - Comprehensive descriptions of each tab's purpose
  - Better guidance for new users
  - Clearer data source information (real-time vs point-in-time)
- **Better messaging** throughout the app
- **Improved tooltips** and help text

### Documentation
- NEW: `REORGANIZATION_SUMMARY.md` - Complete reorganization guide
- Includes future enhancement suggestions
- User feedback mechanisms
- Success metrics

## [2.2.0] - November 24, 2025

### Performance Improvements
- **⚡ Parallel Processing for Run Status Analysis**
  - Up to 10 runs processed simultaneously using ThreadPoolExecutor
  - **5-10x faster** analysis times
  - 20 runs now take ~10-15 seconds (vs 40-100 seconds before)
  - Real-time progress tracking for parallel execution
  - Improved error handling with failed run collection

### UX Improvements
- **Configuration Tab Now First**
  - Moved Configuration tab to first position for better onboarding
  - Added expandable "About the Analysis Tabs" section
  - Comprehensive descriptions of all tabs with use cases
  - Helpful tips when configuration is saved
  - Better warnings when not configured

### Bug Fixes
- Fixed timezone issue in Model Reuse & SLO Analysis tab
  - Resolved "Cannot subtract tz-naive and tz-aware datetime-like objects" error
  - Properly handles UTC timestamps from GraphQL API
- Fixed Streamlit deprecation warnings
  - Replaced `use_container_width` with `width` parameter for dataframes and buttons
  - Updated all 16 instances throughout the app
  - Note: `st.plotly_chart` still uses `use_container_width=True` (not `width`)
  - Now compatible with Streamlit 2025+ API

### Updated Recommendations
- Increased recommended runs for analysis from 5-10 to 20-50
- Updated date ranges from 7 days to 14-30 days
- Revised performance expectations in documentation

## [2.1.0] - November 24, 2025

### Added
- **New Tab: Model Reuse & SLO Analysis** 🔄
  - GraphQL-based environment-wide analysis
  - Real-time model execution status and reuse tracking
  - SLO compliance table with filtering capabilities
  - Interactive visualizations:
    - Model distribution by build_after configuration (bar & pie charts)
    - Status distribution (reused vs success vs error)
    - Total reuse percentage with goal comparison
  - Automatic insights and recommendations
  - Export full analysis to CSV

- **Enhanced Configuration**
  - Added Environment ID field for GraphQL queries
  - Support for both REST API v2 and GraphQL Metadata API
  - Environment ID stored in session state

- **Standalone Script**
  - `api_graphql_reused.py` can now be run independently via command line
  - Environment variable configuration support
  - CSV export of reuse statistics

### Changed
- Updated README with comprehensive documentation of new tab
- Added GraphQL API documentation and usage examples
- Included new workflow examples for SLO audits
- Enhanced troubleshooting section

### Technical Details
- Integrated GraphQL pagination for large model sets
- Added `fetch_all_models_graphql()` function for efficient data fetching
- Implemented SLO calculation logic based on build_after configuration
- Added comprehensive error handling for GraphQL responses

### Features Implemented (from TODO list)
1. ✅ SLO Compliance Table with real-time execution data
2. ✅ Model Distribution by Build After Configuration (bar + pie charts)
3. ✅ Distribution of Statuses visualization (reused vs success vs error)
4. ✅ Current Total Reuse Percentage metric with goal tracking
5. ✅ Automatic insights and recommendations engine
6. ✅ Advanced filtering (status, SLO compliance, freshness config)

### API Support
- REST API v2: For freshness and run status analysis
- GraphQL Metadata API: For environment-wide reuse and SLO analysis

---

## [2.0.0] - Previous Release

- Initial Streamlit app with 4 tabs
- Summary Statistics dashboard
- Freshness Details analysis
- Run Status Details with log parsing
- Configuration management

