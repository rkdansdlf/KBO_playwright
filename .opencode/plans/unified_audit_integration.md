# Unified Audit Integration Plan

## Objective
Integrate PA formula audit and team stats consistency audit into a single monthly unified audit job to improve efficiency and reduce scheduler complexity.

## Components to Modify
1. Create new unified audit script: `src/cli/monthly_unified_audit.py`
2. Update scheduler registration in `scripts/scheduler.py`
3. Update CI/CD workflow in `.github/workflows/periodic_extras.yml`
4. Maintain backward compatibility with existing individual audit scripts

## Implementation Details

### New Unified Audit Script
- Combines PA formula audit (via subprocess call to existing script) and team stats audit (direct function call)
- Provides CLI options: --year, --json, --pa-only, --team-only
- Saves separate JSON reports for each audit type
- Returns non-zero exit code if either audit fails
- Includes logging and error handling

### Scheduler Updates
- Replace separate PA formula and team stats job registrations with single unified job
- Maintain same schedule: 1st of every month at 03:00 KST
- Keep job ID and name descriptive: `crawl_monthly_unified_audit`, "Monthly Unified Audit"

### CI/CD Workflow Updates
- Replace individual audit steps in `periodic_extras.yml` with single unified audit step
- Run for both previous year and current year as before

### Backward Compatibility
- Keep existing individual audit scripts unchanged
- Preserve existing CLI interfaces for direct invocation
- No breaking changes to existing workflows or integrations

## Verification Steps
1. Manual testing of unified audit script with various CLI options
2. Verify scheduler registration shows correct job and timing
3. Test CI/CD workflow locally or via GitHub Actions
4. Confirm existing individual audit scripts still work
5. Run full test suite to ensure no regressions

## Files to Create/Modify
- Create: `src/cli/monthly_unified_audit.py`
- Modify: `scripts/scheduler.py` (replace job registrations)
- Modify: `.github/workflows/periodic_extras.yml` (replace audit steps)
