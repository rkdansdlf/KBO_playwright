# Scheduler Update Plan

## Changes to Make
1. Remove imports for `crawl_monthly_pa_audit_job` and `crawl_monthly_team_audit_job`
2. Add import for `crawl_monthly_unified_audit_job` from `src.cli.monthly_unified_audit`
3. Replace the two separate job registrations (PA audit at 03:00 and team audit at 04:00) with a single unified job registration
4. Keep the same schedule: 1st of every month at 03:00 KST
5. Update job ID and name appropriately

## Implementation Approach
- Keep the retired players job (Job 2.6) unchanged at 02:00 KST
- Replace Jobs 2.7 and 2.8 with a single Job 2.7: Monthly Unified Audit at 03:00 KST
- Shift subsequent job numbers down by one (Job 2.9 becomes Job 2.8, etc.)

## Benefits
- Reduces scheduler complexity by combining two related monthly audits
- Maintains same overall timing (starts at 03:00 instead of having gaps at 03:00 and 04:00)
- Simplifies monitoring and logging
- Easier to modify in the future if audit timing needs to change

## Risks
- None significant - unified audit maintains same functionality as separate audits
- Backward compatibility preserved through existing individual scripts
