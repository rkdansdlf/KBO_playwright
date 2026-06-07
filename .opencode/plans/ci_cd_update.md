# CI/CD Workflow Update Plan

## Current State
The periodic_extras.yml workflow runs four separate audit commands:
1. python3 -m src.cli.monthly_pa_audit --year "$PREV_YEAR"
2. python3 -m src.cli.monthly_pa_audit --year "$YEAR"
3. python3 -m src.cli.monthly_team_audit --year "$PREV_YEAR"
4. python3 -m src.cli.monthly_team_audit --year "$YEAR"

## Desired State
Replace with two unified audit commands:
1. python3 -m src.cli.monthly_unified_audit --year "$PREV_YEAR"
2. python3 -m src.cli.monthly_unified_audit --year "$YEAR"

## Benefits
- Reduces workflow complexity from 4 steps to 2 steps
- Each unified audit step runs both PA formula and team stats checks
- Maintains same coverage: audits both previous year and current year
- Clearer intent in workflow naming

## Implementation
Change the step name from "Monthly Audits (PA Formula + Team Stats)" to "Monthly Unified Audit (PA Formula + Team Stats)"
Update the run command to use the unified audit script twice (for PREV_YEAR and YEAR)

## Verification
- Confirm the unified audit script exists and is executable
- Verify the workflow still runs audits for both years
- Check that both PA formula and team stats results are produced