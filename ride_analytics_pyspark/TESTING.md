# Testing & Validation

## Testing Approach

The pipeline was manually validated by modifying records in the source/Silver tables, executing the relevant pipeline, and comparing the resulting Gold records with the expected results.

No automated testing framework is currently implemented.

## Test Scenarios

| # | Test Scenario | Expected Result | Status |
|---|---|---|---|
| 1 | New trip record | New record is processed into Silver and reflected in the applicable Gold KPI | Passed |
| 2 | Existing trip update | Existing Silver record is updated when the incoming timestamp is newer | Passed |
| 3 | Older trip update | Older data does not overwrite a newer Silver record | Passed |
| 4 | Trip status change | Completed/cancelled/ongoing counts and completed-trip metrics are recalculated | Passed |
| 5 | New driver record | New driver appears in Driver Performance | Passed |
| 6 | Driver-related trip update | Affected driver's KPIs are recalculated | Passed |
| 7 | New customer record | New customer appears in Customer Analysis when applicable | Passed |
| 8 | Customer-related trip update | Affected customer's KPIs are recalculated | Passed |
| 9 | Payment amount update | Payment Analysis is recalculated from current Silver data | Passed |
| 10 | Payment status change | Payment Analysis is rebuilt and the new grouping is reflected | Passed |
| 11 | New payment/group | New payment aggregation group appears after full refresh | Passed |
| 12 | Gold full recalculation for Payment Analysis | All payment groups are recalculated from Silver | Passed |

## Trip Status Transition Tests

A key business-logic test was performed for trip status changes.

### Completed → Cancelled

Expected behavior:

```text
completed_trips  ↓ 1
cancelled_trips  ↑ 1
```

Completed-trip revenue and distance metrics are recalculated so that the cancelled trip is no longer included in completed-trip calculations.

**Result: Passed**

### Ongoing → Completed

Expected behavior:

```text
ongoing_trips    ↓ 1
completed_trips  ↑ 1
```

The trip becomes eligible for completed-trip revenue, fare, distance, duration, and fare-per-kilometer calculations.

**Result: Passed**

## Driver Performance Tests

Driver Performance was tested using changes to existing driver/trip records and a new driver with an associated trip.

Expected behavior:

- New driver is inserted into the Gold aggregation.
- Existing driver's KPIs are recalculated when an associated trip changes.
- Completed-trip financial and distance metrics only include completed trips.
- Status changes affect the corresponding trip counts and completed-trip metrics.

**Result: Passed**

## Customer Analysis Tests

Customer Analysis was tested using changes to existing customer/trip records.

Expected behavior:

- Total trips includes Completed, Cancelled, and Ongoing trips.
- Completed, Cancelled, and Ongoing counts are maintained separately.
- Spending, average fare, and distance calculations include completed trips only.
- Customer KPIs are recalculated for the affected customer.

**Result: Passed**

## Payment Analysis Tests

Payment Analysis uses full refresh.

A payment record was modified and the Gold table was rebuilt from the complete Silver payment dataset.

Expected behavior:

- Updated amount is reflected.
- Updated payment status is reflected.
- New payment groups are created when required.
- Previous aggregation groups do not retain stale counts because the entire aggregation is recalculated.

Example:

```text
Before:
Payment Status = Success

After:
Payment Status = Failed
```

The complete Payment Analysis is recalculated from Silver so that the `Success` and `Failed` groups both represent the current Silver data.

**Result: Passed**

## Validation Method

For each test:

```text
1. Modify test data
       ↓
2. Run the pipeline
       ↓
3. Query the affected Silver/Gold table
       ↓
4. Calculate the expected result
       ↓
5. Compare expected vs actual
       ↓
6. Mark test as Passed/Failed
```

## Test Conclusion

The implemented transformation, incremental processing, KPI recalculation, and Payment Analysis full-refresh logic were manually validated against the defined test scenarios.

All listed test scenarios passed during project validation.
