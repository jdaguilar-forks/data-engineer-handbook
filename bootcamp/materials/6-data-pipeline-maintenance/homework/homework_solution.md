# Week 5 Data Pipeline Maintenance

## Team Ownership

Our data engineering team consists of four members, they are:

- John
- James
- Jill
- Davis


and we will distribute ownership as follows:

| Pipeline                          | Primary Owner | Secondary Owner |
|-----------------------------------|---------------|-----------------|
| Unit-level Profit (Experiments)   | John          | James           |
| Aggregate Profit (Investors)      | James         | Jill            |
| Aggregate Growth (Investors)      | Jill          | Davis           |
| Daily Growth (Experiments)        | Davis         | John            |
| Aggregate Engagement (Investors)  | John          | Davis           |

## On-Call Schedule

To ensure fair coverage, we will use a rotating on-call schedule:

- Each engineer is on-call for one week at a time.

- Rotation happens every Monday at 9 AM.

- If an engineer is unavailable due to vacation or holidays, the secondary owner takes over or swaps weeks with another engineer.

### Holiday Considerations

- Engineers can request time off at least two weeks in advance.

- Major public holidays (e.g., Christmas, New Year's, local holidays) will be accounted for by adjusting the rotation to ensure fair coverage.

- If needed, we will have a backup engineer to support critical issues.

---

## Runbooks for Investor-Reported Metrics Pipelines

### 1. Aggregate Profit Pipeline

#### Potential Issues:

- Data source failures (e.g., missing revenue data)
- Incorrect profit calculations due to upstream changes
- Delayed or failed jobs due to resource constraints

### 2. Aggregate Growth Pipeline

#### Potential Issues:

- Data inconsistencies from different business units
- Mismatched reporting periods leading to incorrect figures
- API failures affecting external data ingestion

### 3. Aggregate Engagement Pipeline

#### Potential Issues:

- Metric definitions changing without proper communication
- Unexpected spikes or drops in engagement leading to data anomalies
- Issues with third-party data sources
