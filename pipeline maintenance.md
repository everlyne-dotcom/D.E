
# DATA ENGINEERING TEAM

## DATA PIPELINE MAINTENANCE

### Overview
You are in charge of managing the following data pipelines:

1. **Growth Pipeline**
   - Tracks increases in statistics per user.
   - Monitors the number of new and upgraded accounts.
   - Measures revenue growth from upgraded accounts.

2. **Profit Pipeline**
   - Calculates subscription revenue minus expenses.
   - Computes unit-level profit: `profit / number of subscribers` (includes cost per account analysis).

3. **Engagement Pipeline**
   - Measures user activity within the company.
   - Tracks total time users spend on their accounts daily.

4. **Aggregate Pipeline for Executives and CFO**
   - Provides fortnightly data reports for investors.

5. **Aggregate Pipeline for the Experiment Team**
   - Supplies unit-level or daily-level data for experimentation.
   - Preferred reporting frequency: Weekly (fortnightly for product team decision-making).

### Investor-Impact Pipelines
The **Profit, Growth, Engagement, and Aggregate Pipelines** directly impact investor reports.

---

## RUNBOOKS

### 1. Growth Pipeline
#### I. Types of Data
- Changes in account type.
- Increase in licensed users.
- Subscription changes:
  - New subscriptions.
  - Canceled subscriptions.
  - Renewals for the next calendar year.

#### II. Owners
- **Primary Owner:** John Doe from Accounts Team
- **Secondary Owner:** Jane Smith from Data Engineering Team

#### III. Common Issues
- Missing time-series data due to Active Directory (AD) team errors.
  - **Clue:** Missing data in a required previous step. Monitor logs for AD sync errors.
  
#### IV. SLAs
- Data will include the latest account statuses by the end of the week. If the data is not updated, escalate to the Data Engineering Lead within 24 hours.

#### V. On-call Schedule
- No on-call required.
- Pipeline issues will be debugged during working hours.

---

### 2. Profit Pipeline
#### I. Types of Data
- Revenue from accounts.
- Expenses (assets, services, operations).
- Aggregated salaries by team.

#### II. Owners
- **Primary Owner:** Finance Team / Risk Team
- **Secondary Owner:** John Doe from Data Engineering Team

#### III. Common Issues
- Revenue figures may not align with financial records.
  - **Solution:** Review figures with an accountant if necessary.
  
#### IV. SLAs
- Monthly review by the accounts team. Escalate any discrepancies within 24 hours.

#### V. On-call Schedule
- Monitored by the Business Intelligence (BI) team in Profit.
- Weekly rotation for monitoring.
- Immediate action required if issues arise.

---

### 3. Engagement Pipeline
#### I. Owners
- **Primary Owner:** Software Frontend Team
- **Secondary Owner:** Data Engineering Team

#### II. Engagement Metrics
- User activity is measured by clicks across teams.
- Data arrives via a Kafka queue.

#### III. Common Issues
- Late data arrival to Kafka.
  - **Solution:** Monitor Kafka consumer lag to identify delays.
- Kafka outages may result in missing website click events.
  - **Solution:** Implement an alerting mechanism for Kafka downtime.
- Duplicate events may require deduplication.

#### IV. SLAs
- Data should be available within **48 hours**.
- Issues must be resolved within **one week**. If unresolved, escalate to Data Engineering Lead.

#### V. On-call Schedule
- One Data Engineer (DE) is assigned weekly for monitoring.
- A designated contact is available for queries.

---

### 4. Aggregate Data for Executives and CFO
#### I. Owners
- **Primary Owner:** Business Analytics Team
- **Secondary Owner:** Data Engineering Team

#### II. Common Issues
- Spark joins may fail due to **Out of Memory (OOM)** issues.
- Stale data from previous pipelines may cause inconsistencies.
- Missing data can lead to `NA` values or **divide-by-zero errors**.

#### III. SLAs
- Issues must be resolved **by the end of the month**, before reporting to executives, CFO, and investors.

#### IV. On-call Schedule
- **Last week of the month:** Data Engineers monitor pipeline stability from the **start of the month**.

---

### 5. Aggregate Data for the Experiment Team
#### I. Owners
- **Primary Owner:** Data Science Team
- **Secondary Owner:** Data Engineering Team

#### II. Common Issues
- Renamed or missing data may cause null values.
  - **Solution:** Ensure correct data labeling and thorough checks for missing values.
- Spark pipeline delays due to large queries.

#### III. SLAs
- Data should be available within **48 hours**.
- Issues must be resolved **by the end of the week**. If unresolved, escalate to Data Engineering Lead.

#### IV. On-call Schedule
- No on-call required.
- Debugging will occur during working hours.

---

### Final Notes
- Ensure **pipelines affecting investor reports (Profit, Growth, Engagement, Aggregated Data)** are prioritized.
- Improve on-call fairness with a structured **rotation schedule**.
- Enhance **SLAs** with clear escalation procedures for unresolved issues.
- Consider adding a **markdown table for better visibility** of the on-call schedule.

---

### **On-call Rotation Schedule:**

| Week | Primary Owner          | Secondary Owner         | On-call Engineer     |
|------|------------------------|-------------------------|----------------------|
| Week 1 | Accounts Team          | Data Engineering Team   | John Doe             |
| Week 2 | Finance Team           | Data Engineering Team   | Jane Smith           |
| Week 3 | Software Frontend Team | Data Engineering Team   | John Doe             |
| Week 4 | Business Analytics Team| Data Engineering Team   | Jane Smith           |
