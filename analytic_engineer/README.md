# Risk Flagging Pipeline — Consultation Exercise

## The Situation

A financial services analytics team built this pipeline to automate their daily client reporting. It's been running in production for a few months. The team is growing and leadership wants an outside perspective before they scale it further.

You've been brought in as a **data consultant**. Your job is to read through the codebase and give the team your honest read — where is this process fragile, what keeps you up at night, and what would you want to address before this gets harder to change.

---

## What the Pipeline Does

Every morning, an automated job:

1. Queries the transactions database for recent activity
2. Aggregates transaction totals per user, across five clients
3. Identifies users who exceeded each client's review threshold
4. Emails each client a CSV of their flagged users for the day
5. Sends an internal summary to the analytics team

---

## Your Task

Read through the code and walk us through your thinking out loud.

There's no written deliverable. We want to hear how you reason about a system you didn't build — what you notice, what questions it raises, and how you'd advise the team on where to focus.

---

## Files

| File | Description |
|---|---|
| `pipeline.py` | Main daily job |
| `summary_report.py` | Internal summary email |
| `data_utils.py` | Utility functions |
| `client_config.py` | Client configuration |

**Time:** ~30 minutes. You won't cover everything — that's expected. We're more interested in how you prioritize than how exhaustive you are.
