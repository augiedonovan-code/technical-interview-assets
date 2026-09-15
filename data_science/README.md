# Model Review Exercise — Customer Churn

Thanks for making the time for this. This exercise is a **code and model review**, not a coding
test. There is nothing to submit beforehand and **you will not be asked to write any code.**

**What to do before we meet:** read through `churn_model_review.html` (about 5–10 minutes) and come
ready to talk about it. If you'd rather read it live with us, that's completely fine too — just let
us know.

---

## The scenario

You've just joined the team as a senior ML engineer.

A junior data scientist on Growth Analytics has built a customer-churn model. The Retention team
wants to use it to decide which subscribers get a "save offer" before they cancel; today that
targeting is done by hand off a spreadsheet. The junior has finished their first pass, written it
up in the notebook in this repo, and is asking to ship it to production.

Your tech lead has asked you to review the work before that happens.

The notebook is genuine, good-faith work by someone early in their career who is trying hard to do
a good job. Treat it that way.

---

## What we'll talk about

The conversation is roughly 45 minutes and unstructured. We'll mostly be asking:

- **Walk us through what they did.** What's the overall approach?
- **Do you believe the headline numbers?** Why or why not?
- **Would you approve the deployment recommendation as written?** If not, what would need to change
  first?
- **What's the single most important thing you'd want fixed**, and how would you explain it to the
  person who wrote this?

We care much more about the depth of a few well-reasoned points than a long list of everything you
noticed. If you find yourself with a lot to say, lead with what matters most.

---

## What's in this repo

| File | What it is |
|---|---|
| `churn_model_review.html` | **Start here.** The notebook, fully rendered with all outputs and plots. Opens in any browser — nothing to install. |
| `churn_model_review.ipynb` | The same notebook, if you'd rather read it in Jupyter/VS Code. Already executed, so all outputs are visible without running anything. |
| `subscriber_churn_snapshot.csv` | The dataset the notebook uses. 8,000 rows, one per subscriber. Only needed if you want to re-run things yourself. |

You do **not** need to run anything to do this exercise.

---

## Ground rules

- **You don't need to fix anything.** Identifying and explaining an issue is the whole task. If you
  want to sketch what you'd do differently, describe it — no need to write working code.
- **Think out loud.** We're interested in how you reason about someone else's work, so a wrong turn
  you talk through is more useful to us than silence.
- **"I'd want to check X before I trusted this" is a great answer.** Knowing what you'd verify is
  part of the skill.
- **No trivia.** We won't ask you to recall API signatures or library minutiae, and you're welcome
  to look things up during the conversation.
- **Ask us questions.** If something about the business context or the data is unclear, ask — we'll
  answer in character as the team that owns this.

---

## What we're looking for

- Can you read an unfamiliar ML notebook and form a view on whether its results can be trusted?
- Can you separate what's cosmetic from what would actually hurt in production?
- Can you give direct, useful feedback to a more junior colleague without being discouraging?

Looking forward to the discussion.
