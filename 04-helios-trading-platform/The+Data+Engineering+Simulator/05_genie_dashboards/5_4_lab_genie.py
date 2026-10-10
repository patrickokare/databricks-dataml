# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.4: Build a Genie Agent on the semantic layer
# MAGIC
# MAGIC ### Ticket: HELIOS-504
# MAGIC
# MAGIC **Context.** Genie lets a business user ask a question in plain English and get an answer as SQL over your data. In this lab you create a Genie Agent over the curated `helios_semantic` metric views and then explore the data with it, to see natural-language BI working over the governed metrics. It is a short, UI-only lab: build the agent, then use it.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Create a Genie Agent scoped to your `helios_semantic` metric views.
# MAGIC 2. Explore the data by asking your own questions, and look at the answers and the SQL it generates.
# MAGIC
# MAGIC **Acceptance criteria** (you confirm these in the UI):
# MAGIC - The agent lists the three metric views as its data.
# MAGIC - You have asked a range of your own questions and seen an answer and the generated SQL for each.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Runs the shared `genie_bootstrap` helper, which rebuilds the canonical Gold (batches one to five) and the finished Lab 5.3 semantic layer, so you start with the metric views ready to point Genie at. It is idempotent, so it is safe to re-run.

# COMMAND ----------

# MAGIC %run ../00_setup/genie_bootstrap

# COMMAND ----------

# The agent points at these three metric views. Use your 2X-Small SQL warehouse in the UI.
print("Your catalog:", catalog)
print("Metric views to add: sales_mv, orders_mv, inventory_mv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Build and use the Genie Agent
# MAGIC
# MAGIC This is a UI lab: you build the agent in the Databricks workspace and then chat with it. There is no code to write, so there are no reveals either. The value is in what you ask and in reading the SQL Genie writes back.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create the Genie Agent on the semantic metric views
# MAGIC
# MAGIC Create a new Genie Agent scoped to your three `helios_semantic` metric views, so a business user can ask about Helios sales, fulfilment and inventory in plain English. Pointing an agent at a small, well-commented set of metric views (rather than a whole schema of raw tables) is the single biggest lever on answer quality, and Lab 5.3 already commented and named everything for exactly this.
# MAGIC
# MAGIC **How.**
# MAGIC - In the workspace sidebar, open **Genie Agents**, then **New**. Pick your 2X-Small SQL warehouse when prompted.
# MAGIC - Add data: choose `helios_semantic.sales_mv`, `helios_semantic.orders_mv` and `helios_semantic.inventory_mv`. Metric views are first-class Genie data, so their dimension and measure names and comments become the vocabulary Genie reasons with.
# MAGIC - Give the agent a name and a one-line description, for example `Helios Trading Commercial Analytics` and "Self-serve analytics for Helios Trading: ask about sales, order fulfilment and inventory across the six depots in plain English. All money is in CREDITS."
# MAGIC - Open the chat.
# MAGIC
# MAGIC **Example.** With the three metric views added, asking "What was total revenue?" should return one number near 1.86 billion CREDITS.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The agent lists `sales_mv`, `orders_mv` and `inventory_mv` under its data.
# MAGIC - It has a name and a description, and the chat is open.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Genie](https://docs.databricks.com/aws/en/genie/)
# MAGIC - [Create and manage a Genie Agent](https://docs.databricks.com/aws/en/genie/set-up)
# MAGIC - [Use a Genie Agent to explore business data](https://docs.databricks.com/aws/en/genie/talk-to-genie)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Explore the data by asking your own questions
# MAGIC
# MAGIC Now use the agent. Ask it whatever you actually want to know about Helios Trading, and for every answer open the generated SQL to see how Genie got there. Reading that SQL is how you sanity-check a conversational answer, so make a habit of it.
# MAGIC
# MAGIC **How.**
# MAGIC - Ask in plain English, one question at a time, and follow your own curiosity. Revenue, margin and units by depot, category, department, supplier or customer tier; cancellations, backorders and on-time fulfilment; stock levels, stockouts and movements; and how any of these change over the five days of data.
# MAGIC - Follow up rather than starting over. Genie keeps the conversation, so after an answer you can say "now split that by day" or "just for Ares Depot" and it will build on what came before.
# MAGIC - Open the generated code on each answer and check it queried a metric view with `MEASURE(...)` and `GROUP BY ALL`, and that it grouped by the dimension you meant.
# MAGIC - When an answer looks wrong or Genie reaches for the wrong field, rephrase using the names from the metric views (`Depot`, `Customer Tier`, `Gross Margin Rate`, `Cancellation Rate`, `Stockout Events`) and see whether that fixes it. Which phrasings work and which do not is exactly what you would write up as instructions and example queries when curating a real agent.
# MAGIC - Dig into anything that looks odd. Something happened at Ares Depot on 2257-03-03, and the agent has enough in it for you to find out what.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - You asked a range of your own questions across all three metric views and read both the answer and the generated SQL for each.
# MAGIC - You used at least one follow-up question in the same conversation.
# MAGIC - You can say which kinds of question the agent handles well and where it needs help.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Use a Genie Agent to explore business data](https://docs.databricks.com/aws/en/genie/talk-to-genie)
# MAGIC - [Curate an effective Genie Agent](https://docs.databricks.com/aws/en/genie/best-practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC A Genie Agent over the `helios_semantic` metric views. You asked it your own questions in plain English, watched it answer with SQL against the governed metrics, and checked that SQL. That is self-serve BI on the semantic layer, working.