# Connecting GitHub to Databricks Free Edition

> A step-by-step guide to link your Databricks workspace to GitHub using the built-in Git (Repos) integration — no terminal or CLI required.

---

## Prerequisites

- A [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account
- A [GitHub](https://github.com) account
- Your project notebooks already saved in your Databricks Workspace

---

## Step 1 — Create a New GitHub Repository

1. Log in to [github.com](https://github.com)
2. Click the **`+`** icon (top-right) → **New repository**
3. Fill in the details:
   - **Repository name:** `spark-structured-streaming` *(or your preferred name)*
   - **Visibility:** `Public` *(recommended for portfolio)*
   - ✅ Check **"Add a README file"**
4. Click **Create repository**

> You will replace the auto-generated README with your project README later.

---

## Step 2 — Generate a GitHub Personal Access Token (PAT)

Databricks uses a PAT to authenticate against GitHub on your behalf.

1. On GitHub, click your **profile picture** (top-right) → **Settings**
2. Scroll down the left sidebar → click **Developer settings**
3. Click **Personal access tokens** → **Tokens (classic)**
4. Click **Generate new token (classic)**
5. Fill in:
   - **Note:** `databricks-repo-sync`
   - **Expiration:** `90 days` *(or No expiration for a portfolio project)*
   - **Scopes:** ✅ Check `repo` *(grants full read/write to your repositories)*
6. Scroll down → click **Generate token**
7. **Copy the token immediately** — GitHub will never show it again

> ⚠️ Store your PAT somewhere safe (e.g. a password manager). If you lose it, you will need to regenerate one.

---

## Step 3 — Add GitHub Credentials to Databricks

1. Log in to your **Databricks Free Edition** workspace
2. Click your **username / profile icon** (bottom-left or top-right depending on UI)
3. Click **Settings**
4. In the left panel, look for **Linked accounts** or **Git integration**
5. Under **Git provider**, select **GitHub**
6. Enter:
   - **Git provider username:** your GitHub username
   - **Personal access token:** paste the PAT you copied in Step 2
7. Click **Save**

Databricks will validate the token. A green confirmation confirms the link is successful.

---

## Step 4 — Create a Databricks Repo (Clone Your GitHub Repo)

1. In the Databricks left sidebar, click **Workspace**
2. Navigate to **Repos** (you may see it directly in the sidebar or under Workspace)
3. Click **Add Repo** (top-right or via the `+` button)
4. In the dialog:
   - **Git repository URL:** `https://github.com/<your-username>/spark-structured-streaming`
   - **Git provider:** GitHub *(auto-detected)*
   - **Repo name:** leave as-is or rename
5. Click **Create Repo**

Databricks will clone your GitHub repository into your workspace at:
```
/Repos/<your-databricks-username>/spark-structured-streaming/
```

---

## Step 5 — Move Your Notebooks Into the Repo Folder

> ⚠️ **Critical:** Only files physically located inside the `/Repos/` folder are tracked by Git. Notebooks outside this folder will NOT be pushed to GitHub.

**To move existing notebooks:**

1. In the Workspace explorer, locate your existing notebooks (e.g. under `/Users/<your-email>/`)
2. Right-click the notebook → **Move**
3. Navigate to `/Repos/<your-username>/spark-structured-streaming/notebooks/`
4. Confirm the move

**Recommended folder structure inside your Repo:**

```
spark-structured-streaming/
│
├── README.md                        ← replace GitHub's auto-generated README
├── notebooks/
│   ├── 00_setup.py                  ← catalog / schema / volume creation
│   ├── 01_producer.py               ← fake transaction data generator
│   └── 02_streaming_pipeline.py     ← main readStream → agg → writeStream
└── docs/
    └── architecture.png             ← optional: export of your architecture diagram
```

To upload your `README.md`:
1. Click the **`+`** button inside the Repo folder → **Upload**
2. Select your `README.md` file from your local machine
3. It will appear at the root of the Repo

---

## Step 6 — Commit and Push to GitHub

1. Open any notebook inside your `/Repos/` folder
2. In the top-right of the notebook, click the **Git branch icon** (shows the current branch name e.g. `main`)
3. The **Databricks Git dialog** opens — you will see all new/changed files listed
4. Write a commit message:
   ```
   Initial commit: Add Project 1 - Real-Time Sales Analytics pipeline
   ```
5. Click **Commit & Push**

Databricks performs `git add` + `git commit` + `git push` in one action. No terminal needed.

6. Go to your GitHub repository and refresh — your files should now appear ✅

---

## Step 7 — Verify on GitHub

On your GitHub repo page, confirm:

- [ ] `README.md` is visible and renders correctly on the repo home page
- [ ] `notebooks/` folder contains all three notebooks
- [ ] Commit history shows your message

---

## Day-to-Day Git Operations in Databricks Repos

| Action | How to do it |
|---|---|
| **Check current branch** | Click the branch icon (top-right of any notebook in the Repo) |
| **Pull latest changes from GitHub** | Git dialog → **Pull** |
| **Commit + push new changes** | Git dialog → write message → **Commit & Push** |
| **Create a new branch** | Git dialog → branch dropdown → **+ Create branch** |
| **Switch branches** | Git dialog → branch dropdown → select branch |
| **Discard uncommitted changes** | Git dialog → **Discard changes** |

---

## Recommended GitHub Repo Settings (Portfolio Polish)

After pushing, make your repo stand out:

1. **Add a description:**
   Repo page → ⚙️ gear icon (top-right of About section) → Description:
   > *"Real-time sales analytics pipeline using PySpark Structured Streaming on Databricks Free Edition with Delta Lake sink."*

2. **Add topics/tags:**
   Same gear icon → Topics:
   ```
   apache-spark  pyspark  databricks  structured-streaming  delta-lake  data-engineering
   ```

3. **Pin the repo on your profile:**
   Your GitHub profile → **Customize your pins** → select the repo

---

## Troubleshooting

| Issue | Fix |
|---|---|
| "Invalid credentials" when saving PAT | Re-generate the PAT; ensure `repo` scope is checked |
| Notebooks not showing in Git dialog | Confirm the notebook is inside `/Repos/` not `/Users/` |
| Push fails with "permission denied" | Check the PAT hasn't expired; regenerate if needed |
| Can't find "Repos" in sidebar | Click **Workspace** in the sidebar; Repos is a sub-section |
| Changes not reflecting on GitHub | Ensure you clicked **Commit & Push**, not just **Commit** |

---

*Databricks Free Edition · Git Integration · Last updated: 2025*
