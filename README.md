# 🎓 ScholAmigo_BDM

**ScholAmigo_BDM** is a data engineering project built for the **Big Data Management** course at **UPC**. It orchestrates the collection, processing, and management of data related to international scholarship programs. The project uses **Apache Airflow**, integrates with **AWS S3**, and automates parts of its workflow through **GitHub Actions**.


## ⚙️ Technologies Used

- **Apache Airflow 2.8.1** (via Docker Compose)
- **AWS S3** (for data storage)
- **Python** and **Boto3**
- **GitHub Actions** (for secure, automated cloud interactions)
- **Docker & Docker Compose**
- **Jupyter Notebooks** (for exploratory analysis)

---

## 🚀 Features

- Automatically stages and organizes scholarship data.
- Pushes processed data to AWS S3.
- Triggers workflows with Apache Airflow DAGs.
- Verifies AWS connectivity via GitHub Actions.
- Clean `.env` configuration for easy local development.

---

## 🛠️ Setup Instructions (Local)

> You need **Docker**, **Docker Compose**, and optionally **AWS CLI** installed.

### 1. Clone the Repo

```bash
git clone https://github.com/YOUR_USERNAME/ScholAmigo_BDM.git
cd ScholAmigo_BDM/airflow
```

### 2. Start Airflow Locally

```bash
docker compose up
```

### 3. Access the Airflow UI

- Open your browser: [http://localhost:8080](http://localhost:8080)
- Login (default):
  ```
  Username: airflow
  Password: airflow
  ```

### 5. Trigger a DAG Manually

```bash
docker compose exec airflow-cli airflow dags list
docker compose exec airflow-cli airflow dags trigger -d dag_id
```

Replace `dag_id` with your actual DAG name.

---

## 🧪 How to Test (Using GitHub Actions)

A workflow is available to test the AWS S3 integration.

### ✅ Run it via GitHub:

1. Go to the [Actions tab](../../actions) of this repository.
2. Click **"Test AWS Access"** workflow.
3. Click **"Run workflow"** → Select branch → Click **"Run"**.

This will:
- Authenticate to AWS using GitHub Secrets
- List S3 buckets (visible in the logs)

> AWS credentials are managed securely using GitHub Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).


