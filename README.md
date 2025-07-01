# 🎓 ScholAmigo_BDM


**ScholAmigo_BDM** is a data engineering platform designed for the **Big Data Management** course at **UPC**. It orchestrates the collection, processing, and management of data related to international scholarship programs, leveraging modern data tools and cloud infrastructure.

---

## 🌐 Live Demo
[Visit the ScholAmigo Website](https://scholamigo-website.lovable.app)

---

## 🚀 Features
- Automated staging and organization of scholarship data
- Processed data pushed to AWS S3
- Workflow orchestration with Apache Airflow DAGs
- Secure AWS connectivity via GitHub Actions
- Clean `.env` configuration for easy local development

---

## 🏗️ System Architecture

![System Architecture](./ScholAmigo%20-%20Architecture%20Design%20-%20P2.png)

---

## ⚙️ Technologies Used
- **Orchestration & Cloud:**
  - Apache Airflow 2.8.1 (via Docker Compose)
  - AWS S3 (cloud data storage)
  - AWS CLI (command-line AWS operations)
  - Docker & Docker Compose
  - Git & GitHub Actions (CI/CD & cloud automation)

- **Data Processing & Analysis:**
  - Python & Boto3
  - Pandas, NumPy
  - Jupyter Notebooks (exploratory analysis)
  - Parquet (efficient data storage format)
  - JSON/JSONL (data interchange formats)
  - SQL (data querying and transformation)
  - Shell scripting (automation scripts)

- **Machine Learning & Vector Search:**
  - FAISS (vector database for similarity search)

- **PDF & Data Extraction:**
  - PyPDF2 or pdfminer (PDF extraction for resume processing)

- **Visualization:**
  - Matplotlib, Seaborn (data visualization)

---

## 🗂️ Project Structure
```
ScholAmigo_BDM/
├── airflow/                  # Airflow orchestration (DAGs, Docker, logs)
│   ├── dags/                 # Airflow DAG scripts
│   ├── docker-compose.yaml   # Airflow Docker Compose config
│   ├── Dockerfile            # Airflow Dockerfile
│   └── logs/                 # Airflow logs
│
├── exploitation_zone/        # Data processing, matching, and vector DB
│   ├── formatters/           # Data formatting scripts
│   │   ├── 00_Scholarships_formatter.py
│   │   ├── 01_Aggregator_formatter.py
│   │   ├── 02_student_alumni_graph_creation.py
│   │   └── 03_graph_cypher_queries.py
│   ├── scripts/              # SQL and export scripts
│   │   ├── export_scholarships.py
│   │   ├── Scholarships.sql
│   │   ├── truncateAll.sql
│   │   └── Exploitation_Sch
│   ├── data/                 # Processed data (e.g., scholarships.json)
│   ├── vector_database/      # FAISS index and metadata
│   │   ├── cv_index.faiss
│   │   └── cv_metadata.json
│   ├── scholarship_matcher.py
│   ├── setup_db.sh
│   ├── setup.sh
│   └── init_vector_db.py
│
├── landing_zone/             # Data collection (raw data, collectors)
│   ├── collectors/           # Data collector scripts and notebooks
│   │   ├── erasmus_collector.py/.ipynb
│   │   ├── uk_collector.py/.ipynb
│   │   ├── users_collector.py/.ipynb
│   │   ├── scholarship_collector.py/.ipynb
│   │   ├── resume_collector.py/.ipynb
│   │   └── LinkedinProfiles_collector.py/.ipynb
│   └── data/                 # Raw data (scholarships, resumes, etc.)
│       ├── aggregator_data/
│       ├── erasmus_data/
│       ├── erasmus_linkedin_profiles/
│       ├── resume_data/
│       ├── uk_data/
│       └── users_data/
│
├── trusted_zone/             # Cleaned/processed data and formatters
│   ├── formatters/           # Data cleaning and transformation scripts
│   │   ├── aggregator_processor.py
│   │   ├── student_alumni_cleaner.py
│   │   ├── UK_Scholarships_Extract_Fields.py
│   │   ├── UK_Scholarships_clean_load.py
│   │   ├── Erasmus_Extract_Fields.py
│   │   ├── Erasmus_clean_load.py
│   │   ├── cv_extract_pdfs.py
│   │   ├── cv_upload_to_s3.py
│   │   └── cv_transformer.py
│   └── data/                 # Cleaned data (JSON, Parquet, etc.)
│       ├── aggregator_data/
│       ├── erasmus_data/
│       ├── graph_data/
│       ├── resume_data/
│       └── uk_data/
│
├── metadata/                 # Schemas, metadata, and scripts
│   ├── schemas/              # JSON schemas for data validation
│   ├── scripts/              # Metadata-related scripts
│   ├── description.md        # Project metadata description
│   ├── registry.yaml         # Data registry
│   └── sources.json          # Data sources
│
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
├── ScholAmigo - Architecture Design - P2.png # System architecture diagram
└── BDM_P2_Report.pdf         # Project report
```

---

## 🛠️ Setup Instructions (Local)

> **Requirements:** Docker, Docker Compose, (optional: AWS CLI)

1. **Clone the Repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/ScholAmigo_BDM.git
   cd ScholAmigo_BDM/airflow
   ```

2. **Start Airflow Locally**
   ```bash
   docker compose up
   ```

3. **Access the Airflow UI**
   - Open your browser: [http://localhost:8080](http://localhost:8080)
   - Login (default):
     ```
     Username: airflow
     Password: airflow
     ```

---

## ▶️ Usage

- **Trigger a DAG Manually:**
  ```bash
  docker compose exec airflow-cli airflow dags list
  docker compose exec airflow-cli airflow dags trigger -d <dag_id>
  ```
  Replace `<dag_id>` with your actual DAG name.

---

## 🧪 Testing (GitHub Actions)

A workflow is available to test AWS S3 integration:

1. Go to the [Actions tab](../../actions) of this repository.
2. Click **"Test AWS Access"** workflow.
3. Click **"Run workflow"** → Select branch → Click **"Run"**.

This will:
- Authenticate to AWS using GitHub Secrets
- List S3 buckets (visible in the logs)

> AWS credentials are managed securely using GitHub Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request for improvements, bug fixes, or new features.