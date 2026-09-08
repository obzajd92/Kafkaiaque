# Django + SQLAlchemy + Kafka (KRaft & Postgres Stack)

This setup couples a custom scoped-session SQLAlchemy infrastructure with manual Kafka commit controls to achieve highly-resilient, **at-least-once** event streaming delivery.

## 🛠️ Bash Requirements & Environment Setup

Run these sequentially to orchestrate your local environment.

### 1. Project Dependencies
```bash
# Setup python isolation env
python3 -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate

# Add app components
pip install --upgrade pip
pip install django sqlalchemy psycopg2-binary confluent-kafka
```

### 2. Microservice Stack Deployment (Kafka & PostgreSQL)
```bash
# Stand up database and stream clusters simultaneously
docker compose up -d

# Verify both structures pass verification rules
docker compose ps
```

---

## 🚀 Execution & System Lifecycles

You will need **three** concurrent terminal shells to watch the pipeline execute in real-time.

### Terminal 1: Schema Construction & Server Binding
Wait roughly 5 seconds for Postgres to clear health checks, then initialize data structures before spawning the app server:
```bash
# Build table blueprints directly via SQLAlchemy Models
python -c "from myproject.sqlalchemy_base import engine, Base; from core.models_sqla import Item; Base.metadata.create_all(bind=engine)"

# Bind local developer server
python manage.py runserver
```

### Terminal 2: Manual Commit Worker Daemon
```bash
source venv/bin/activate
python manage.py run_consumer
```

### Terminal 3: Log Extraction & Teardown Routine
```bash
# Target real-time streaming feedback
docker compose logs -f kafka

# Full application component teardown
docker compose down
```
