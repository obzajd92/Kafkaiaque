# Django + SQLAlchemy + Kafka App

A highly decoupled boilerplate combining **Django views**, a custom scoped-session **SQLAlchemy** layer, and an event-driven setup using **Apache Kafka** (KRaft mode).

## 🛠️ Bash Requirements & System Setup

Execute these steps in your terminal to initialize the environment and backends.

### 1. Environment & Dependencies Setup
```bash
# Set up virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows users: .\venv\Scripts\activate

# Install required stack
pip install --upgrade pip
pip install django sqlalchemy psycopg2-binary confluent-kafka
```

### 2. Infrastructure Deployment (Kafka Container)
```bash
# Start Kafka in the background via KRaft Mode (No Zookeeper required)
docker compose up -d

# Verify that the container is up and healthy
docker compose ps
```

---

## 🏗️ Project Components Reference

### Data Engine Layer
* **`myproject/sqlalchemy_base.py`**: Configures engines, session hooks, and the `Base` declarative meta model.
* **`myproject/middleware.py`**: Intercepts queries to bind database sessions right into `request.db` per web request.

### Stream Event Layer
* **`core/kafka.py`**: Holds low-level Producer logic and the long-polling loop wrapper for the database tracker.
* **`core/management/commands/run_consumer.py`**: Exposes the Kafka polling system right into native Django CLI operations.

---

## 🚀 Execution & Operational Workflows

You will need **three** terminal windows running concurrently to interact with this application locally.

### Terminal 1: Database Generation & Web App Host
If your target schema does not exist yet, trigger structural creation via SQLAlchemy before serving the endpoints:
```bash
# Build data layer
python -c "from myproject.sqlalchemy_base import engine, Base; from core.models_sqla import Item; Base.metadata.create_all(bind=engine)"

# Spin up Django
python manage.py runserver
```

### Terminal 2: Background Consumer Worker Daemon
Run this native management command to dynamically intercept write operations occurring on your database:
```bash
source venv/bin/activate
python manage.py run_consumer
```

### Terminal 3: Infrastructure Lifecycle Management
Use this window to look under the hood or tear down infrastructure when your sessions finish:
```bash
# Tail live stream container outputlogs
docker compose logs -f kafka

# Complete teardown of active messaging cluster
docker compose down
```
