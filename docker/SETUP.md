# Docker Setup with PostgreSQL - Airflow + Python

## What You Get:
✅ **3 Containers:**
1. **postgres-container** - PostgreSQL database
2. **airflow-container** - Apache Airflow with LocalExecutor
3. **python-container** - Python 3.11

✅ **LocalExecutor** - Can run tasks in parallel (much better than SequentialExecutor)

✅ **Shared Network** - All containers can communicate

✅ **File Persistence** - All data persists on your machine

## Why PostgreSQL + LocalExecutor is Better:

| Feature | SQLite + Sequential | PostgreSQL + Local |
|---------|-------------------|-------------------|
| Parallel Tasks | ❌ No (one at a time) | ✅ Yes (multiple tasks) |
| Production Ready | ❌ No | ✅ Yes |
| Performance | ❌ Slow | ✅ Fast |
| Reliability | ❌ Limited | ✅ High |

## Files Required:
1. `.env` - Configuration file
2. `docker-compose.yml` - Container definitions

## Setup Steps:

### 1. Clean up old setup (if exists)
```bash
docker stop airflow-container python-container postgres-container
docker rm airflow-container python-container postgres-container
rm -rf airflow-data python-data postgres-data
```

### 2. Place both files in your folder
Make sure `.env` and `docker-compose.yml` are in the same folder.

### 3. Start all containers
```bash
docker compose up -d
```

This will:
- Start PostgreSQL first
- Wait for PostgreSQL to be healthy
- Then start Airflow and Python containers

### 4. Wait for initialization (first time only)
First time setup takes 1-2 minutes. Check progress:
```bash
docker logs -f airflow-container
```

Wait until you see:
```
INFO - Running <TaskInstance: ...
```

### 5. Verify all containers are running
```bash
docker ps
```

You should see 3 containers:
- postgres-container
- airflow-container  
- python-container

All should show **"Up"** status.

### 6. Access Airflow
Open browser: http://localhost:8080
- Username: **admin**
- Password: **admin**

## Configuration via .env File

Edit `.env` to customize:

```env
# Change folder locations
AIRFLOW_DATA_PATH=D:/my-airflow-data
PYTHON_DATA_PATH=D:/my-python-work
POSTGRES_DATA_PATH=D:/my-postgres-data

# Change Airflow port
AIRFLOW_PORT=9090

# Change credentials
AIRFLOW_ADMIN_USERNAME=myuser
AIRFLOW_ADMIN_PASSWORD=mypassword

# Change PostgreSQL credentials
POSTGRES_USER=airflow_user
POSTGRES_PASSWORD=securepassword
POSTGRES_DB=airflow_db
```

## Common Commands

### View logs
```bash
# Airflow logs
docker logs -f airflow-container

# PostgreSQL logs
docker logs -f postgres-container

# Python logs
docker logs -f python-container
```

### Access containers
```bash
# Access Python container
docker exec -it python-container bash

# Access PostgreSQL
docker exec -it postgres-container psql -U airflow -d airflow

# Access Airflow
docker exec -it airflow-container bash
```

### Install packages in Python container
```bash
docker exec -it python-container bash
pip install dbt-core dbt-snowflake pandas sqlalchemy
```

### Stop and start
```bash
# Stop all
docker compose down

# Start all
docker compose up -d

# Restart specific container
docker restart airflow-container
```

### Complete reset
```bash
# Stop and remove everything
docker compose down -v

# Delete data folders
rm -rf airflow-data python-data postgres-data

# Start fresh
docker compose up -d
```

## Container Communication

All containers are on the same network and can communicate:

- **From Python → Airflow**: `http://airflow:8080`
- **From Python → PostgreSQL**: `postgresql://airflow:airflow@postgres:5432/airflow`
- **From Airflow → Python**: `http://python:8000` (if running a service)
- **From Airflow → PostgreSQL**: Already configured internally

## Folder Structure

After running `docker compose up -d`, you'll have:

```
your-project-folder/
├── .env
├── docker-compose.yml
├── airflow-data/        (auto-created)
│   ├── dags/
│   ├── logs/
│   └── plugins/
├── python-data/         (auto-created)
└── postgres-data/       (auto-created)
```

## Troubleshooting

### Airflow still restarting?
```bash
# Check if PostgreSQL is healthy
docker ps

# Check Airflow logs
docker logs airflow-container
```

### Can't connect to Airflow UI?
- Wait 2-3 minutes for first-time initialization
- Check if port 8080 is free: `netstat -ano | findstr :8080`
- Try accessing: http://127.0.0.1:8080

### PostgreSQL connection issues?
```bash
# Check PostgreSQL is running
docker exec -it postgres-container pg_isready -U airflow

# Check database exists
docker exec -it postgres-container psql -U airflow -l
```

### Reset everything
```bash
docker compose down -v
rm -rf airflow-data python-data postgres-data
docker compose up -d
```

## Advantages of This Setup

1. ✅ **Parallel Task Execution** - LocalExecutor runs multiple tasks simultaneously
2. ✅ **Production-Grade** - PostgreSQL is robust and reliable
3. ✅ **Better Performance** - Much faster than SQLite
4. ✅ **Scalable** - Can handle more complex workflows
5. ✅ **All data persists** - Survives container restarts
6. ✅ **Easy to configure** - Just edit .env file

## Next Steps

1. Place your DAG files in `airflow-data/dags/`
2. Install packages in Python container: `pip install dbt-core dbt-snowflake`
3. Create your workflows in Airflow UI
4. Both containers can now run tasks in parallel!
