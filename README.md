# RAMStream Engine 

A high-performance, end-to-end distributed data engineering pipeline that ingests high-velocity raw telemetry data streams, processes them in real time using distributed micro-batches, and stores the structured output in a shared in-memory database to be served instantly via an asynchronous REST API dashboard.

---

## Architecture Blueprint

[ RAW Data Source ] ──► [ Kafka Producer ]
│
▼
[ Apache Kafka Broker ]  (Decoupled Ingestion Buffer)
│
▼
[ PySpark Stream Engine ]  (Schema Enforcement & Rules)
│
▼
[ SQLite In-Memory DB ]    (Shared RAM Cache via SQLAlchemy)
▲
│ (Sub-millisecond Read Requests)
[ FastAPI REST API ]
▲
│
[ End User / UI ]

### Key Architectural Pillars:
* **Ingestion (Load Leveling):** **Apache Kafka** buffers unpredictable high-volume incoming raw payloads, safeguarding the downstream system from database-level write locks or connection drops.
* **Processing (Distributed Compute):** **PySpark Structured Streaming** ingests Kafka message chunks concurrently, handling data transformations and rule evaluations natively.
* **Serving (Ultra-low Latency):** **SQLAlchemy ORM** routes micro-batches into a shared-cache in-memory database space. **FastAPI** handles thousands of asynchronous connection reads directly out of your machine's system RAM.

---

## Repository Layout

```text
ram-stream-engine/
│
├── database.py       # Shared SQLite in-memory DB + SQLAlchemy 2.0 structures
├── processor.py      # PySpark Structured Streaming consumer framework
├── app.py            # Asynchronous FastAPI web endpoints
├── producer_mock.py  # Mock data stream injector (CSV/Scraper compatible)
├── run.py            # Master orchestration execution harness
├── requirements.txt  # System library dependency targets
└── README.md         # Project portfolio documentation
```

---

## Tech Stack & Dependencies

* Core Languages: **Python 3.10+**, **Java 11+** (Required for PySpark engine runtimes)
* Message Broker: **Apache Kafka** (with Zookeeper or KRaft deployment)
* Stream Processor: **PySpark 3.5.0** (Structured Streaming Engine)
* Storage Layer: **SQLAlchemy 2.0 ORM** / **SQLite In-Memory Cache**
* Presentation Layer: **FastAPI**, **Uvicorn**

---

## Step-by-Step Installation & Setup

### 1. Clone & Set Up Python Virtual Environment
```bash
git clone https://github.com
cd ram-stream-engine
python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

### 2. Install Project Requirements
```bash
pip install -r requirements.txt
```
*(Your `requirements.txt` includes: `fastapi`, `uvicorn`, `pyspark`, `kafka-python`, and `sqlalchemy`)*

### 3. Initialize Your Local Apache Kafka Broker
Ensure your Kafka server instance is running locally on port `9092`. Create the dedicated tracking event stream topic:
```bash
kafka-topics.sh --create --topic telemetry-stream --bootstrap-server localhost:9092
```

---

## Running the Engine Workspace

Instead of opening multiple independent shell terminals, launch the full end-to-end stack securely using the automated master orchestration entry-point:

```bash
python run.py
```

### What happens under the hood?
1. `run.py` spawns **FastAPI** on port `8000` and configures the cross-thread RAM database layout.
2. The orchestrator boots the **PySpark Streaming cluster**, downloading the required native `spark-sql-kafka` drivers automatically.
3. The background **Telemetry Data Injector** initializes, pumping random simulated logistics truck payloads (`Truck ID`, `Speed`, `Engine Temperature`) directly into your active Kafka partition buffers.

---

## 📡 REST API Endpoint Verification

Once the engine status is fully operational, open your web browser or any HTTP client to monitor live processing:

### 1. View Live Streaming Logs (RAM Ingest)
* **URL:** `http://127.0.0`
* **Response:** Returns the newest structured logs retrieved out of system memory.

### 2. View Active Speeding Violations (Filtered via PySpark)
* **URL:** `http://127.0.0`
* **Response:** Displays only the trucks flag-tagged as `"ALERT"` because their processed reading speed exceeded `75.0 km/h`.

---

## Production Scaling Strategies (Interview Q&A)

If asked how to transition this architecture from a single local workspace environment to true distributed cloud-scale data flows:
1. **Swap out Storage:** Exchange the single-node SQLite connection mapping for a high-availability distributed in-memory dataset store like **Redis**, or high-performance time-series columns like **ClickHouse**.
2. **Horizontal Kafka Sharding:** Introduce multiple **partitions** and consumer groups to allow PySpark to parallel-process events across multiple server cluster nodes concurrently.