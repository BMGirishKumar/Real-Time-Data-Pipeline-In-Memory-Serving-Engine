import uvicorn
from fastapi import FastAPI, HTTPException,Depends
from sqlalchemy.orm import Session
from database import sessionLocal, LiveTelemetry


app= FastAPI(title="Unified Read-Time API Dashboard",version="1.0.0")

def get_db_session():
    db=sessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/metrics/live")
def fetch_live_metrix(limit:int = 50, db:Session=Depends(get_db_session)):
    """Pulls freshly structured records instantly out of RAM memory."""
    records = db.query(LiveTelemetry).order_by(LiveTelemetry.id.desc()).limit(limit).all()
    return {"count": len(records), "data": records}

@app.get("/metrics/alerts")
def fetch_active_alerts(db: Session = Depends(get_db_session)):
    """Filter records requiring operational intervention."""
    alerts = db.query(LiveTelemetry).filter(LiveTelemetry.status == "ALERT").all()
    return {"active_alerts_count": len(alerts), "alerts": alerts}

def start_api():
    uvicorn.run(app, host="127.0.0.1", port=8000)

if __name__ == "__main__":
    start_api()