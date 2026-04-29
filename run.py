"""
BioSentinel Entry Point
Railway sets the PORT environment variable automatically.
run.py reads it so the app binds correctly in production.
"""
import os
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    debug = os.getenv("BIOSENTINEL_DEBUG", "false").lower() == "true"

    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=port,
        reload=debug,
        log_level="info",
    )
