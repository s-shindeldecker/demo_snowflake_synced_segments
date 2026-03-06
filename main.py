import os
import logging
from typing import List
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Snowflake to LaunchDarkly Segment Sync", version="1.0.0")

LD_API_KEY = os.getenv("LD_API_KEY")
LD_PROJECT_KEY = os.getenv("LD_PROJECT_KEY")
LD_ENV_KEY = os.getenv("LD_ENV_KEY")

if not all([LD_API_KEY, LD_PROJECT_KEY, LD_ENV_KEY]):
    logger.warning("Missing required environment variables: LD_API_KEY, LD_PROJECT_KEY, LD_ENV_KEY")


class SnowflakeSyncRequest(BaseModel):
    audience: str = Field(..., description="Segment key in LaunchDarkly")
    included: List[str] = Field(..., description="Context keys to add to the segment")
    excluded: List[str] = Field(default_factory=list, description="Context keys to remove from the segment")
    version: int = Field(..., description="Sync version number (for auditing)")


class SyncResponse(BaseModel):
    status: str
    ld_response: str
    count_included: int
    count_excluded: int


@app.post("/api/snowflake-sync", response_model=SyncResponse)
async def sync_snowflake_to_launchdarkly(request: SnowflakeSyncRequest) -> SyncResponse:
    """
    Receive segment membership changes from Snowflake and forward them
    to LaunchDarkly using the semantic patch API.
    """
    segment_key = request.audience.strip()

    if not segment_key:
        raise HTTPException(status_code=400, detail="audience field is required and cannot be empty")

    if not segment_key.replace('-', '').replace('_', '').isalnum():
        raise HTTPException(
            status_code=400,
            detail="audience (segment key) must contain only alphanumeric characters, hyphens, and underscores",
        )

    logger.info(f"Syncing segment '{segment_key}': {len(request.included)} included, {len(request.excluded)} excluded")

    if not all([LD_API_KEY, LD_PROJECT_KEY, LD_ENV_KEY]):
        logger.warning("Missing LaunchDarkly credentials -- returning mock response")
        return SyncResponse(
            status="ok",
            ld_response="Mock response - LaunchDarkly credentials not configured",
            count_included=len(request.included),
            count_excluded=len(request.excluded),
        )

    ld_url = f"https://app.launchdarkly.com/api/v2/segments/{LD_PROJECT_KEY}/{LD_ENV_KEY}/{segment_key}"
    headers = {
        "Authorization": f"{LD_API_KEY}",
        "Content-Type": "application/json; domain-model=launchdarkly.semanticpatch",
        "LD-API-Version": "20240415",
    }

    instructions = []
    if request.included:
        instructions.append({
            "kind": "addIncludedTargets",
            "contextKind": "user",
            "values": request.included,
        })
    if request.excluded:
        instructions.append({
            "kind": "removeIncludedTargets",
            "contextKind": "user",
            "values": request.excluded,
        })

    if not instructions:
        return SyncResponse(
            status="ok",
            ld_response="No changes to apply",
            count_included=0,
            count_excluded=0,
        )

    payload = {"instructions": instructions}

    logger.info(f"PATCH {ld_url}")
    logger.info(f"Payload: {payload}")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.patch(ld_url, json=payload, headers=headers)
    except httpx.HTTPError as e:
        logger.error(f"HTTP error calling LaunchDarkly: {e}")
        raise HTTPException(status_code=500, detail="Error communicating with LaunchDarkly")

    logger.info(f"Response {response.status_code}: {response.text}")

    if response.status_code in (200, 204):
        logger.info(f"Successfully synced segment '{segment_key}'")
        return SyncResponse(
            status="ok",
            ld_response="Segment updated successfully",
            count_included=len(request.included),
            count_excluded=len(request.excluded),
        )

    error_map = {
        401: (401, "Unauthorized: Invalid LaunchDarkly API key or insufficient permissions"),
        403: (403, "Forbidden: API key lacks required permissions for this operation"),
        404: (404, f"Segment '{segment_key}' not found in LaunchDarkly project '{LD_PROJECT_KEY}'"),
        409: (409, f"Conflict updating segment '{segment_key}': {response.text}"),
    }
    if response.status_code in error_map:
        code, msg = error_map[response.status_code]
        logger.error(msg)
        raise HTTPException(status_code=code, detail=msg)

    logger.error(f"LaunchDarkly API error: {response.status_code} - {response.text}")
    raise HTTPException(status_code=500, detail=f"LaunchDarkly API error: {response.status_code} - {response.text}")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "snowflake-ld-sync"}


@app.get("/")
async def root():
    return {
        "message": "Snowflake to LaunchDarkly Segment Sync",
        "version": app.version,
        "endpoints": {
            "sync": "/api/snowflake-sync",
            "health": "/health",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
