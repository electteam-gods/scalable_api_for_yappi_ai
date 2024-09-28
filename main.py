from typing import Optional

from celery.result import AsyncResult
from fastapi import FastAPI
from pydantic import BaseModel

import celery_worker

app = FastAPI()


class CheckVideoDuplicatePayload(BaseModel):
    link: str


class CheckVideoDuplicateResponse(BaseModel):
    is_duplicate: bool
    duplicate_for: Optional[str]


@app.post("/check-video-duplicate", response_model=CheckVideoDuplicateResponse)
async def check_video_duplicate(payload: CheckVideoDuplicatePayload):
    task: AsyncResult = celery_worker.find_duplicates.delay(payload.link)
    taskResult = celery_worker.FindDuplicatesTaskResult.model_validate(task.get())
    return CheckVideoDuplicateResponse(
        is_duplicate=taskResult.is_duplicate,
        duplicate_for=taskResult.duplicate_for
    )
