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
    duplicate_for: Optional[str] = None


@app.post("/check-video-duplicate", response_model=CheckVideoDuplicateResponse, response_model_exclude_unset=True, response_model_exclude_none=True)
async def check_video_duplicate(payload: CheckVideoDuplicatePayload):
    task: AsyncResult = celery_worker.find_duplicates.delay(payload.link)
    taskResult = celery_worker.FindDuplicatesTaskResult.model_validate(task.get())
    return taskResult
