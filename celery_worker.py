from urllib.parse import urlparse
from pathlib import Path
from random import random
from typing import Optional
import requests
import os

from celery import Celery
from pydantic import BaseModel

celery_app = Celery(__name__, broker='redis://redis/0', backend='redis://redis/0')


class FindDuplicatesTaskResult(BaseModel):
    is_duplicate: bool
    duplicate_for: Optional[str]


@celery_app.task()
def find_duplicates(url: str):
    video_id = Path(urlparse(url).path).stem
    result = requests.post('http://find_duplicates_ai', {
        "id": video_id,
        "url": url
    }).json()
    return FindDuplicatesTaskResult(
        is_duplicate=result['answer'],
        duplicate_for=result['id']['0']
    ).model_dump(mode='json')
