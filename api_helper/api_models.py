from typing import Any, Dict, List

from pydantic import BaseModel


class PipelineModel(BaseModel):
    steps: List[Dict[str, Any]]
