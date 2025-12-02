from typing import List
from pydantic import BaseModel, Field, field_validator

class Node(BaseModel):
    id: str = Field(..., description="节点唯一标识符")
    func_name: str = Field(..., description="函数名称")
    type: str = Field(..., description="任务类型")
    params: dict = Field(default_factory=dict, description="函数参数")
    dependencies: List[str] = Field(default_factory=list, description="依赖节点ID列表")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        valid_types = {"map", "filter", "flatmap", "aggregate", "map_batch"}
        if v not in valid_types:
            raise ValueError(f"任务类型必须是 {valid_types} 之一")
        return v

class Config(BaseModel):
    nodes: List[Node] = Field(..., min_length=1, description="节点列表")

    @field_validator('nodes')
    def validate_unique_ids(cls, v: List[Node]) -> List[Node]:
        ids = [node.id for node in v]
        if len(ids) != len(set(ids)):
            duplicates = {id_ for id_ in ids if ids.count(id_) > 1}
            raise ValueError(f"发现重复的节点 id: {duplicates}")
        return v
