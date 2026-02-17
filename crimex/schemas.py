"""
Pydantic schemas for crimex.
Defines Fact, QuerySpec, and RunManifest models.
"""
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class Fact(BaseModel):
    """
    Canonical fact schema for normalized crime data.
    """
    source: str = Field(..., description="The data source (e.g., fbi_cde, bjs_ncvs)")
    series: str = Field(..., description="The type of crime/stat being measured (e.g., violent_crime, victimization_rate)")
    geo: str = Field(..., description="Geographic identifier (e.g., US, AL, NY)")
    period: int = Field(..., description="The year or period of the data (e.g., 2020)")
    value: float = Field(..., description="The measured value")
    unit: str = Field(..., description="The unit of the value (e.g., count, rate_per_100k)")
    denominator: Optional[float] = Field(None, description="The population or base used for rates (if applicable)")
    dimensions: Dict[str, Any] = Field(default_factory=dict, description="Additional dimensions (e.g., race, age, sex)")
    ci_lower: Optional[float] = Field(None, description="Confidence interval lower bound")
    ci_upper: Optional[float] = Field(None, description="Confidence interval upper bound")
    se: Optional[float] = Field(None, description="Standard error")
    notes: Optional[str] = Field(None, description="Any notes or caveats about this data point")
    retrieved_at: datetime = Field(default_factory=datetime.utcnow, description="Timestamp when this fact was retrieved/processed")
    query_fingerprint: str = Field(..., description="Hash of the query/spec used to generate this fact")

class QuerySpec(BaseModel):
    """
    Schema for a data query specification.
    """
    source: str = Field(..., description="Source system: 'fbi_cde' or 'bjs_ncvs'")
    endpoint: Optional[str] = Field(None, description="API endpoint path (e.g., /api/data/...)")
    dataset_id: Optional[str] = Field(None, description="Dataset ID for BJS/SODA (e.g., 'ncvs-2020')")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters for the API call")
    series_name: str = Field(..., description="Name for the resulting series")
    notes: Optional[str] = Field(None, description="Notes about what this query fetches")
    expected_unit: Optional[str] = Field(None, description="Expected unit of the result (for validation)")
    expected_denominator: Optional[float] = Field(None, description="Expected denominator (if constant)")
    download_url: Optional[str] = Field(None, description="Fallback URL for direct download if API fails (NCVS)")

    class Config:
        extra = "forbid"

class ManifestEntry(BaseModel):
    """
    Entry in the run manifest describing a generated file.
    """
    filepath: str = Field(..., description="Relative path to the file")
    sha256: str = Field(..., description="SHA256 hash of the file content")
    size_bytes: int = Field(..., description="Size of the file in bytes")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")

class RunManifest(BaseModel):
    """
    Manifest for a run, listing all artifacts created.
    """
    run_id: str = Field(..., description="Unique ID for the run (e.g., timestamp + hash)")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Run timestamp")
    command: str = Field(..., description="The command that triggered this run")
    artifacts: List[ManifestEntry] = Field(default_factory=list, description="List of artifacts created")

def generate_json_schemas(output_dir: str):
    """
    Generates JSON schema files for the models.
    """
    import json
    import os

    os.makedirs(output_dir, exist_ok=True)

    models = {
        "fact-1.0.json": Fact,
        "query_spec-1.0.json": QuerySpec,
        "run_manifest-1.0.json": RunManifest
    }

    for filename, model in models.items():
        schema = model.schema_json(indent=2)
        with open(os.path.join(output_dir, filename), "w") as f:
            f.write(schema)
