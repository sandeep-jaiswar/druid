"""
Druid ingestion specification models.
SRP: Each class represents one aspect of Druid configuration.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import json


@dataclass
class DataSchema:
    """Defines the data schema for Druid ingestion."""
    dataSource: str
    timestampSpec: Dict[str, Any] = field(default_factory=lambda: {
        "column": "timestamp",
        "format": "iso"
    })
    dimensionsSpec: Dict[str, Any] = field(default_factory=lambda: {
        "dimensions": [
            "ticker",
            {"name": "fetch_timestamp", "type": "string"}
        ]
    })
    metricsSpec: List[Dict[str, Any]] = field(default_factory=lambda: [
        {"name": "open", "type": "doubleSum", "fieldName": "open"},
        {"name": "high", "type": "doubleSum", "fieldName": "high"},
        {"name": "low", "type": "doubleSum", "fieldName": "low"},
        {"name": "close", "type": "doubleSum", "fieldName": "close"},
        {"name": "volume", "type": "longSum", "fieldName": "volume"}
    ])
    granularitySpec: Dict[str, Any] = field(default_factory=lambda: {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "MINUTE",
        "rollup": False
    })


@dataclass 
class KafkaIOConfig:
    """Kafka input/output configuration for Druid."""
    topic: str
    consumerProperties: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap.servers": "kafka:9092",
        "group.id": "druid-kafka-indexing-service",
        "auto.offset.reset": "latest"
    })
    taskCount: int = 1
    replicas: int = 1
    taskDuration: str = "PT3600S"  # 1 hour
    useEarliestOffset: bool = False


@dataclass
class TuningConfig:
    """Tuning configuration for Druid ingestion performance."""
    type: str = "kafka"
    maxRowsInMemory: int = 100000
    maxBytesInMemory: int = 134217728  # 128MB
    maxRowsPerSegment: int = 5000000
    intermediatePersistPeriod: str = "PT10M"
    maxPendingPersists: int = 0
    reportParseExceptions: bool = True


@dataclass
class DruidIngestionSpec:
    """
    Complete Druid ingestion specification.
    SRP: Only responsible for Druid ingestion configuration.
    """
    dataSchema: DataSchema
    ioConfig: KafkaIOConfig
    tuningConfig: TuningConfig
    type: str = "kafka"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Druid API submission."""
        return {
            "type": self.type,
            "dataSchema": {
                "dataSource": self.dataSchema.dataSource,
                "timestampSpec": self.dataSchema.timestampSpec,
                "dimensionsSpec": self.dataSchema.dimensionsSpec,
                "metricsSpec": self.dataSchema.metricsSpec,
                "granularitySpec": self.dataSchema.granularitySpec
            },
            "ioConfig": {
                "type": "kafka",
                "topic": self.ioConfig.topic,
                "consumerProperties": self.ioConfig.consumerProperties,
                "taskCount": self.ioConfig.taskCount,
                "replicas": self.ioConfig.replicas,
                "taskDuration": self.ioConfig.taskDuration,
                "useEarliestOffset": self.ioConfig.useEarliestOffset
            },
            "tuningConfig": {
                "type": self.tuningConfig.type,
                "maxRowsInMemory": self.tuningConfig.maxRowsInMemory,
                "maxBytesInMemory": self.tuningConfig.maxBytesInMemory,
                "maxRowsPerSegment": self.tuningConfig.maxRowsPerSegment,
                "intermediatePersistPeriod": self.tuningConfig.intermediatePersistPeriod,
                "maxPendingPersists": self.tuningConfig.maxPendingPersists,
                "reportParseExceptions": self.tuningConfig.reportParseExceptions
            }
        }
    
    def to_json(self) -> str:
        """Convert to JSON string for API submission."""
        return json.dumps(self.to_dict(), indent=2)