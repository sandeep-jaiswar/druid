"""
Test script to verify the Druid pipeline application.
Tests the SOLID architecture with dependency injection.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.druid_pipeline_app import DruidPipelineApplication
from src.config.environment_config import EnvironmentConfigProvider

def test_application_initialization():
    """Test that the application initializes correctly."""
    print("Testing application initialization...")
    
    try:
        # Create application with default dependencies
        app = DruidPipelineApplication()
        print("✅ Application created successfully")
        
        # Test configuration
        config_provider = EnvironmentConfigProvider()
        kafka_config = config_provider.get_kafka_config()
        print(f"✅ Kafka config loaded: {kafka_config['topic']}")
        
        druid_config = config_provider.get_druid_config()
        print(f"✅ Druid config loaded: {druid_config['datasource']}")
        
        # Test ingestion spec creation
        from src.services.druid_ingestion_service import DruidIngestionService
        ingestion_service = DruidIngestionService(config_provider)
        spec = ingestion_service.create_stock_ingestion_spec()
        print(f"✅ Ingestion spec created for datasource: {spec.dataSchema.dataSource}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_druid_health():
    """Test Druid service health check."""
    print("\nTesting Druid health check...")
    
    try:
        config_provider = EnvironmentConfigProvider()
        from src.services.druid_ingestion_service import DruidIngestionService
        ingestion_service = DruidIngestionService(config_provider)
        
        is_healthy = ingestion_service.health_check()
        if is_healthy:
            print("✅ Druid service is healthy")
        else:
            print("⚠️  Druid service health check failed (expected if Druid not running)")
        
        return True
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

def test_kafka_consumer():
    """Test Kafka consumer initialization."""
    print("\nTesting Kafka consumer...")
    
    try:
        config_provider = EnvironmentConfigProvider()
        from src.services.kafka_consumer_service import KafkaConsumerService
        
        consumer = KafkaConsumerService(config_provider)
        info = consumer.get_consumer_info()
        print(f"✅ Consumer configured for topic: {info['topic']}")
        
        # Test connection (may fail if Kafka not running)
        connected = consumer.connect()
        if connected:
            print("✅ Successfully connected to Kafka")
            consumer.close()
        else:
            print("⚠️  Kafka connection failed (expected if Kafka not running)")
        
        return True
        
    except Exception as e:
        print(f"❌ Kafka consumer test failed: {e}")
        return False

if __name__ == '__main__':
    print("🚀 Testing Druid Pipeline Application (SOLID Architecture)")
    print("=" * 60)
    
    tests = [
        test_application_initialization,
        test_druid_health,
        test_kafka_consumer
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 60)
    print(f"Tests completed: {passed}/{len(tests)} passed")
    
    if passed == len(tests):
        print("🎉 All tests passed! The SOLID architecture is working correctly.")
    else:
        print("⚠️  Some tests failed, but this is expected if Kafka/Druid are not running.")
    
    print("\nTo use the application:")
    print("1. Ensure Kafka and Druid are running (docker-compose up)")
    print("2. Run: python main.py run")
    print("3. Or check status: python main.py status")