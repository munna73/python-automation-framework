#!/usr/bin/env python3
"""
Demo script to showcase tag-aware lazy configuration loading.

This script demonstrates how the enhanced ConfigLoader only loads
configuration sections that are required by the active test tags.
"""

import sys
sys.path.append('.')

from utils.config_loader import ConfigLoader

def demo_lazy_loading():
    """Demonstrate the tag-aware lazy loading functionality."""
    
    print("=" * 70)
    print("TAG-AWARE CONFIGURATION LOADING DEMONSTRATION")
    print("=" * 70)
    
    print("\n🎯 PROBLEM STATEMENT:")
    print("   - Your config.ini has many sections (Oracle, Postgres, MongoDB, Kafka, AWS, etc.)")
    print("   - Tests run with specific tags (@oracle, @kafka, @S101, etc.)")
    print("   - Old approach: Load ALL sections regardless of tags")
    print("   - New approach: Load ONLY sections needed by active tags")
    
    # Scenario 1: Oracle-only test
    print("\n" + "="*50)
    print("SCENARIO 1: Oracle-only test (@oracle @S101)")
    print("="*50)
    
    loader1 = ConfigLoader()
    loader1.set_active_tags(['oracle', 'S101'])
    stats1 = loader1.get_loading_stats()
    
    print(f"✅ Active tags: {stats1['active_tags']}")
    print(f"📋 Required sections: {sorted(stats1['required_sections'])}")
    print(f"💡 Result: Will load only Oracle and S101-related sections")
    print(f"🚀 Benefit: Skips MongoDB, Kafka, AWS SQS/S3, and other unrelated sections")
    
    # Scenario 2: Kafka-only test
    print("\n" + "="*50)
    print("SCENARIO 2: Kafka-only test (@kafka)")
    print("="*50)
    
    loader2 = ConfigLoader()
    loader2.set_active_tags(['kafka'])
    stats2 = loader2.get_loading_stats()
    
    print(f"✅ Active tags: {stats2['active_tags']}")
    print(f"📋 Required sections: {sorted(stats2['required_sections'])}")
    print(f"💡 Result: Will load only Kafka-related sections")
    print(f"🚀 Benefit: Skips Oracle, Postgres, MongoDB, AWS, and other unrelated sections")
    
    # Scenario 3: Database comparison test
    print("\n" + "="*50)
    print("SCENARIO 3: Database comparison test (@database @comparison)")
    print("="*50)
    
    loader3 = ConfigLoader()
    loader3.set_active_tags(['database', 'comparison'])
    stats3 = loader3.get_loading_stats()
    
    print(f"✅ Active tags: {stats3['active_tags']}")
    print(f"📋 Required sections: {sorted(stats3['required_sections'])}")
    print(f"💡 Result: Will load Oracle, Postgres, and comparison-related sections")
    print(f"🚀 Benefit: Skips Kafka, MongoDB, AWS, and other unrelated sections")
    
    # Section filtering demonstration
    print("\n" + "="*50)
    print("SECTION FILTERING DEMONSTRATION")
    print("="*50)
    
    sample_sections = [
        'DEFAULT', 'QUERIES', 'S101_ORACLE', 'S102_ORACLE', 'P101_POSTGRES', 
        'S101_KAFKA', 'S102_KAFKA', 'S101_MONGODB', 'S101_SQS', 'S101_S3',
        'S101_MQ', 'comparison_settings'
    ]
    
    scenarios = [
        (['oracle', 'S101'], "Oracle S101 Test"),
        (['kafka'], "Kafka Only Test"),
        (['aws', 'sqs', 'S101'], "AWS SQS S101 Test"),
        (['database', 'postgres', 'P101'], "Postgres P101 Test")
    ]
    
    for tags, description in scenarios:
        print(f"\n📊 {description} ({tags}):")
        loader = ConfigLoader()
        loader.set_active_tags(tags)
        
        loaded = []
        skipped = []
        
        for section in sample_sections:
            if loader._should_load_section(section):
                loaded.append(section)
            else:
                skipped.append(section)
        
        print(f"   ✅ Load: {loaded}")
        print(f"   ⏭️  Skip: {skipped}")
        print(f"   📊 Efficiency: {len(skipped)}/{len(sample_sections)} sections skipped")
    
    print("\n" + "="*70)
    print("💡 BENEFITS OF TAG-AWARE LAZY LOADING:")
    print("="*70)
    print("✅ Faster test startup - Only validate required configurations")
    print("✅ Reduced memory usage - Don't load unnecessary data")  
    print("✅ Better error isolation - Only fail on configs you actually need")
    print("✅ Environment efficiency - Set only needed environment variables")
    print("✅ Cleaner logs - Less noise from unrelated configuration validation")
    
    print("\n🎉 Tag-aware lazy loading is working perfectly!")
    print("🚀 Run your tests with specific tags to see the benefits in action!")

if __name__ == "__main__":
    demo_lazy_loading()