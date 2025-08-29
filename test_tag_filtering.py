#!/usr/bin/env python3
"""
Test script to demonstrate the tag filtering and HTML report issue
"""
import json
import subprocess
import os
from pathlib import Path

def main():
    print("=== Testing Tag Filtering and HTML Report Issue ===")
    print()
    
    # Test 1: Run with @database tag and see what JSON is generated
    print("1. Testing with @database tag...")
    
    cmd = [
        "python3", "-m", "behave",
        "features/database/data_comparison.feature",
        "--tags", "@database",
        "--format", "json",
        "--outfile", "test_database_only.json",
        "--dry-run"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Behave command succeeded")
            
            # Check what was generated
            if os.path.exists("test_database_only.json"):
                with open("test_database_only.json", 'r') as f:
                    data = json.load(f)
                
                print(f"📊 Features in JSON: {len(data)}")
                
                total_scenarios = 0
                database_scenarios = 0
                
                for feature_data in data:
                    print(f"🔍 Feature: {feature_data.get('name', 'Unknown')}")
                    
                    for element in feature_data.get('elements', []):
                        if element.get('type') == 'scenario':
                            total_scenarios += 1
                            tags = element.get('tags', [])
                            tag_names = [tag.get('name', '') for tag in tags]
                            
                            has_database_tag = any('@database' in tag for tag in tag_names)
                            if has_database_tag:
                                database_scenarios += 1
                                
                            print(f"  📋 Scenario: {element.get('name', 'Unknown')}")
                            print(f"     Tags: {tag_names}")
                            print(f"     Has @database: {has_database_tag}")
                            
                            # Check if scenario has results (was actually run)
                            has_results = any(step.get('result') for step in element.get('steps', []))
                            print(f"     Has results: {has_results}")
                            print()
                
                print(f"📈 Summary:")
                print(f"   Total scenarios in JSON: {total_scenarios}")
                print(f"   Scenarios with @database tag: {database_scenarios}")
                print(f"   This explains why HTML shows all scenarios!")
                
                # Clean up
                os.remove("test_database_only.json")
            else:
                print("❌ No JSON file was generated")
        else:
            print(f"❌ Behave command failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error running test: {e}")
    
    print()
    print("=== Test Complete ===")
    print()
    print("🔍 Root Cause Analysis:")
    print("   - Behave includes ALL scenarios in JSON output (even unmatched ones)")
    print("   - Tag filtering affects execution, but not JSON structure")
    print("   - HTML reporter needs to detect which scenarios were actually executed")
    print("   - Unexecuted scenarios should show as 'skipped', not 'passed'")

if __name__ == "__main__":
    main()