#!/usr/bin/env python3
"""
Test runner script that executes Behave tests and generates HTML reports
"""
import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.html_reporter import generate_html_report_from_json


def run_tests_with_reports(
    feature_paths: list = None, 
    tags: list = None, 
    report_title: str = "Test Automation Report",
    output_dir: str = "output/reports"
):
    """
    Run Behave tests and generate HTML reports
    
    Args:
        feature_paths: List of feature file paths to run (default: all features)
        tags: List of tags to include/exclude
        report_title: Title for the HTML report
        output_dir: Output directory for reports
    """
    print("🚀 Starting Test Execution with HTML Reporting")
    print("=" * 60)
    
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs("output/junit", exist_ok=True)
    
    # Build behave command
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_output = f"{output_dir}/behave_results_{timestamp}.json"
    html_output = f"{output_dir}/test_report_{timestamp}.html"
    
    # Determine the correct Python executable
    python_executable = "python3" if sys.platform.startswith("darwin") or sys.platform.startswith("linux") else "python"
    
    behave_cmd = [
        python_executable, "-m", "behave",  # Use python -m behave for better compatibility
        "--format=json",           # JSON format (must be LAST to go to outfile)
        f"--outfile={json_output}",# JSON output file (override behave.ini)
        "--junit",                 # JUnit XML output
        "--junit-directory=output/junit",
        "--no-capture",            # Don't capture stdout/stderr
        "--no-capture-stderr",
        "--no-logcapture"          # Prevent log capture interfering with JSON
    ]
    
    # Add feature paths
    if feature_paths:
        behave_cmd.extend(feature_paths)
    else:
        behave_cmd.append("features/")
    
    # Add tags - handle both --tags=value and --tags value formats
    if tags:
        for tag in tags:
            if "=" in tag:
                # Format: --tags=@database
                behave_cmd.append(tag)
            else:
                # Format: --tags @database
                behave_cmd.extend(["--tags", tag])
    
    print(f"📋 Command: {' '.join(behave_cmd)}")
    print(f"📄 JSON Output: {json_output}")
    print(f"🌐 HTML Report: {html_output}")
    print()
    
    # Run behave tests
    print("🔄 Executing Tests...")
    print("-" * 40)
    
    try:
        result = subprocess.run(behave_cmd, cwd=project_root, capture_output=False)
        test_exit_code = result.returncode
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False
    
    print()
    print("-" * 40)
    
    # Generate HTML report
    print("📊 Generating HTML Report...")
    
    if os.path.exists(json_output):
        try:
            from utils.html_reporter import HTMLReportGenerator
            generator = HTMLReportGenerator()
            
            if generator.generate_report(json_output, html_output, report_title):
                print(f"✅ HTML Report Generated: {html_output}")
                
                # Try to open report in browser (optional)
                if sys.platform == "darwin":  # macOS
                    subprocess.run(["open", html_output], check=False)
                elif sys.platform.startswith("linux"):  # Linux
                    subprocess.run(["xdg-open", html_output], check=False)
                elif sys.platform == "win32":  # Windows
                    subprocess.run(["start", "", html_output], shell=True, check=False)
            else:
                print("❌ Failed to generate HTML report")
                return False
                
        except Exception as e:
            print(f"❌ Error generating HTML report: {e}")
            return False
    else:
        print(f"❌ JSON output file not found: {json_output}")
        return False
    
    # Summary
    print()
    print("=" * 60)
    print("📈 Test Execution Summary")
    print("=" * 60)
    print(f"Test Exit Code: {test_exit_code}")
    print(f"JSON Results: {json_output}")
    print(f"HTML Report: {html_output}")
    print(f"JUnit XML: output/junit/")
    
    if test_exit_code == 0:
        print("🎉 All tests passed!")
    else:
        print("⚠️ Some tests failed. Check the HTML report for details.")
    
    return test_exit_code == 0


def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description="Run Behave tests with HTML reporting")
    
    parser.add_argument(
        "features", 
        nargs="*", 
        help="Feature files or directories to run (default: all features)"
    )
    
    parser.add_argument(
        "--tags", 
        "-t", 
        action="append", 
        help="Tags to include/exclude (e.g., --tags @database --tags ~@skip)"
    )
    
    parser.add_argument(
        "--title", 
        default="Test Automation Report",
        help="Title for the HTML report"
    )
    
    parser.add_argument(
        "--output", 
        "-o", 
        default="output/reports",
        help="Output directory for reports"
    )
    
    args = parser.parse_args()
    
    # Run tests with reports
    success = run_tests_with_reports(
        feature_paths=args.features if args.features else None,
        tags=args.tags,
        report_title=args.title,
        output_dir=args.output
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()