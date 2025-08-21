"""
HTML Report Generator for Test Automation Framework
Generates comprehensive HTML reports from Behave JSON output
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import html


class HTMLReportGenerator:
    """Generate HTML reports from Behave test results"""
    
    def __init__(self):
        self.template = self._get_html_template()
    
    def generate_report(self, json_file: str, output_file: str, title: str = "Test Automation Report") -> bool:
        """
        Generate HTML report from Behave JSON output
        
        Args:
            json_file: Path to Behave JSON output file
            output_file: Path for generated HTML report
            title: Report title
            
        Returns:
            True if report generated successfully
        """
        try:
            # Load JSON data
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Parse test results
            report_data = self._parse_behave_results(data)
            report_data['title'] = title
            report_data['generated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Generate HTML
            html_content = self._render_html(report_data)
            
            # Write HTML file
            output_dir = os.path.dirname(os.path.abspath(output_file))
            os.makedirs(output_dir, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✓ HTML report generated: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to generate HTML report: {e}")
            return False
    
    def _parse_behave_results(self, data: List[Dict]) -> Dict[str, Any]:
        """Parse Behave JSON results into report data structure"""
        total_features = len(data)
        total_scenarios = 0
        passed_scenarios = 0
        failed_scenarios = 0
        skipped_scenarios = 0
        total_steps = 0
        passed_steps = 0
        failed_steps = 0
        skipped_steps = 0
        
        features = []
        
        for feature_data in data:
            feature = {
                'name': feature_data.get('name', 'Unknown Feature'),
                'description': feature_data.get('description', ''),
                'location': feature_data.get('location', ''),
                'status': 'passed',
                'scenarios': []
            }
            
            feature_failed = False
            
            for element in feature_data.get('elements', []):
                if element.get('type') == 'scenario':
                    total_scenarios += 1
                    
                    scenario = {
                        'name': element.get('name', 'Unknown Scenario'),
                        'location': element.get('location', ''),
                        'status': 'passed',
                        'steps': [],
                        'duration': 0,
                        'tags': element.get('tags', [])
                    }
                    
                    scenario_failed = False
                    scenario_skipped = False
                    scenario_duration = 0
                    
                    # Check if scenario has any steps with results - if not, it was likely skipped due to tags
                    has_executed_steps = any(step.get('result') for step in element.get('steps', []))
                    if not has_executed_steps and element.get('steps'):
                        scenario_skipped = True
                    
                    for step in element.get('steps', []):
                        total_steps += 1
                        step_status = step.get('result', {}).get('status', 'unknown')
                        step_duration = step.get('result', {}).get('duration', 0)
                        scenario_duration += step_duration
                        
                        step_data = {
                            'keyword': step.get('keyword', ''),
                            'name': step.get('name', ''),
                            'location': step.get('location', ''),
                            'status': step_status,
                            'duration': step_duration,
                            'error_message': ''
                        }
                        
                        # Handle step status
                        if step_status == 'passed':
                            passed_steps += 1
                        elif step_status == 'failed':
                            failed_steps += 1
                            scenario_failed = True
                            feature_failed = True
                            # Get error message
                            error_msg = step.get('result', {}).get('error_message', '')
                            step_data['error_message'] = error_msg
                        elif step_status in ['skipped', 'undefined', 'untested']:
                            skipped_steps += 1
                        else:
                            # Handle cases where there's no result (not executed due to tags)
                            skipped_steps += 1
                        
                        scenario['steps'].append(step_data)
                    
                    scenario['duration'] = scenario_duration
                    
                    # Determine scenario status
                    if scenario_skipped:
                        scenario['status'] = 'skipped'
                        skipped_scenarios += 1
                    elif scenario_failed:
                        scenario['status'] = 'failed'
                        failed_scenarios += 1
                    else:
                        scenario['status'] = 'passed'
                        passed_scenarios += 1
                    
                    feature['scenarios'].append(scenario)
            
            feature['status'] = 'failed' if feature_failed else 'passed'
            features.append(feature)
        
        return {
            'features': features,
            'summary': {
                'total_features': total_features,
                'total_scenarios': total_scenarios,
                'passed_scenarios': passed_scenarios,
                'failed_scenarios': failed_scenarios,
                'skipped_scenarios': skipped_scenarios,
                'total_steps': total_steps,
                'passed_steps': passed_steps,
                'failed_steps': failed_steps,
                'skipped_steps': skipped_steps,
                'success_rate': round((passed_scenarios / total_scenarios * 100) if total_scenarios > 0 else 0, 1)
            }
        }
    
    def _render_html(self, data: Dict[str, Any]) -> str:
        """Render HTML content from data"""
        summary = data['summary']
        
        # Generate summary section
        summary_html = f"""
        <div class="summary">
            <h2>Test Execution Summary</h2>
            <div class="summary-grid">
                <div class="summary-item">
                    <div class="summary-number">{summary['total_scenarios']}</div>
                    <div class="summary-label">Total Scenarios</div>
                </div>
                <div class="summary-item passed">
                    <div class="summary-number">{summary['passed_scenarios']}</div>
                    <div class="summary-label">Passed</div>
                </div>
                <div class="summary-item failed">
                    <div class="summary-number">{summary['failed_scenarios']}</div>
                    <div class="summary-label">Failed</div>
                </div>
                <div class="summary-item">
                    <div class="summary-number">{summary['success_rate']}%</div>
                    <div class="summary-label">Success Rate</div>
                </div>
            </div>
        </div>
        """
        
        # Generate features section
        features_html = ""
        for feature in data['features']:
            feature_class = f"feature {feature['status']}"
            scenarios_html = ""
            
            for scenario in feature['scenarios']:
                scenario_class = f"scenario {scenario['status']}"
                
                # Generate tags HTML
                tags_html = ""
                if scenario.get('tags'):
                    tags_html = '<div class="scenario-tags">'
                    for tag in scenario['tags']:
                        tags_html += f'<span class="tag">{html.escape(tag)}</span>'
                    tags_html += '</div>'
                
                steps_html = ""
                
                for step in scenario['steps']:
                    step_class = f"step {step['status']}"
                    error_html = ""
                    if step['error_message']:
                        error_html = f"""<div class="error-message">{html.escape(step['error_message'])}</div>"""
                    
                    steps_html += f"""
                    <div class="{step_class}">
                        <span class="step-keyword">{step['keyword']}</span>
                        <span class="step-name">{html.escape(step['name'])}</span>
                        <span class="step-duration">{step['duration']:.3f}s</span>
                        {error_html}
                    </div>
                    """
                
                scenarios_html += f"""
                <div class="{scenario_class}">
                    <h4 class="scenario-name">
                        <span class="status-icon">{'✓' if scenario['status'] == 'passed' else '✗'}</span>
                        {html.escape(scenario['name'])}
                        <span class="scenario-duration">({scenario['duration']:.3f}s)</span>
                    </h4>
                    {tags_html}
                    <div class="steps">
                        {steps_html}
                    </div>
                </div>
                """
            
            features_html += f"""
            <div class="{feature_class}">
                <h3 class="feature-name">
                    <span class="status-icon">{'✓' if feature['status'] == 'passed' else '✗'}</span>
                    {html.escape(feature['name'])}
                </h3>
                <div class="scenarios">
                    {scenarios_html}
                </div>
            </div>
            """
        
        # Replace template placeholders
        html_content = self.template.replace('{{TITLE}}', html.escape(data['title']))
        html_content = html_content.replace('{{GENERATED_AT}}', data['generated_at'])
        html_content = html_content.replace('{{SUMMARY}}', summary_html)
        html_content = html_content.replace('{{FEATURES}}', features_html)
        
        return html_content
    
    def _get_html_template(self) -> str:
        """Get HTML template for report"""
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{TITLE}}</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 0;
            margin-bottom: 30px;
            border-radius: 10px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .summary {
            background: white;
            padding: 25px;
            margin-bottom: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .summary h2 {
            color: #333;
            margin-bottom: 20px;
            font-size: 1.5em;
        }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }
        
        .summary-item {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
            border: 2px solid #e9ecef;
        }
        
        .summary-item.passed {
            background: #d4edda;
            border-color: #28a745;
        }
        
        .summary-item.failed {
            background: #f8d7da;
            border-color: #dc3545;
        }
        
        .summary-number {
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
        }
        
        .summary-label {
            font-size: 1em;
            color: #666;
            margin-top: 5px;
        }
        
        .feature {
            background: white;
            margin-bottom: 25px;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .feature-name {
            background: #f8f9fa;
            padding: 20px;
            margin: 0;
            font-size: 1.3em;
            color: #333;
            border-bottom: 2px solid #e9ecef;
        }
        
        .feature.failed .feature-name {
            background: #f8d7da;
            border-bottom-color: #dc3545;
        }
        
        .feature.passed .feature-name {
            background: #d4edda;
            border-bottom-color: #28a745;
        }
        
        .status-icon {
            margin-right: 10px;
            font-size: 1.2em;
        }
        
        .scenario {
            border-bottom: 1px solid #e9ecef;
            padding: 15px 20px;
        }
        
        .scenario:last-child {
            border-bottom: none;
        }
        
        .scenario-name {
            font-size: 1.1em;
            margin-bottom: 15px;
            color: #333;
        }
        
        .scenario-duration {
            font-size: 0.9em;
            color: #666;
            font-weight: normal;
        }
        
        .steps {
            margin-left: 20px;
        }
        
        .step {
            padding: 8px 0;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.9em;
            display: flex;
            align-items: center;
        }
        
        .step-keyword {
            font-weight: bold;
            color: #6f42c1;
            margin-right: 8px;
            min-width: 60px;
        }
        
        .step-name {
            flex: 1;
            color: #333;
        }
        
        .step-duration {
            margin-left: auto;
            color: #666;
            font-size: 0.8em;
        }
        
        .step.passed {
            color: #28a745;
        }
        
        .step.failed {
            color: #dc3545;
            background: #fff5f5;
            padding: 10px;
            border-radius: 5px;
            margin: 5px 0;
        }
        
        .error-message {
            margin-top: 10px;
            padding: 10px;
            background: #fee;
            border: 1px solid #fcc;
            border-radius: 4px;
            font-size: 0.85em;
            white-space: pre-wrap;
            color: #c33;
        }
        
        .scenario-tags {
            margin: 10px 0;
        }
        
        .tag {
            display: inline-block;
            background: #e9ecef;
            color: #495057;
            padding: 4px 8px;
            margin: 2px 4px 2px 0;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 500;
            border: 1px solid #dee2e6;
        }
        
        .tag:first-child {
            margin-left: 0;
        }
        
        .footer {
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            color: #666;
            border-top: 1px solid #e9ecef;
        }
        
        @media (max-width: 768px) {
            .container {
                padding: 10px;
            }
            
            .header h1 {
                font-size: 2em;
            }
            
            .summary-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{TITLE}}</h1>
            <p>Generated on {{GENERATED_AT}}</p>
        </div>
        
        {{SUMMARY}}
        
        <div class="features">
            {{FEATURES}}
        </div>
        
        <div class="footer">
            <p>Test Automation Framework - HTML Report</p>
        </div>
    </div>
</body>
</html>"""


def generate_html_report_from_json(json_file: str, output_dir: str = "output/reports", title: str = "Test Automation Report") -> str:
    """
    Convenience function to generate HTML report from Behave JSON output
    
    Args:
        json_file: Path to Behave JSON output file
        output_dir: Directory for HTML report
        title: Report title
        
    Returns:
        Path to generated HTML report
    """
    generator = HTMLReportGenerator()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/test_report_{timestamp}.html"
    
    if generator.generate_report(json_file, output_file, title):
        return output_file
    return ""


if __name__ == "__main__":
    # Test the HTML reporter
    import sys
    
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "output/reports/test_report.html"
        
        generator = HTMLReportGenerator()
        generator.generate_report(json_file, output_file)
    else:
        print("Usage: python html_reporter.py <json_file> [output_file]")