# Multiple Tags Command Line Usage

## 🎯 Multiple Tag Syntax Options

### **1. AND Logic (Tests must have ALL specified tags)**

```bash
# Basic AND - tests with BOTH @abctest AND @smoke
behave --tags="@abctest and @smoke"

# Multiple AND - tests with ALL three tags
behave --tags="@abctest and @smoke and @critical"

# Framework scripts with AND logic
./run_tests.sh --tags "@abctest and @smoke"
run_tests.bat --tags "@abctest and @regression"
```

### **2. OR Logic (Tests with ANY of the specified tags)**

```bash
# Basic OR - tests with EITHER @abctest OR @xyztest  
behave --tags="@abctest or @xyztest"

# Multiple OR - tests with ANY of these tags
behave --tags="@smoke or @critical or @regression"

# Framework scripts with OR logic
./run_tests.sh --tags "@abctest or @xyztest"
run_tests.bat --tags "@smoke or @critical"
```

### **3. NOT Logic (Exclude specific tags)**

```bash
# Exclude slow tests
behave --tags="@abctest and not @slow"

# Exclude multiple types
behave --tags="@regression and not (@slow or @performance)"

# Framework scripts with NOT logic  
./run_tests.sh --tags "@abctest and not @performance"
run_tests.bat --tags "not @slow"
```

### **4. Complex Combinations**

```bash
# ABC tests that are either smoke OR critical, but not slow
behave --tags="@abctest and (@smoke or @critical) and not @slow"

# Either ABC smoke tests OR XYZ regression tests
behave --tags="(@abctest and @smoke) or (@xyztest and @regression)"

# All regression tests except performance and slow ones
behave --tags="@regression and not (@performance or @slow)"
```

## 🔧 Practical Examples

### **Scenario 1: Your ABC Test Suite**

```bash
# Run all ABC tests
behave --tags=@abctest
./run_tests.sh --tags "@abctest"
run_tests.bat --tags "@abctest"

# Run ABC smoke tests only
behave --tags="@abctest and @smoke"

# Run ABC tests but exclude slow ones
behave --tags="@abctest and not @slow"

# Run ABC critical tests (high priority)
behave --tags="@abctest and @critical"
```

### **Scenario 2: Multi-Component Testing**

```bash
# Run both ABC and XYZ tests
behave --tags="@abctest or @xyztest"

# Run integration tests between ABC and XYZ
behave --tags="@integration and (@abctest or @xyztest)"

# Run smoke tests for all components
behave --tags="@smoke and (@abctest or @xyztest or @database or @api)"
```

### **Scenario 3: Test Phase Execution**

```bash
# Phase 1: Quick validation
behave --tags="@smoke and not @slow"

# Phase 2: Component testing  
behave --tags="@abctest or @database or @api"

# Phase 3: Integration testing
behave --tags="@integration and not @performance"

# Phase 4: Full regression (excluding performance)
behave --tags="@regression and not (@performance or @slow)"
```

## 🪟 Windows-Specific Commands

### **Command Prompt (cmd):**
```cmd
REM AND logic
behave --tags="@abctest and @smoke"
run_tests.bat --tags "@abctest and @smoke"

REM OR logic  
behave --tags="@abctest or @xyztest"
run_tests.bat --tags "@abctest or @xyztest"

REM NOT logic
behave --tags="@abctest and not @slow"
run_tests.bat --tags "@abctest and not @slow"

REM Complex combinations
behave --tags="(@abctest and @smoke) or (@xyztest and @critical)"
```

### **PowerShell:**
```powershell
# AND logic
behave --tags="@abctest and @smoke"
.\run_tests.ps1 -Tags "@abctest and @smoke"

# OR logic
behave --tags="@abctest or @xyztest"  
.\run_tests.ps1 -Tags "@abctest or @xyztest"

# NOT logic
behave --tags="@abctest and not @slow"
.\run_tests.ps1 -Tags "@abctest and not @slow"

# Complex combinations
behave --tags="(@abctest and @smoke) or (@regression and @critical)"
```

## 📊 Framework Script Usage

### **Linux/macOS Shell Script:**
```bash
# Single tag
./run_tests.sh --tags "@abctest"

# Multiple tags with AND
./run_tests.sh --tags "@abctest and @smoke"

# Multiple tags with OR
./run_tests.sh --tags "@abctest or @xyztest"

# Complex logic
./run_tests.sh --tags "(@abctest and @smoke) or (@database and @critical)"

# With custom report title
./run_tests.sh --tags "@abctest and @regression" --title "ABC Regression Tests"
```

### **Windows Batch Script:**
```cmd
REM Single tag
run_tests.bat --tags "@abctest"

REM Multiple tags with AND
run_tests.bat --tags "@abctest and @smoke"

REM Multiple tags with OR  
run_tests.bat --tags "@abctest or @xyztest"

REM Complex logic
run_tests.bat --tags "(@abctest and @smoke) or (@database and @critical)"

REM With custom report title
run_tests.bat --tags "@abctest and @regression" --title "ABC Regression Tests"
```

### **Python Script Direct:**
```bash
# Single tag
python3 scripts/run_tests_with_reports.py --tags "@abctest"

# Multiple tags with AND
python3 scripts/run_tests_with_reports.py --tags "@abctest and @smoke"

# Multiple tags with OR
python3 scripts/run_tests_with_reports.py --tags "@abctest or @xyztest"

# Complex combinations
python3 scripts/run_tests_with_reports.py --tags "(@abctest and @smoke) or (@xyztest and @critical)"
```

## 🎯 Tag Validation Commands

### **Check which tests will run (dry-run):**
```bash
# See which tests match your tag criteria
behave --dry-run --tags="@abctest and @smoke"
behave --dry-run --tags="@abctest or @xyztest"
behave --dry-run --tags="@regression and not @slow"

# Count matching tests
behave --dry-run --tags="@abctest" | grep -c "Scenario"
```

### **List all available tags:**
```bash
# Find all tags in the project
grep -r "@\w*" features/ | grep -o "@\w*" | sort | uniq

# Find tags in specific feature file
grep -o "@\w*" features/sample_tagged_tests.feature
```

## 📈 Performance Considerations

### **Optimize tag combinations for speed:**
```bash
# Fast execution - only quick tests
behave --tags="@abctest and @fast and not @slow"

# Medium execution - exclude only slow tests  
behave --tags="@abctest and not @slow"

# Full execution - include everything
behave --tags="@abctest"
```

## 🧪 Testing Your Tag Logic

Create a simple test to verify your tag combinations work:

```bash
# Test 1: Verify @abctest exists
behave --dry-run --tags="@abctest"

# Test 2: Check combination logic
behave --dry-run --tags="@abctest and @smoke"

# Test 3: Validate NOT logic
behave --dry-run --tags="@abctest and not @slow"

# Test 4: Complex validation
behave --dry-run --tags="(@abctest and @smoke) or (@xyztest and @critical)"
```

## 💡 Pro Tips

### **1. Use parentheses for clarity:**
```bash
# Clear precedence with parentheses
behave --tags="(@abctest and @smoke) or (@xyztest and @critical)"

# Ambiguous without parentheses
behave --tags="@abctest and @smoke or @xyztest and @critical"
```

### **2. Quote your tag expressions:**
```bash
# Always use quotes to prevent shell interpretation
behave --tags="@abctest and @smoke"  ✅
behave --tags=@abctest and @smoke    ❌
```

### **3. Test incrementally:**
```bash
# Start simple
behave --tags="@abctest"

# Add complexity gradually  
behave --tags="@abctest and @smoke"
behave --tags="@abctest and @smoke and not @slow"
```

This gives you complete control over running multiple tagged tests from the command line! 🎯