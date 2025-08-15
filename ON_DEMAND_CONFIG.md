# On-Demand Configuration Loading

## Overview
Your test automation framework now loads configuration **only when needed** by specific tests, rather than performing upfront validation. This approach provides flexibility and allows tests to run with minimal setup requirements.

## How It Works

### 🔄 **On-Demand Loading Process**
1. **Test starts** - No configuration loaded initially
2. **Test needs config** - Configuration loaded dynamically when required
3. **Configuration cached** - Subsequent requests use cached values
4. **Helpful errors** - Clear guidance if configuration is missing

### 📁 **Configuration Loading Hierarchy**
1. **Cached values** (fastest) - Already loaded configurations
2. **Direct config.ini reading** - Bypasses environment variable validation
3. **Environment variables** - Only when specifically requested
4. **Helpful error messages** - Guides users to set required values

## Usage in Tests

### Basic Configuration Loading
```gherkin
Given I load configuration from "config.ini"
When I connect to Oracle database using "DEV_ORACLE" configuration
Then the connection should be established
```

### Configuration Values On-Demand
```python
# In step definitions:
from utils.config_helper import load_config_value_when_needed

# Load any config value when needed
primary_key = load_config_value_when_needed(context, 'TEST_SETTINGS', 'primary_key')
chunk_size = load_config_value_when_needed(context, 'DEFAULT', 'chunk_size')
```

### Database Configuration On-Demand
```python
# In step definitions:
from utils.config_helper import load_db_config_when_needed

# Load database config only when connecting
db_config = load_db_config_when_needed(context, 'DEV_ORACLE')
```

## What Changed

### ✅ **Before (Setup Validation)**
- ❌ All configurations validated upfront
- ❌ Tests failed if any config was missing
- ❌ Required all environment variables
- ❌ Slow startup with comprehensive checks

### ✅ **After (On-Demand Loading)**
- ✅ Configuration loaded only when needed
- ✅ Tests run if their specific configs are available
- ✅ Environment variables only required for used databases
- ✅ Fast startup with minimal overhead

## Benefits

### 🚀 **Performance**
- **Faster test startup** - No upfront validation
- **Efficient caching** - Configuration loaded once per test run
- **Minimal overhead** - Only load what tests actually use

### 🔧 **Flexibility**
- **Partial configurations** - Tests run with available configs
- **Environment-specific** - Different configs for different test environments
- **Gradual setup** - Add configurations as needed

### 🛡️ **Error Handling**
- **Clear error messages** - Specific guidance for missing configurations
- **Helpful hints** - Environment variable examples
- **Graceful fallbacks** - Direct file reading when possible

## Examples

### Loading Different Configuration Types

```python
# Load basic configuration values
log_level = load_config_value_when_needed(context, 'DEFAULT', 'log_level')
api_url = load_config_value_when_needed(context, 'API', 'base_url')

# Load database configuration (with environment variables)
oracle_config = load_db_config_when_needed(context, 'DEV_ORACLE', {
    'DEV_ORACLE_PWD': 'your_password'
})

# Load with fallback
try:
    prod_config = load_db_config_when_needed(context, 'PROD_ORACLE')
except ConfigurationError:
    # Fallback to dev config
    dev_config = load_db_config_when_needed(context, 'DEV_ORACLE')
```

### Configuration Caching

```python
# Get cache information
helper = get_config_helper(context)
cache_info = helper.get_cache_info()
print(f"Cached configs: {cache_info['cached_configs']}")

# Clear cache if needed
helper.clear_config_cache()
```

## Error Messages and Troubleshooting

### Common Scenarios

#### Missing Environment Variable
```
❌ Failed to load database config for DEV_ORACLE: Environment variable 'DEV_ORACLE_PWD' not found
💡 Hint: Set environment variable for DEV_ORACLE
   Example: export DEV_ORACLE_PWD=your_password
```

#### Missing Configuration Section
```
❌ Failed to load config TEST_SETTINGS.primary_key: Configuration key 'primary_key' not found
💡 Hint: Check that [TEST_SETTINGS] section exists in config.ini
```

#### Database Connection Issues
```
❌ Oracle connection failed for DEV_ORACLE: Connection refused
💡 Hint: Check if Oracle database is running and accessible
```

## Configuration File Structure

Your `config.ini` remains the same, but now values are loaded on-demand:

```ini
[DEFAULT]
log_level = INFO
chunk_size = 10000

[DEV_ORACLE]
host = dev-oracle-host.com
port = 1521
service_name = DEVDB
username = dev_user
password = DEV_ORACLE_PWD  # Loaded only when connecting

[API]
base_url = https://api.example.com/v1
timeout = 30
```

## Best Practices

1. **Set only needed environment variables** for the tests you're running
2. **Use meaningful section names** in config.ini
3. **Cache configurations** by reusing the same context
4. **Handle errors gracefully** with try/catch where appropriate
5. **Test incrementally** - start with basic configs, add more as needed

## Migration Guide

### For Test Writers
- **No changes needed** - existing Gherkin scenarios work as before
- **Faster feedback** - tests run even with partial configuration
- **Better error messages** - clear guidance when configuration is missing

### For Framework Users  
- **Set environment variables** only for databases you're testing
- **Check error messages** for specific configuration requirements
- **Add configurations** incrementally as you use more features

This on-demand approach makes the framework much more flexible and user-friendly while maintaining all the powerful configuration capabilities!