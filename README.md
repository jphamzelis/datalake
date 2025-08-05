# Snowflake Zero-Copy Cloning Framework

A comprehensive framework for performing zero-copy cloning operations from PROD_DATALAKE to new databases in Snowflake, with integrated Role-Based Access Control (RBAC) management for Service Role (SR) and System Full (SFULL) permissions.

## Features

- **Zero-Copy Cloning**: Efficiently clone databases, schemas, and tables using Snowflake's zero-copy cloning technology
- **RBAC Management**: Automated setup and management of Service Role (SR) and System Full (SFULL) permissions
- **Configuration-Driven**: YAML-based configuration for flexible and maintainable operations
- **CLI Interface**: Easy-to-use command-line interface for all operations
- **Bulk Operations**: Support for bulk cloning operations with templates
- **Validation & Monitoring**: Built-in validation, logging, and monitoring capabilities
- **Audit Trail**: Complete audit trail of cloning operations and RBAC changes

## Architecture

The framework consists of several key components:

- **SnowflakeCloneFramework**: Main framework class handling cloning operations
- **RBACManager**: Dedicated RBAC management for role creation and privilege assignment
- **CLI Interface**: Command-line interface for interactive operations
- **Configuration System**: YAML-based configuration management
- **Logging & Monitoring**: Comprehensive logging and optional monitoring integration

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd snowflake-clone-framework
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Create configuration file**:
   ```bash
   python cli.py create-config --output config.yaml
   ```

4. **Edit configuration file** with your Snowflake credentials and settings.

## Configuration

### Basic Configuration

Edit `config.yaml` with your Snowflake connection details:

```yaml
snowflake:
  account: "your_account.snowflakecomputing.com"
  user: "your_username"
  password: "your_password"
  warehouse: "COMPUTE_WH"
  database: "PROD_DATALAKE"
  schema: "PUBLIC"
  role: "SYSADMIN"
```

### RBAC Configuration

Configure Service Role (SR) and System Full (SFULL) roles:

```yaml
rbac:
  service_roles:
    - name: "SR_DATA_READER"
      description: "Service role for data reading operations"
      privileges:
        databases:
          - privilege: "USAGE"
            objects: ["${TARGET_DATABASE}"]
        schemas:
          - privilege: "USAGE"
            objects: ["${TARGET_DATABASE}.*"]
        tables:
          - privilege: "SELECT"
            objects: ["${TARGET_DATABASE}.*.*"]

  system_full_roles:
    - name: "SFULL_ADMIN"
      description: "System full administrative role"
      privileges:
        databases:
          - privilege: "ALL"
            objects: ["${TARGET_DATABASE}"]
```

## Usage

### Command Line Interface

The framework provides a comprehensive CLI for all operations:

#### Discover Database Structure

```bash
# Discover structure of PROD_DATALAKE
python cli.py discover --database PROD_DATALAKE --output structure.json
```

#### Clone Entire Database

```bash
# Clone entire database with RBAC setup
python cli.py clone-database \
  --source PROD_DATALAKE \
  --target DEV_DATALAKE \
  --apply-rbac \
  --output clone_results.json
```

#### Clone Specific Schema

```bash
# Clone specific schema
python cli.py clone-schema \
  --source-db PROD_DATALAKE \
  --source-schema AAPC \
  --target-db TEST_DATALAKE \
  --target-schema AAPC
```

#### Clone Specific Table

```bash
# Clone specific table
python cli.py clone-table \
  --source-db PROD_DATALAKE \
  --source-schema ADAPTIVE \
  --source-table ACCOUNTS \
  --target-db SANDBOX_DATALAKE \
  --target-schema ADAPTIVE \
  --target-table ACCOUNTS
```

#### Bulk Clone Operations

```bash
# Use predefined template
python cli.py bulk-clone \
  --template full_prod_clone \
  --output bulk_results.json

# Use custom operation file
python cli.py bulk-clone \
  --operation-file my_clone_operations.yaml \
  --output bulk_results.json
```

#### RBAC Management

```bash
# Setup RBAC for a database
python cli.py rbac-setup \
  --database DEV_DATALAKE \
  --output rbac_results.json

# Audit RBAC configuration
python cli.py rbac-audit \
  --database DEV_DATALAKE \
  --output audit_results.json
```

#### Validation and History

```bash
# Validate clone operation
python cli.py validate \
  --source PROD_DATALAKE.AAPC.AAPC_APC \
  --target DEV_DATALAKE.AAPC.AAPC_APC \
  --output validation_results.json

# View clone history
python cli.py history \
  --object-name DEV_DATALAKE \
  --output history.json
```

### Python API

You can also use the framework programmatically:

```python
from snowflake_clone_framework import SnowflakeCloneFramework
from rbac_manager import RBACManager

# Initialize framework
framework = SnowflakeCloneFramework('config.yaml')
framework.connect()

try:
    # Discover source structure
    structure = framework.discover_source_structure('PROD_DATALAKE')
    
    # Clone database
    success = framework.clone_database('PROD_DATALAKE', 'DEV_DATALAKE')
    
    if success:
        # Setup RBAC
        rbac_manager = RBACManager(framework.cursor, framework.config)
        rbac_result = rbac_manager.complete_rbac_setup('DEV_DATALAKE')
        
finally:
    framework.disconnect()
```

## Templates and Bulk Operations

### Predefined Templates

The framework includes several predefined templates:

- **full_prod_clone**: Clone entire PROD_DATALAKE database
- **schema_clone**: Clone specific schemas
- **table_clone**: Clone specific tables

### Custom Operation Files

Create custom YAML files for complex operations:

```yaml
description: "Custom clone operation"
databases:
  - source: "PROD_DATALAKE"
    target: "CUSTOM_DATALAKE"

schemas:
  - source_db: "PROD_DATALAKE"
    source_schema: "AAPC"
    target_db: "CUSTOM_DATALAKE"
    target_schema: "AAPC_CLONE"

rbac_apply: true
roles_to_grant: ["SR_DATA_READER", "SR_DATA_WRITER"]
```

## RBAC Concepts

### Service Roles (SR)

Service roles are designed for application and service access:

- **SR_DATA_READER**: Read-only access to data
- **SR_DATA_WRITER**: Read and write access to data
- **SR_DATA_ADMIN**: Administrative access within schemas

### System Full Roles (SFULL)

System full roles provide comprehensive administrative access:

- **SFULL_ADMIN**: Complete administrative control
- **SFULL_OPERATOR**: Operational administrative access
- **SFULL_MONITOR**: Monitoring and auditing access

### Role Hierarchy

The framework establishes a hierarchical role structure:

```
SFULL_ADMIN
├── SFULL_OPERATOR
│   └── SR_DATA_WRITER
│       └── SR_DATA_READER
└── Direct assignments
```

## Monitoring and Logging

### Logging Configuration

Configure logging levels and output:

```yaml
logging:
  level: "INFO"
  file: "snowflake_clone.log"
  max_file_size: "10MB"
  backup_count: 5
```

### Monitoring Metrics

The framework can track various metrics:

- Clone success rate
- Clone duration
- RBAC application success
- Storage usage
- Operation frequency

### Alerting

Configure alerts for failures:

```yaml
monitoring:
  alerts:
    clone_failure_threshold: 0.1  # Alert if >10% fail
    rbac_failure_threshold: 0.05  # Alert if >5% fail
```

## Security Features

### Authentication

Supports multiple authentication methods:

- Username/password
- Key-pair authentication
- SSO/External authentication

### Access Control

- IP-based restrictions
- Time-based operation windows
- Audit logging
- Sensitive data masking

### Security Configuration

```yaml
security:
  audit_logging: true
  encrypt_logs: false
  sensitive_data_masking: true
  
  allowed_ips:
    - "10.0.0.0/8"
    - "192.168.0.0/16"
  
  operation_windows:
    - start_time: "22:00"
      end_time: "06:00"
      timezone: "UTC"
      allowed_operations: ["clone_database", "clone_schema"]
```

## Performance Optimization

### Parallel Operations

Configure parallel processing:

```yaml
performance:
  parallel_operations: 5
  warehouse_scaling:
    auto_scale: true
    min_cluster: 1
    max_cluster: 3
```

### Resource Management

Set up resource monitors:

```yaml
performance:
  resource_monitors:
    enabled: true
    credit_quota: 1000
    frequency: "DAILY"
```

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify Snowflake credentials
   - Check network connectivity
   - Validate account name format

2. **Permission Errors**
   - Ensure user has required privileges
   - Check role assignments
   - Verify warehouse access

3. **Clone Failures**
   - Verify source objects exist
   - Check storage quotas
   - Validate naming conventions

### Debug Mode

Enable debug logging:

```yaml
logging:
  level: "DEBUG"
```

### Validation Tools

Use built-in validation:

```bash
python cli.py validate --source SOURCE --target TARGET
```

## Best Practices

### Security
- Use key-pair authentication in production
- Regularly rotate credentials
- Implement least-privilege access
- Enable audit logging

### Operations
- Test cloning operations in development first
- Use descriptive naming conventions
- Monitor resource usage
- Schedule operations during low-usage periods

### Configuration Management
- Version control configuration files
- Use environment-specific configurations
- Document role assignments
- Regular RBAC audits

## API Reference

### SnowflakeCloneFramework Class

#### Methods

- `connect()`: Establish Snowflake connection
- `disconnect()`: Close Snowflake connection
- `discover_source_structure(database)`: Discover database structure
- `clone_database(source, target, type)`: Clone entire database
- `clone_schema(source_db, source_schema, target_db, target_schema)`: Clone schema
- `clone_table(source_db, source_schema, source_table, target_db, target_schema, target_table)`: Clone table
- `bulk_clone_operation(config)`: Perform bulk operations
- `validate_clone_operation(source, target)`: Validate clone
- `get_clone_history(object_name)`: Get clone history

### RBACManager Class

#### Methods

- `create_service_roles(database)`: Create SR roles
- `create_system_full_roles(database)`: Create SFULL roles
- `apply_role_privileges(database, role_types)`: Apply privileges
- `setup_role_hierarchy()`: Setup role hierarchy
- `assign_users_to_roles(assignments)`: Assign users
- `complete_rbac_setup(database, role_types)`: Complete setup
- `audit_rbac_configuration(database)`: Audit configuration

## Support

For issues and questions:

1. Check the troubleshooting section
2. Review log files for error details
3. Validate configuration settings
4. Test with minimal configurations

## License

This framework is provided as-is for educational and operational use. Please ensure compliance with your organization's policies and Snowflake's terms of service.

## Contributing

To contribute to this framework:

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit a pull request

## Changelog

### Version 1.0.0
- Initial release
- Zero-copy cloning functionality
- RBAC management
- CLI interface
- Configuration system
- Logging and monitoring