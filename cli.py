#!/usr/bin/env python3
"""
Command Line Interface for Snowflake Zero-Copy Cloning Framework

This CLI provides easy access to all framework functionality including:
- Database, schema, and table cloning operations
- RBAC management and setup
- Configuration management
- Monitoring and auditing
"""

import argparse
import json
import sys
import os
from pathlib import Path
import yaml
from datetime import datetime

# Import framework modules
from snowflake_clone_framework import SnowflakeCloneFramework
from rbac_manager import RBACManager


def setup_logging():
    """Setup basic logging for CLI operations."""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def load_config(config_path: str):
    """Load configuration file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: Configuration file {config_path} not found")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid configuration file: {e}")
        sys.exit(1)


def discover_command(args):
    """Handle discover command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        logger.info(f"Discovering structure of {args.database}")
        structure = framework.discover_source_structure(args.database)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(structure, f, indent=2)
            logger.info(f"Discovery results saved to {args.output}")
        else:
            print(json.dumps(structure, indent=2))
            
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def clone_database_command(args):
    """Handle clone database command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        logger.info(f"Cloning database {args.source} to {args.target}")
        success = framework.clone_database(args.source, args.target, args.clone_type)
        
        if success:
            logger.info("Database clone completed successfully")
            
            # Apply RBAC if requested
            if args.apply_rbac:
                logger.info("Applying RBAC configuration")
                rbac_manager = RBACManager(framework.cursor, framework.config)
                rbac_result = rbac_manager.complete_rbac_setup(args.target)
                
                if rbac_result['overall_success']:
                    logger.info("RBAC setup completed successfully")
                else:
                    logger.warning("RBAC setup completed with some failures")
                    if args.output:
                        with open(f"{args.output}_rbac.json", 'w') as f:
                            json.dump(rbac_result, f, indent=2)
        else:
            logger.error("Database clone failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Clone operation failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def clone_schema_command(args):
    """Handle clone schema command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        target_schema = args.target_schema or args.source_schema
        logger.info(f"Cloning schema {args.source_db}.{args.source_schema} to {args.target_db}.{target_schema}")
        
        success = framework.clone_schema(
            args.source_db, 
            args.source_schema, 
            args.target_db, 
            target_schema
        )
        
        if success:
            logger.info("Schema clone completed successfully")
        else:
            logger.error("Schema clone failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Clone operation failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def clone_table_command(args):
    """Handle clone table command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        target_schema = args.target_schema or args.source_schema
        target_table = args.target_table or args.source_table
        
        logger.info(f"Cloning table {args.source_db}.{args.source_schema}.{args.source_table} to {args.target_db}.{target_schema}.{target_table}")
        
        success = framework.clone_table(
            args.source_db,
            args.source_schema,
            args.source_table,
            args.target_db,
            target_schema,
            target_table
        )
        
        if success:
            logger.info("Table clone completed successfully")
        else:
            logger.error("Table clone failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Clone operation failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def bulk_clone_command(args):
    """Handle bulk clone command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        # Load operation configuration
        if args.template:
            # Use predefined template
            templates = framework.config.get('operation_templates', {})
            if args.template not in templates:
                logger.error(f"Template {args.template} not found in configuration")
                sys.exit(1)
            operation_config = templates[args.template]
        else:
            # Load from file
            with open(args.operation_file, 'r') as f:
                operation_config = yaml.safe_load(f)
        
        logger.info("Starting bulk clone operation")
        results = framework.bulk_clone_operation(operation_config)
        
        # Apply RBAC if configured
        if operation_config.get('rbac_apply', False):
            roles_to_grant = operation_config.get('roles_to_grant', [])
            if roles_to_grant:
                logger.info("Applying RBAC configuration")
                # This would need additional implementation to apply RBAC to specific cloned objects
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Bulk clone results saved to {args.output}")
        else:
            print(json.dumps(results, indent=2))
            
        # Summary
        summary = results['summary']
        logger.info(f"Bulk clone completed: {summary['successful_operations']}/{summary['total_operations']} operations successful")
        
        if summary['failed_operations'] > 0:
            logger.warning(f"{summary['failed_operations']} operations failed")
            
    except Exception as e:
        logger.error(f"Bulk clone operation failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def rbac_setup_command(args):
    """Handle RBAC setup command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        rbac_manager = RBACManager(framework.cursor, framework.config)
        
        logger.info(f"Setting up RBAC for database: {args.database}")
        result = rbac_manager.complete_rbac_setup(args.database, args.role_types)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
            logger.info(f"RBAC setup results saved to {args.output}")
        else:
            print(json.dumps(result, indent=2))
        
        if result['overall_success']:
            logger.info("RBAC setup completed successfully")
        else:
            logger.warning("RBAC setup completed with some failures")
            
    except Exception as e:
        logger.error(f"RBAC setup failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def rbac_audit_command(args):
    """Handle RBAC audit command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        rbac_manager = RBACManager(framework.cursor, framework.config)
        
        logger.info(f"Auditing RBAC for database: {args.database}")
        audit_result = rbac_manager.audit_rbac_configuration(args.database)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(audit_result, f, indent=2)
            logger.info(f"RBAC audit results saved to {args.output}")
        else:
            print(json.dumps(audit_result, indent=2))
            
    except Exception as e:
        logger.error(f"RBAC audit failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def validate_command(args):
    """Handle validate command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        logger.info(f"Validating clone: {args.source} -> {args.target}")
        validation_result = framework.validate_clone_operation(args.source, args.target)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(validation_result, f, indent=2)
            logger.info(f"Validation results saved to {args.output}")
        else:
            print(json.dumps(validation_result, indent=2))
        
        if validation_result['overall_status'] == 'SUCCESS':
            logger.info("Clone validation successful")
        else:
            logger.warning("Clone validation failed")
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def history_command(args):
    """Handle history command."""
    logger = setup_logging()
    
    try:
        framework = SnowflakeCloneFramework(args.config)
        framework.connect()
        
        logger.info("Retrieving clone history")
        history = framework.get_clone_history(args.object_name)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(history, f, indent=2)
            logger.info(f"Clone history saved to {args.output}")
        else:
            print(json.dumps(history, indent=2))
            
    except Exception as e:
        logger.error(f"Failed to retrieve clone history: {e}")
        sys.exit(1)
    finally:
        if 'framework' in locals():
            framework.disconnect()


def create_example_config(args):
    """Create an example configuration file."""
    config_template = """# Snowflake Zero-Copy Cloning Framework Configuration
# This is an example configuration file. Customize according to your environment.

snowflake:
  account: "your_account.snowflakecomputing.com"
  user: "your_username"
  password: "your_password"
  warehouse: "COMPUTE_WH"
  database: "PROD_DATALAKE"
  schema: "PUBLIC"
  role: "SYSADMIN"

rbac:
  service_roles:
    - name: "SR_DATA_READER"
      description: "Service role for data reading"
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
"""
    
    with open(args.output, 'w') as f:
        f.write(config_template)
    
    print(f"Example configuration file created: {args.output}")
    print("Please customize the configuration according to your environment.")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Snowflake Zero-Copy Cloning Framework CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover source database structure
  python cli.py discover --database PROD_DATALAKE --output structure.json

  # Clone entire database with RBAC
  python cli.py clone-database --source PROD_DATALAKE --target DEV_DATALAKE --apply-rbac

  # Clone specific schema
  python cli.py clone-schema --source-db PROD_DATALAKE --source-schema AAPC --target-db TEST_DATALAKE

  # Bulk clone using template
  python cli.py bulk-clone --template full_prod_clone --output results.json

  # Setup RBAC for existing database
  python cli.py rbac-setup --database DEV_DATALAKE --output rbac_results.json

  # Audit RBAC configuration
  python cli.py rbac-audit --database DEV_DATALAKE --output audit.json
        """
    )
    
    parser.add_argument(
        '--config', 
        default='config.yaml',
        help='Configuration file path (default: config.yaml)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover database structure')
    discover_parser.add_argument('--database', default='PROD_DATALAKE', help='Source database to discover')
    discover_parser.add_argument('--output', help='Output file for discovery results (JSON)')
    discover_parser.set_defaults(func=discover_command)
    
    # Clone database command
    clone_db_parser = subparsers.add_parser('clone-database', help='Clone entire database')
    clone_db_parser.add_argument('--source', required=True, help='Source database name')
    clone_db_parser.add_argument('--target', required=True, help='Target database name')
    clone_db_parser.add_argument('--clone-type', default='ZERO_COPY', choices=['ZERO_COPY', 'AT_TIME'], help='Clone type')
    clone_db_parser.add_argument('--apply-rbac', action='store_true', help='Apply RBAC configuration after cloning')
    clone_db_parser.add_argument('--output', help='Output file for operation results')
    clone_db_parser.set_defaults(func=clone_database_command)
    
    # Clone schema command
    clone_schema_parser = subparsers.add_parser('clone-schema', help='Clone specific schema')
    clone_schema_parser.add_argument('--source-db', required=True, help='Source database name')
    clone_schema_parser.add_argument('--source-schema', required=True, help='Source schema name')
    clone_schema_parser.add_argument('--target-db', required=True, help='Target database name')
    clone_schema_parser.add_argument('--target-schema', help='Target schema name (default: same as source)')
    clone_schema_parser.set_defaults(func=clone_schema_command)
    
    # Clone table command
    clone_table_parser = subparsers.add_parser('clone-table', help='Clone specific table')
    clone_table_parser.add_argument('--source-db', required=True, help='Source database name')
    clone_table_parser.add_argument('--source-schema', required=True, help='Source schema name')
    clone_table_parser.add_argument('--source-table', required=True, help='Source table name')
    clone_table_parser.add_argument('--target-db', required=True, help='Target database name')
    clone_table_parser.add_argument('--target-schema', help='Target schema name (default: same as source)')
    clone_table_parser.add_argument('--target-table', help='Target table name (default: same as source)')
    clone_table_parser.set_defaults(func=clone_table_command)
    
    # Bulk clone command
    bulk_parser = subparsers.add_parser('bulk-clone', help='Perform bulk clone operations')
    bulk_group = bulk_parser.add_mutually_exclusive_group(required=True)
    bulk_group.add_argument('--template', help='Use predefined template from config')
    bulk_group.add_argument('--operation-file', help='YAML file with bulk operation configuration')
    bulk_parser.add_argument('--output', help='Output file for bulk operation results')
    bulk_parser.set_defaults(func=bulk_clone_command)
    
    # RBAC setup command
    rbac_setup_parser = subparsers.add_parser('rbac-setup', help='Setup RBAC for database')
    rbac_setup_parser.add_argument('--database', required=True, help='Target database for RBAC setup')
    rbac_setup_parser.add_argument('--role-types', nargs='+', choices=['service_roles', 'system_full_roles'], help='Types of roles to create')
    rbac_setup_parser.add_argument('--output', help='Output file for RBAC setup results')
    rbac_setup_parser.set_defaults(func=rbac_setup_command)
    
    # RBAC audit command
    rbac_audit_parser = subparsers.add_parser('rbac-audit', help='Audit RBAC configuration')
    rbac_audit_parser.add_argument('--database', required=True, help='Database to audit')
    rbac_audit_parser.add_argument('--output', help='Output file for audit results')
    rbac_audit_parser.set_defaults(func=rbac_audit_command)
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate clone operation')
    validate_parser.add_argument('--source', required=True, help='Source object name')
    validate_parser.add_argument('--target', required=True, help='Target object name')
    validate_parser.add_argument('--output', help='Output file for validation results')
    validate_parser.set_defaults(func=validate_command)
    
    # History command
    history_parser = subparsers.add_parser('history', help='Show clone history')
    history_parser.add_argument('--object-name', help='Specific object to show history for')
    history_parser.add_argument('--output', help='Output file for history results')
    history_parser.set_defaults(func=history_command)
    
    # Create example config
    config_parser = subparsers.add_parser('create-config', help='Create example configuration file')
    config_parser.add_argument('--output', default='config_example.yaml', help='Output configuration file')
    config_parser.set_defaults(func=create_example_config)
    
    # Parse arguments and execute command
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the selected command
    args.func(args)


if __name__ == "__main__":
    main()