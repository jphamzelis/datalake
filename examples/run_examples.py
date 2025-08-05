#!/usr/bin/env python3
"""
Example scripts for Snowflake Zero-Copy Cloning Framework

This file demonstrates how to use the framework programmatically
for various cloning and RBAC operations.
"""

import sys
import os
import json
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snowflake_clone_framework import SnowflakeCloneFramework
from rbac_manager import RBACManager


def example_discover_database():
    """Example: Discover database structure."""
    print("=== Example: Discover Database Structure ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Discover structure
        structure = framework.discover_source_structure('PROD_DATALAKE')
        
        print(f"Found {structure['total_tables']} tables across {len(structure['schemas'])} schemas")
        
        # Display schema summary
        for schema_name, schema_info in structure['schemas'].items():
            print(f"  Schema {schema_name}: {schema_info['table_count']} tables")
        
        # Save to file
        with open('discovery_results.json', 'w') as f:
            json.dump(structure, f, indent=2)
        
        print("Discovery results saved to discovery_results.json")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_clone_database():
    """Example: Clone entire database."""
    print("\n=== Example: Clone Database ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Clone database
        source_db = "PROD_DATALAKE"
        target_db = "DEV_DATALAKE"
        
        print(f"Cloning {source_db} to {target_db}...")
        success = framework.clone_database(source_db, target_db)
        
        if success:
            print("Database clone completed successfully!")
            
            # Validate the clone
            validation = framework.validate_clone_operation(source_db, target_db)
            print(f"Validation status: {validation['overall_status']}")
            
        else:
            print("Database clone failed!")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_clone_schema():
    """Example: Clone specific schema."""
    print("\n=== Example: Clone Schema ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Clone specific schema
        success = framework.clone_schema(
            source_db="PROD_DATALAKE",
            source_schema="AAPC",
            target_db="TEST_DATALAKE",
            target_schema="AAPC_TEST"
        )
        
        if success:
            print("Schema clone completed successfully!")
        else:
            print("Schema clone failed!")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_clone_table():
    """Example: Clone specific table."""
    print("\n=== Example: Clone Table ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Clone specific table
        success = framework.clone_table(
            source_db="PROD_DATALAKE",
            source_schema="ADAPTIVE",
            source_table="ACCOUNTS",
            target_db="SANDBOX_DATALAKE",
            target_schema="ADAPTIVE",
            target_table="ACCOUNTS_SNAPSHOT"
        )
        
        if success:
            print("Table clone completed successfully!")
        else:
            print("Table clone failed!")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_bulk_clone():
    """Example: Bulk clone operations."""
    print("\n=== Example: Bulk Clone Operations ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Define bulk operation
        operation_config = {
            'description': 'Example bulk clone operation',
            'schemas': [
                {
                    'source_db': 'PROD_DATALAKE',
                    'source_schema': 'AAPC',
                    'target_db': 'BULK_TEST_DATALAKE',
                    'target_schema': 'AAPC'
                },
                {
                    'source_db': 'PROD_DATALAKE',
                    'source_schema': 'ADAPTIVE',
                    'target_db': 'BULK_TEST_DATALAKE',
                    'target_schema': 'ADAPTIVE'
                }
            ],
            'tables': [
                {
                    'source_db': 'PROD_DATALAKE',
                    'source_schema': 'AAPC',
                    'source_table': 'AAPC_APC',
                    'target_db': 'BULK_TEST_DATALAKE',
                    'target_schema': 'AAPC',
                    'target_table': 'AAPC_APC_COPY'
                }
            ]
        }
        
        # Execute bulk operation
        results = framework.bulk_clone_operation(operation_config)
        
        # Display results
        summary = results['summary']
        print(f"Bulk operation completed:")
        print(f"  Total operations: {summary['total_operations']}")
        print(f"  Successful: {summary['successful_operations']}")
        print(f"  Failed: {summary['failed_operations']}")
        
        # Save results
        with open('bulk_clone_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print("Results saved to bulk_clone_results.json")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_rbac_setup():
    """Example: RBAC setup."""
    print("\n=== Example: RBAC Setup ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Initialize RBAC manager
        rbac_manager = RBACManager(framework.cursor, framework.config)
        
        # Setup RBAC for a database
        target_database = "DEV_DATALAKE"
        print(f"Setting up RBAC for {target_database}...")
        
        result = rbac_manager.complete_rbac_setup(target_database)
        
        if result['overall_success']:
            print("RBAC setup completed successfully!")
            
            # Display summary
            for phase, phase_result in result['phases'].items():
                if isinstance(phase_result, dict) and 'summary' in phase_result:
                    summary = phase_result['summary']
                    print(f"  {phase}: {summary.get('successful_roles', 0)} successful")
                elif isinstance(phase_result, dict):
                    successful = sum(1 for v in phase_result.values() if v)
                    print(f"  {phase}: {successful} successful")
        else:
            print("RBAC setup completed with some failures")
            if 'failed_phases' in result:
                print(f"Failed phases: {result['failed_phases']}")
        
        # Save RBAC results
        with open('rbac_setup_results.json', 'w') as f:
            json.dump(result, f, indent=2)
        
        print("RBAC results saved to rbac_setup_results.json")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_rbac_audit():
    """Example: RBAC audit."""
    print("\n=== Example: RBAC Audit ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Initialize RBAC manager
        rbac_manager = RBACManager(framework.cursor, framework.config)
        
        # Audit RBAC configuration
        target_database = "DEV_DATALAKE"
        print(f"Auditing RBAC for {target_database}...")
        
        audit_result = rbac_manager.audit_rbac_configuration(target_database)
        
        # Display audit summary
        summary = audit_result['summary']
        print(f"Audit results:")
        print(f"  Total roles: {summary['total_roles']}")
        print(f"  Roles with grants: {summary['roles_with_grants']}")
        print(f"  Users with roles: {summary['users_with_roles']}")
        
        # Save audit results
        with open('rbac_audit_results.json', 'w') as f:
            json.dump(audit_result, f, indent=2)
        
        print("Audit results saved to rbac_audit_results.json")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_clone_with_rbac():
    """Example: Clone database and setup RBAC in one operation."""
    print("\n=== Example: Clone with RBAC ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Step 1: Clone database
        source_db = "PROD_DATALAKE"
        target_db = "COMPLETE_DEMO_DB"
        
        print(f"Step 1: Cloning {source_db} to {target_db}...")
        clone_success = framework.clone_database(source_db, target_db)
        
        if clone_success:
            print("Database clone successful!")
            
            # Step 2: Setup RBAC
            print("Step 2: Setting up RBAC...")
            rbac_manager = RBACManager(framework.cursor, framework.config)
            rbac_result = rbac_manager.complete_rbac_setup(target_db)
            
            if rbac_result['overall_success']:
                print("RBAC setup successful!")
                
                # Step 3: Validate everything
                print("Step 3: Validating clone...")
                validation = framework.validate_clone_operation(source_db, target_db)
                
                print(f"Final validation status: {validation['overall_status']}")
                
                # Save complete results
                complete_result = {
                    'clone_result': clone_success,
                    'rbac_result': rbac_result,
                    'validation_result': validation,
                    'timestamp': datetime.now().isoformat()
                }
                
                with open('complete_demo_results.json', 'w') as f:
                    json.dump(complete_result, f, indent=2)
                
                print("Complete operation results saved to complete_demo_results.json")
                
            else:
                print("RBAC setup failed!")
        else:
            print("Database clone failed!")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def example_clone_history():
    """Example: View clone history."""
    print("\n=== Example: Clone History ===")
    
    framework = SnowflakeCloneFramework('config.yaml')
    
    try:
        framework.connect()
        
        # Get clone history
        print("Retrieving clone history...")
        history = framework.get_clone_history()
        
        if history:
            print(f"Found {len(history)} clone operations:")
            for clone in history[:5]:  # Show first 5
                print(f"  {clone['source_object']} -> {clone['clone_object']} ({clone['created_on']})")
        else:
            print("No clone history found")
        
        # Save history
        with open('clone_history.json', 'w') as f:
            json.dump(history, f, indent=2)
        
        print("Clone history saved to clone_history.json")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        framework.disconnect()


def main():
    """Run all examples."""
    print("Snowflake Zero-Copy Cloning Framework - Examples")
    print("=" * 50)
    
    # Note: These examples assume you have a valid config.yaml file
    # and appropriate permissions in Snowflake
    
    try:
        # Run examples (comment out any you don't want to run)
        example_discover_database()
        # example_clone_database()
        # example_clone_schema()
        # example_clone_table()
        # example_bulk_clone()
        # example_rbac_setup()
        # example_rbac_audit()
        # example_clone_with_rbac()
        example_clone_history()
        
    except Exception as e:
        print(f"Example execution failed: {e}")
    
    print("\nExamples completed!")


if __name__ == "__main__":
    main()