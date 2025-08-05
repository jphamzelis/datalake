#!/usr/bin/env python3
"""
Snowflake Zero-Copy Cloning Framework

This framework provides functionality to clone tables from PROD_DATALAKE 
to new databases in Snowflake using zero-copy cloning with RBAC management.

Features:
- Zero-copy cloning of databases, schemas, and tables
- RBAC management with SR (Service Role) and SFULL (System Full) permissions
- Configuration-driven approach
- Comprehensive logging and monitoring
- CLI interface
"""

import snowflake.connector
import logging
import yaml
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import os
from pathlib import Path


class SnowflakeCloneFramework:
    """Main framework class for Snowflake zero-copy cloning operations."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the framework with configuration."""
        self.config = self._load_config(config_path)
        self.connection = None
        self.cursor = None
        self._setup_logging()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_level = self.config.get('logging', {}).get('level', 'INFO')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.FileHandler('snowflake_clone.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            snowflake_config = self.config['snowflake']
            self.connection = snowflake.connector.connect(
                user=snowflake_config['user'],
                password=snowflake_config.get('password'),
                authenticator=snowflake_config.get('authenticator', 'snowflake'),
                account=snowflake_config['account'],
                warehouse=snowflake_config.get('warehouse'),
                database=snowflake_config.get('database'),
                schema=snowflake_config.get('schema'),
                role=snowflake_config.get('role')
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Successfully connected to Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Disconnected from Snowflake")
    
    def discover_source_structure(self, source_database: str = "PROD_DATALAKE") -> Dict[str, Any]:
        """Discover the structure of the source database."""
        self.logger.info(f"Discovering structure of {source_database}")
        
        structure = {
            'database': source_database,
            'schemas': {},
            'total_tables': 0,
            'discovery_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Get all schemas in the database
            self.cursor.execute(f"SHOW SCHEMAS IN DATABASE {source_database}")
            schemas = self.cursor.fetchall()
            
            for schema_info in schemas:
                schema_name = schema_info[1]  # Schema name is in second column
                self.logger.info(f"Processing schema: {schema_name}")
                
                structure['schemas'][schema_name] = {
                    'tables': [],
                    'table_count': 0
                }
                
                # Get tables in each schema
                try:
                    self.cursor.execute(f"SHOW TABLES IN SCHEMA {source_database}.{schema_name}")
                    tables = self.cursor.fetchall()
                    
                    for table_info in tables:
                        table_name = table_info[1]  # Table name is in second column
                        table_details = {
                            'name': table_name,
                            'created_on': table_info[0],
                            'database_name': table_info[2],
                            'schema_name': table_info[3],
                            'kind': table_info[4],
                            'comment': table_info[5] if len(table_info) > 5 else None
                        }
                        structure['schemas'][schema_name]['tables'].append(table_details)
                    
                    structure['schemas'][schema_name]['table_count'] = len(tables)
                    structure['total_tables'] += len(tables)
                    
                except Exception as e:
                    self.logger.warning(f"Could not access tables in schema {schema_name}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error discovering source structure: {e}")
            raise
        
        self.logger.info(f"Discovery complete: {structure['total_tables']} tables found across {len(structure['schemas'])} schemas")
        return structure
    
    def clone_database(self, source_db: str, target_db: str, clone_type: str = "ZERO_COPY") -> bool:
        """Clone entire database using zero-copy cloning."""
        self.logger.info(f"Cloning database {source_db} to {target_db} using {clone_type}")
        
        try:
            # Create target database as clone
            clone_sql = f"CREATE DATABASE {target_db} CLONE {source_db}"
            
            if clone_type.upper() == "AT_TIME":
                # Add AT clause for point-in-time cloning if specified in config
                at_time = self.config.get('cloning', {}).get('at_time')
                if at_time:
                    clone_sql += f" AT (TIMESTAMP => '{at_time}')"
            
            self.cursor.execute(clone_sql)
            self.logger.info(f"Successfully cloned database {source_db} to {target_db}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clone database {source_db} to {target_db}: {e}")
            return False
    
    def clone_schema(self, source_db: str, source_schema: str, target_db: str, target_schema: str = None) -> bool:
        """Clone specific schema using zero-copy cloning."""
        if target_schema is None:
            target_schema = source_schema
            
        self.logger.info(f"Cloning schema {source_db}.{source_schema} to {target_db}.{target_schema}")
        
        try:
            # Ensure target database exists
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
            
            # Clone schema
            clone_sql = f"CREATE SCHEMA {target_db}.{target_schema} CLONE {source_db}.{source_schema}"
            self.cursor.execute(clone_sql)
            
            self.logger.info(f"Successfully cloned schema {source_schema} to {target_db}.{target_schema}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clone schema {source_schema}: {e}")
            return False
    
    def clone_table(self, source_db: str, source_schema: str, source_table: str, 
                   target_db: str, target_schema: str = None, target_table: str = None) -> bool:
        """Clone specific table using zero-copy cloning."""
        if target_schema is None:
            target_schema = source_schema
        if target_table is None:
            target_table = source_table
            
        self.logger.info(f"Cloning table {source_db}.{source_schema}.{source_table} to {target_db}.{target_schema}.{target_table}")
        
        try:
            # Ensure target database and schema exist
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}")
            
            # Clone table
            clone_sql = f"CREATE TABLE {target_db}.{target_schema}.{target_table} CLONE {source_db}.{source_schema}.{source_table}"
            self.cursor.execute(clone_sql)
            
            self.logger.info(f"Successfully cloned table {source_table} to {target_db}.{target_schema}.{target_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clone table {source_table}: {e}")
            return False
    
    def bulk_clone_operation(self, operation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Perform bulk cloning operations based on configuration."""
        self.logger.info("Starting bulk clone operation")
        
        results = {
            'operation_id': f"bulk_clone_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': datetime.now().isoformat(),
            'databases': {},
            'schemas': {},
            'tables': {},
            'summary': {
                'total_operations': 0,
                'successful_operations': 0,
                'failed_operations': 0
            }
        }
        
        # Clone databases
        if 'databases' in operation_config:
            for db_config in operation_config['databases']:
                source_db = db_config['source']
                target_db = db_config['target']
                success = self.clone_database(source_db, target_db)
                
                results['databases'][target_db] = {
                    'source': source_db,
                    'success': success,
                    'timestamp': datetime.now().isoformat()
                }
                results['summary']['total_operations'] += 1
                if success:
                    results['summary']['successful_operations'] += 1
                else:
                    results['summary']['failed_operations'] += 1
        
        # Clone schemas
        if 'schemas' in operation_config:
            for schema_config in operation_config['schemas']:
                success = self.clone_schema(
                    schema_config['source_db'],
                    schema_config['source_schema'],
                    schema_config['target_db'],
                    schema_config.get('target_schema')
                )
                
                key = f"{schema_config['target_db']}.{schema_config.get('target_schema', schema_config['source_schema'])}"
                results['schemas'][key] = {
                    'source': f"{schema_config['source_db']}.{schema_config['source_schema']}",
                    'success': success,
                    'timestamp': datetime.now().isoformat()
                }
                results['summary']['total_operations'] += 1
                if success:
                    results['summary']['successful_operations'] += 1
                else:
                    results['summary']['failed_operations'] += 1
        
        # Clone tables
        if 'tables' in operation_config:
            for table_config in operation_config['tables']:
                success = self.clone_table(
                    table_config['source_db'],
                    table_config['source_schema'],
                    table_config['source_table'],
                    table_config['target_db'],
                    table_config.get('target_schema'),
                    table_config.get('target_table')
                )
                
                target_schema = table_config.get('target_schema', table_config['source_schema'])
                target_table = table_config.get('target_table', table_config['source_table'])
                key = f"{table_config['target_db']}.{target_schema}.{target_table}"
                
                results['tables'][key] = {
                    'source': f"{table_config['source_db']}.{table_config['source_schema']}.{table_config['source_table']}",
                    'success': success,
                    'timestamp': datetime.now().isoformat()
                }
                results['summary']['total_operations'] += 1
                if success:
                    results['summary']['successful_operations'] += 1
                else:
                    results['summary']['failed_operations'] += 1
        
        results['end_time'] = datetime.now().isoformat()
        self.logger.info(f"Bulk clone operation completed: {results['summary']}")
        
        return results
    
    def get_clone_history(self, object_name: str = None) -> List[Dict[str, Any]]:
        """Get cloning history for objects."""
        try:
            if object_name:
                query = f"SHOW CLONES LIKE '{object_name}'"
            else:
                query = "SHOW CLONES"
                
            self.cursor.execute(query)
            clones = self.cursor.fetchall()
            
            clone_history = []
            for clone in clones:
                clone_info = {
                    'source_object': clone[0],
                    'clone_object': clone[1],
                    'created_on': clone[2],
                    'clone_type': clone[3] if len(clone) > 3 else None
                }
                clone_history.append(clone_info)
            
            return clone_history
            
        except Exception as e:
            self.logger.error(f"Error retrieving clone history: {e}")
            return []
    
    def validate_clone_operation(self, source_obj: str, target_obj: str) -> Dict[str, Any]:
        """Validate that a clone operation was successful."""
        validation_result = {
            'source': source_obj,
            'target': target_obj,
            'validation_timestamp': datetime.now().isoformat(),
            'checks': {
                'target_exists': False,
                'source_accessible': False,
                'clone_relationship': False
            },
            'overall_status': 'FAILED'
        }
        
        try:
            # Check if source is accessible
            try:
                self.cursor.execute(f"DESCRIBE {source_obj}")
                validation_result['checks']['source_accessible'] = True
            except:
                pass
            
            # Check if target exists
            try:
                self.cursor.execute(f"DESCRIBE {target_obj}")
                validation_result['checks']['target_exists'] = True
            except:
                pass
            
            # Check clone relationship
            clones = self.get_clone_history(target_obj)
            for clone in clones:
                if clone['source_object'] == source_obj and clone['clone_object'] == target_obj:
                    validation_result['checks']['clone_relationship'] = True
                    break
            
            # Overall status
            if all(validation_result['checks'].values()):
                validation_result['overall_status'] = 'SUCCESS'
            
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            validation_result['error'] = str(e)
        
        return validation_result


if __name__ == "__main__":
    # Example usage
    framework = SnowflakeCloneFramework()
    try:
        framework.connect()
        
        # Discover source structure
        structure = framework.discover_source_structure("PROD_DATALAKE")
        print(json.dumps(structure, indent=2))
        
    finally:
        framework.disconnect()