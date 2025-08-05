#!/usr/bin/env python3
"""
RBAC Manager for Snowflake Zero-Copy Cloning Framework

This module handles Role-Based Access Control (RBAC) operations including:
- Creating and managing SR (Service Role) and SFULL (System Full) roles
- Applying privileges to databases, schemas, and tables
- Managing role hierarchies and user assignments
"""

import logging
from typing import Dict, List, Optional, Any
import yaml
from datetime import datetime


class RBACManager:
    """Manages RBAC operations for cloned Snowflake objects."""
    
    def __init__(self, cursor, config: Dict[str, Any]):
        """Initialize RBAC manager with Snowflake cursor and configuration."""
        self.cursor = cursor
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.rbac_config = config.get('rbac', {})
    
    def create_service_roles(self, target_database: str) -> Dict[str, bool]:
        """Create all configured service roles (SR)."""
        self.logger.info(f"Creating service roles for database: {target_database}")
        
        results = {}
        service_roles = self.rbac_config.get('service_roles', [])
        
        for role_config in service_roles:
            role_name = role_config['name']
            description = role_config.get('description', '')
            
            success = self._create_role(role_name, description)
            results[role_name] = success
            
            if success:
                self.logger.info(f"Created service role: {role_name}")
            else:
                self.logger.error(f"Failed to create service role: {role_name}")
        
        return results
    
    def create_system_full_roles(self, target_database: str) -> Dict[str, bool]:
        """Create all configured system full roles (SFULL)."""
        self.logger.info(f"Creating system full roles for database: {target_database}")
        
        results = {}
        system_full_roles = self.rbac_config.get('system_full_roles', [])
        
        for role_config in system_full_roles:
            role_name = role_config['name']
            description = role_config.get('description', '')
            
            success = self._create_role(role_name, description)
            results[role_name] = success
            
            if success:
                self.logger.info(f"Created system full role: {role_name}")
            else:
                self.logger.error(f"Failed to create system full role: {role_name}")
        
        return results
    
    def _create_role(self, role_name: str, description: str = "") -> bool:
        """Create a single role in Snowflake."""
        try:
            # Create role
            create_sql = f"CREATE ROLE IF NOT EXISTS {role_name}"
            if description:
                create_sql += f" COMMENT = '{description}'"
            
            self.cursor.execute(create_sql)
            self.logger.debug(f"Executed: {create_sql}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating role {role_name}: {e}")
            return False
    
    def apply_role_privileges(self, target_database: str, role_types: List[str] = None) -> Dict[str, Any]:
        """Apply privileges to roles based on configuration."""
        self.logger.info(f"Applying role privileges for database: {target_database}")
        
        if role_types is None:
            role_types = ['service_roles', 'system_full_roles']
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'target_database': target_database,
            'roles': {},
            'summary': {
                'total_roles': 0,
                'successful_roles': 0,
                'failed_roles': 0
            }
        }
        
        for role_type in role_types:
            roles_config = self.rbac_config.get(role_type, [])
            
            for role_config in roles_config:
                role_name = role_config['name']
                privileges = role_config.get('privileges', {})
                
                self.logger.info(f"Applying privileges for role: {role_name}")
                
                role_result = self._apply_role_privileges(role_name, privileges, target_database)
                results['roles'][role_name] = role_result
                results['summary']['total_roles'] += 1
                
                if role_result['success']:
                    results['summary']['successful_roles'] += 1
                else:
                    results['summary']['failed_roles'] += 1
        
        return results
    
    def _apply_role_privileges(self, role_name: str, privileges: Dict[str, List], target_database: str) -> Dict[str, Any]:
        """Apply privileges for a specific role."""
        result = {
            'role': role_name,
            'success': True,
            'applied_privileges': [],
            'failed_privileges': [],
            'error_messages': []
        }
        
        try:
            # Apply database privileges
            if 'databases' in privileges:
                for db_privilege in privileges['databases']:
                    success = self._grant_privilege(
                        privilege=db_privilege['privilege'],
                        object_type='DATABASE',
                        objects=self._substitute_variables(db_privilege['objects'], target_database),
                        role=role_name
                    )
                    
                    privilege_info = {
                        'type': 'database',
                        'privilege': db_privilege['privilege'],
                        'objects': db_privilege['objects']
                    }
                    
                    if success:
                        result['applied_privileges'].append(privilege_info)
                    else:
                        result['failed_privileges'].append(privilege_info)
                        result['success'] = False
            
            # Apply schema privileges
            if 'schemas' in privileges:
                for schema_privilege in privileges['schemas']:
                    success = self._grant_privilege(
                        privilege=schema_privilege['privilege'],
                        object_type='SCHEMA',
                        objects=self._substitute_variables(schema_privilege['objects'], target_database),
                        role=role_name
                    )
                    
                    privilege_info = {
                        'type': 'schema',
                        'privilege': schema_privilege['privilege'],
                        'objects': schema_privilege['objects']
                    }
                    
                    if success:
                        result['applied_privileges'].append(privilege_info)
                    else:
                        result['failed_privileges'].append(privilege_info)
                        result['success'] = False
            
            # Apply table privileges
            if 'tables' in privileges:
                for table_privilege in privileges['tables']:
                    success = self._grant_privilege(
                        privilege=table_privilege['privilege'],
                        object_type='TABLE',
                        objects=self._substitute_variables(table_privilege['objects'], target_database),
                        role=role_name
                    )
                    
                    privilege_info = {
                        'type': 'table',
                        'privilege': table_privilege['privilege'],
                        'objects': table_privilege['objects']
                    }
                    
                    if success:
                        result['applied_privileges'].append(privilege_info)
                    else:
                        result['failed_privileges'].append(privilege_info)
                        result['success'] = False
            
            # Apply view privileges
            if 'views' in privileges:
                for view_privilege in privileges['views']:
                    success = self._grant_privilege(
                        privilege=view_privilege['privilege'],
                        object_type='VIEW',
                        objects=self._substitute_variables(view_privilege['objects'], target_database),
                        role=role_name
                    )
                    
                    privilege_info = {
                        'type': 'view',
                        'privilege': view_privilege['privilege'],
                        'objects': view_privilege['objects']
                    }
                    
                    if success:
                        result['applied_privileges'].append(privilege_info)
                    else:
                        result['failed_privileges'].append(privilege_info)
                        result['success'] = False
            
            # Apply warehouse privileges
            if 'warehouses' in privileges:
                for warehouse_privilege in privileges['warehouses']:
                    success = self._grant_privilege(
                        privilege=warehouse_privilege['privilege'],
                        object_type='WAREHOUSE',
                        objects=warehouse_privilege['objects'],
                        role=role_name
                    )
                    
                    privilege_info = {
                        'type': 'warehouse',
                        'privilege': warehouse_privilege['privilege'],
                        'objects': warehouse_privilege['objects']
                    }
                    
                    if success:
                        result['applied_privileges'].append(privilege_info)
                    else:
                        result['failed_privileges'].append(privilege_info)
                        result['success'] = False
                        
        except Exception as e:
            self.logger.error(f"Error applying privileges for role {role_name}: {e}")
            result['success'] = False
            result['error_messages'].append(str(e))
        
        return result
    
    def _grant_privilege(self, privilege: str, object_type: str, objects: List[str], role: str) -> bool:
        """Grant a specific privilege on objects to a role."""
        try:
            for obj in objects:
                if privilege.upper() == 'ALL':
                    grant_sql = f"GRANT ALL PRIVILEGES ON {object_type} {obj} TO ROLE {role}"
                else:
                    grant_sql = f"GRANT {privilege} ON {object_type} {obj} TO ROLE {role}"
                
                self.cursor.execute(grant_sql)
                self.logger.debug(f"Executed: {grant_sql}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error granting {privilege} on {object_type} {objects} to {role}: {e}")
            return False
    
    def _substitute_variables(self, objects: List[str], target_database: str) -> List[str]:
        """Substitute variables in object names."""
        substituted = []
        for obj in objects:
            substituted_obj = obj.replace('${TARGET_DATABASE}', target_database)
            substituted.append(substituted_obj)
        return substituted
    
    def setup_role_hierarchy(self) -> Dict[str, bool]:
        """Setup role hierarchy based on configuration."""
        self.logger.info("Setting up role hierarchy")
        
        results = {}
        role_hierarchy = self.rbac_config.get('role_hierarchy', [])
        
        for hierarchy in role_hierarchy:
            parent_role = hierarchy['parent']
            child_roles = hierarchy['children']
            
            for child_role in child_roles:
                try:
                    grant_sql = f"GRANT ROLE {child_role} TO ROLE {parent_role}"
                    self.cursor.execute(grant_sql)
                    results[f"{parent_role} -> {child_role}"] = True
                    self.logger.info(f"Granted role {child_role} to {parent_role}")
                    
                except Exception as e:
                    self.logger.error(f"Error granting role {child_role} to {parent_role}: {e}")
                    results[f"{parent_role} -> {child_role}"] = False
        
        return results
    
    def assign_users_to_roles(self, user_assignments: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """Assign users to roles based on configuration."""
        self.logger.info("Assigning users to roles")
        
        if user_assignments is None:
            user_assignments = self.rbac_config.get('user_assignments', {}).get('example_users', [])
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'users': {},
            'summary': {
                'total_assignments': 0,
                'successful_assignments': 0,
                'failed_assignments': 0
            }
        }
        
        for user_config in user_assignments:
            username = user_config['username']
            roles = user_config['roles']
            
            user_result = {
                'username': username,
                'assigned_roles': [],
                'failed_roles': [],
                'success': True
            }
            
            for role in roles:
                try:
                    grant_sql = f"GRANT ROLE {role} TO USER {username}"
                    self.cursor.execute(grant_sql)
                    user_result['assigned_roles'].append(role)
                    results['summary']['successful_assignments'] += 1
                    self.logger.info(f"Granted role {role} to user {username}")
                    
                except Exception as e:
                    self.logger.error(f"Error granting role {role} to user {username}: {e}")
                    user_result['failed_roles'].append({'role': role, 'error': str(e)})
                    user_result['success'] = False
                    results['summary']['failed_assignments'] += 1
                
                results['summary']['total_assignments'] += 1
            
            results['users'][username] = user_result
        
        return results
    
    def complete_rbac_setup(self, target_database: str, role_types: List[str] = None) -> Dict[str, Any]:
        """Complete RBAC setup including roles, privileges, hierarchy, and user assignments."""
        self.logger.info(f"Starting complete RBAC setup for database: {target_database}")
        
        setup_result = {
            'timestamp': datetime.now().isoformat(),
            'target_database': target_database,
            'phases': {},
            'overall_success': True
        }
        
        try:
            # Phase 1: Create service roles
            self.logger.info("Phase 1: Creating service roles")
            service_roles_result = self.create_service_roles(target_database)
            setup_result['phases']['service_roles'] = service_roles_result
            
            # Phase 2: Create system full roles
            self.logger.info("Phase 2: Creating system full roles")
            system_roles_result = self.create_system_full_roles(target_database)
            setup_result['phases']['system_full_roles'] = system_roles_result
            
            # Phase 3: Apply privileges
            self.logger.info("Phase 3: Applying role privileges")
            privileges_result = self.apply_role_privileges(target_database, role_types)
            setup_result['phases']['privileges'] = privileges_result
            
            # Phase 4: Setup role hierarchy
            self.logger.info("Phase 4: Setting up role hierarchy")
            hierarchy_result = self.setup_role_hierarchy()
            setup_result['phases']['hierarchy'] = hierarchy_result
            
            # Phase 5: Assign users (optional)
            if self.rbac_config.get('user_assignments'):
                self.logger.info("Phase 5: Assigning users to roles")
                user_assignments_result = self.assign_users_to_roles()
                setup_result['phases']['user_assignments'] = user_assignments_result
            
            # Check overall success
            failed_phases = []
            for phase, result in setup_result['phases'].items():
                if isinstance(result, dict):
                    if 'summary' in result and result['summary'].get('failed_roles', 0) > 0:
                        failed_phases.append(phase)
                    elif phase == 'hierarchy' and not all(result.values()):
                        failed_phases.append(phase)
                    elif not all(result.values()) if isinstance(result, dict) else False:
                        failed_phases.append(phase)
            
            if failed_phases:
                setup_result['overall_success'] = False
                setup_result['failed_phases'] = failed_phases
                self.logger.warning(f"RBAC setup completed with failures in phases: {failed_phases}")
            else:
                self.logger.info("RBAC setup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during RBAC setup: {e}")
            setup_result['overall_success'] = False
            setup_result['error'] = str(e)
        
        return setup_result
    
    def audit_rbac_configuration(self, target_database: str) -> Dict[str, Any]:
        """Audit current RBAC configuration for the target database."""
        self.logger.info(f"Auditing RBAC configuration for database: {target_database}")
        
        audit_result = {
            'timestamp': datetime.now().isoformat(),
            'target_database': target_database,
            'roles': {},
            'grants': {},
            'users': {},
            'summary': {
                'total_roles': 0,
                'roles_with_grants': 0,
                'users_with_roles': 0
            }
        }
        
        try:
            # Audit roles
            self.cursor.execute("SHOW ROLES")
            roles = self.cursor.fetchall()
            
            for role in roles:
                role_name = role[1]  # Role name is in second column
                audit_result['roles'][role_name] = {
                    'created_on': role[0],
                    'owner': role[2] if len(role) > 2 else None,
                    'comment': role[3] if len(role) > 3 else None
                }
                audit_result['summary']['total_roles'] += 1
            
            # Audit grants for each role
            for role_name in audit_result['roles'].keys():
                try:
                    self.cursor.execute(f"SHOW GRANTS TO ROLE {role_name}")
                    grants = self.cursor.fetchall()
                    
                    role_grants = []
                    for grant in grants:
                        grant_info = {
                            'privilege': grant[1],
                            'granted_on': grant[2],
                            'name': grant[3],
                            'granted_to': grant[4] if len(grant) > 4 else None,
                            'grantee_name': grant[5] if len(grant) > 5 else None
                        }
                        role_grants.append(grant_info)
                    
                    if role_grants:
                        audit_result['grants'][role_name] = role_grants
                        audit_result['summary']['roles_with_grants'] += 1
                        
                except Exception as e:
                    self.logger.warning(f"Could not audit grants for role {role_name}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error during RBAC audit: {e}")
            audit_result['error'] = str(e)
        
        return audit_result


def main():
    """Example usage of RBAC Manager."""
    # This would typically be called from the main framework
    pass


if __name__ == "__main__":
    main()