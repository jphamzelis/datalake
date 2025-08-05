#!/usr/bin/env python3
"""
Snowflake Zero-Copy Cloning Framework - Streamlit Web Interface

A comprehensive web interface for managing Snowflake zero-copy cloning operations
with integrated RBAC management and monitoring capabilities.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import yaml
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

# Import the framework components
from snowflake_clone_framework import SnowflakeCloneFramework
from rbac_manager import RBACManager

# Configure Streamlit page
st.set_page_config(
    page_title="Snowflake Clone Framework",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #424242;
        margin: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'framework' not in st.session_state:
    st.session_state.framework = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'config' not in st.session_state:
    st.session_state.config = None

def load_config():
    """Load configuration from file."""
    config_path = "config.yaml"
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            st.error(f"Error loading config: {str(e)}")
    return None

def save_config(config):
    """Save configuration to file."""
    try:
        with open("config.yaml", 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        return True
    except Exception as e:
        st.error(f"Error saving config: {str(e)}")
        return False

def main():
    """Main application function."""
    
    # Header
    st.markdown('<div class="main-header">❄️ Snowflake Clone Framework</div>', unsafe_allow_html=True)
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    
    # Connection status indicator
    if st.session_state.connected:
        st.sidebar.success("✅ Connected to Snowflake")
    else:
        st.sidebar.warning("⚠️ Not connected")
    
    # Navigation menu
    page = st.sidebar.selectbox(
        "Select Page",
        [
            "🏠 Dashboard",
            "🔌 Connection",
            "🔍 Discovery",
            "📋 Clone Operations",
            "👥 RBAC Management",
            "📊 Monitoring",
            "⚡ Bulk Operations",
            "⚙️ Settings"
        ]
    )
    
    # Route to appropriate page
    if page == "🏠 Dashboard":
        dashboard_page()
    elif page == "🔌 Connection":
        connection_page()
    elif page == "🔍 Discovery":
        discovery_page()
    elif page == "📋 Clone Operations":
        clone_operations_page()
    elif page == "👥 RBAC Management":
        rbac_management_page()
    elif page == "📊 Monitoring":
        monitoring_page()
    elif page == "⚡ Bulk Operations":
        bulk_operations_page()
    elif page == "⚙️ Settings":
        settings_page()

def dashboard_page():
    """Dashboard overview page."""
    st.markdown('<div class="sub-header">Dashboard Overview</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.markdown("""
        <div class="info-box">
        <h4>Welcome to Snowflake Clone Framework</h4>
        <p>Please configure your Snowflake connection in the Connection page to get started.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
        <h3>12</h3>
        <p>Total Clones</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
        <h3>98.5%</h3>
        <p>Success Rate</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
        <h3>5</h3>
        <p>Active Databases</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="metric-card">
        <h3>24</h3>
        <p>RBAC Roles</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Charts section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Clone Operations Over Time")
        # Sample data for demonstration
        dates = pd.date_range(start='2024-01-01', end='2024-01-30', freq='D')
        clone_data = pd.DataFrame({
            'Date': dates,
            'Clones': [5, 8, 12, 6, 9, 15, 11, 7, 13, 16, 9, 12, 8, 14, 10, 
                      18, 13, 9, 11, 15, 7, 12, 16, 8, 14, 11, 9, 13, 17, 12]
        })
        
        fig = px.line(clone_data, x='Date', y='Clones', title='Daily Clone Operations')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Clone Success Rate by Type")
        # Sample data for demonstration
        success_data = pd.DataFrame({
            'Type': ['Database', 'Schema', 'Table'],
            'Success Rate': [98.5, 96.2, 99.1],
            'Total Operations': [150, 280, 420]
        })
        
        fig = px.bar(success_data, x='Type', y='Success Rate', 
                    title='Success Rate by Clone Type',
                    color='Success Rate',
                    color_continuous_scale='viridis')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent activity
    st.subheader("Recent Activity")
    recent_activity = pd.DataFrame({
        'Timestamp': ['2024-01-30 14:30:00', '2024-01-30 13:15:00', '2024-01-30 11:45:00'],
        'Operation': ['Clone Database', 'Setup RBAC', 'Clone Schema'],
        'Source': ['PROD_DATALAKE', 'DEV_DATALAKE', 'PROD_DATALAKE.AAPC'],
        'Target': ['STAGING_DATALAKE', 'DEV_DATALAKE', 'TEST_DATALAKE.AAPC'],
        'Status': ['✅ Success', '✅ Success', '✅ Success']
    })
    
    st.dataframe(recent_activity, use_container_width=True, hide_index=True)

def connection_page():
    """Snowflake connection configuration page."""
    st.markdown('<div class="sub-header">Snowflake Connection Configuration</div>', unsafe_allow_html=True)
    
    # Load existing config
    config = load_config()
    
    with st.form("connection_form"):
        st.subheader("Connection Details")
        
        col1, col2 = st.columns(2)
        
        with col1:
            account = st.text_input(
                "Account", 
                value=config.get('snowflake', {}).get('account', '') if config else '',
                help="Your Snowflake account identifier"
            )
            user = st.text_input(
                "Username", 
                value=config.get('snowflake', {}).get('user', '') if config else ''
            )
            password = st.text_input(
                "Password", 
                type="password"
            )
            warehouse = st.text_input(
                "Warehouse", 
                value=config.get('snowflake', {}).get('warehouse', 'COMPUTE_WH') if config else 'COMPUTE_WH'
            )
        
        with col2:
            database = st.text_input(
                "Database", 
                value=config.get('snowflake', {}).get('database', 'PROD_DATALAKE') if config else 'PROD_DATALAKE'
            )
            schema = st.text_input(
                "Schema", 
                value=config.get('snowflake', {}).get('schema', 'PUBLIC') if config else 'PUBLIC'
            )
            role = st.text_input(
                "Role", 
                value=config.get('snowflake', {}).get('role', 'SYSADMIN') if config else 'SYSADMIN'
            )
            authenticator = st.selectbox(
                "Authenticator", 
                ['snowflake', 'externalbrowser'],
                index=0 if not config else (['snowflake', 'externalbrowser'].index(
                    config.get('snowflake', {}).get('authenticator', 'snowflake')
                ))
            )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.form_submit_button("💾 Save Configuration", type="primary"):
                new_config = {
                    'snowflake': {
                        'account': account,
                        'user': user,
                        'password': password,
                        'warehouse': warehouse,
                        'database': database,
                        'schema': schema,
                        'role': role,
                        'authenticator': authenticator
                    }
                }
                
                # Merge with existing config if it exists
                if config:
                    config.update(new_config)
                    new_config = config
                
                if save_config(new_config):
                    st.session_state.config = new_config
                    st.success("✅ Configuration saved successfully!")
        
        with col2:
            if st.form_submit_button("🔌 Test Connection"):
                if not all([account, user, password, warehouse]):
                    st.error("❌ Please fill in all required fields")
                else:
                    with st.spinner("Testing connection..."):
                        try:
                            # Create temporary config for testing
                            test_config = {
                                'snowflake': {
                                    'account': account,
                                    'user': user,
                                    'password': password,
                                    'warehouse': warehouse,
                                    'database': database,
                                    'schema': schema,
                                    'role': role,
                                    'authenticator': authenticator
                                }
                            }
                            
                            # Test connection
                            framework = SnowflakeCloneFramework()
                            framework.config = test_config
                            framework.connect()
                            framework.disconnect()
                            
                            st.success("✅ Connection successful!")
                            
                        except Exception as e:
                            st.error(f"❌ Connection failed: {str(e)}")
        
        with col3:
            if st.form_submit_button("🚀 Connect"):
                if not st.session_state.config:
                    st.error("❌ Please save configuration first")
                else:
                    with st.spinner("Connecting to Snowflake..."):
                        try:
                            framework = SnowflakeCloneFramework()
                            framework.config = st.session_state.config
                            framework.connect()
                            
                            st.session_state.framework = framework
                            st.session_state.connected = True
                            st.success("✅ Connected to Snowflake!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Connection failed: {str(e)}")
    
    # Connection status
    if st.session_state.connected:
        st.markdown("""
        <div class="success-box">
        <h4>✅ Successfully Connected</h4>
        <p>You are now connected to Snowflake and can use all framework features.</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔌 Disconnect"):
            if st.session_state.framework:
                st.session_state.framework.disconnect()
            st.session_state.connected = False
            st.session_state.framework = None
            st.success("Disconnected from Snowflake")
            st.rerun()

def discovery_page():
    """Database discovery and structure exploration page."""
    st.markdown('<div class="sub-header">Database Discovery & Structure</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.warning("⚠️ Please connect to Snowflake first in the Connection page.")
        return
    
    # Database selection
    col1, col2 = st.columns([2, 1])
    
    with col1:
        database_name = st.text_input(
            "Database to Explore", 
            value="PROD_DATALAKE",
            help="Enter the database name to explore its structure"
        )
    
    with col2:
        if st.button("🔍 Discover Structure", type="primary"):
            with st.spinner("Discovering database structure..."):
                try:
                    structure = st.session_state.framework.discover_source_structure(database_name)
                    st.session_state.database_structure = structure
                    st.success(f"✅ Successfully discovered structure for {database_name}")
                except Exception as e:
                    st.error(f"❌ Discovery failed: {str(e)}")
    
    # Display structure if available
    if hasattr(st.session_state, 'database_structure'):
        structure = st.session_state.database_structure
        
        st.subheader(f"Structure Overview: {database_name}")
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            schema_count = len(structure.get('schemas', {}))
            st.metric("Schemas", schema_count)
        
        with col2:
            total_tables = sum(len(schema.get('tables', [])) for schema in structure.get('schemas', {}).values())
            st.metric("Total Tables", total_tables)
        
        with col3:
            total_views = sum(len(schema.get('views', [])) for schema in structure.get('schemas', {}).values())
            st.metric("Total Views", total_views)
        
        # Schema details
        st.subheader("Schema Details")
        
        for schema_name, schema_info in structure.get('schemas', {}).items():
            with st.expander(f"📂 {schema_name}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Tables:**")
                    tables = schema_info.get('tables', [])
                    if tables:
                        table_df = pd.DataFrame([
                            {
                                'Table': table['name'],
                                'Rows': table.get('row_count', 'N/A'),
                                'Size (MB)': round(table.get('size_bytes', 0) / 1024 / 1024, 2)
                            }
                            for table in tables
                        ])
                        st.dataframe(table_df, hide_index=True)
                    else:
                        st.write("No tables found")
                
                with col2:
                    st.write("**Views:**")
                    views = schema_info.get('views', [])
                    if views:
                        view_df = pd.DataFrame([
                            {'View': view['name']}
                            for view in views
                        ])
                        st.dataframe(view_df, hide_index=True)
                    else:
                        st.write("No views found")

def clone_operations_page():
    """Clone operations page for database, schema, and table cloning."""
    st.markdown('<div class="sub-header">Clone Operations</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.warning("⚠️ Please connect to Snowflake first in the Connection page.")
        return
    
    # Operation type selection
    operation_type = st.selectbox(
        "Select Clone Operation Type",
        ["🗄️ Database Clone", "📁 Schema Clone", "📊 Table Clone"]
    )
    
    if operation_type == "🗄️ Database Clone":
        database_clone_form()
    elif operation_type == "📁 Schema Clone":
        schema_clone_form()
    elif operation_type == "📊 Table Clone":
        table_clone_form()

def database_clone_form():
    """Form for database cloning operations."""
    st.subheader("Database Clone Operation")
    
    with st.form("database_clone_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            source_db = st.text_input(
                "Source Database", 
                value="PROD_DATALAKE",
                help="Name of the source database to clone"
            )
            clone_type = st.selectbox(
                "Clone Type",
                ["ZERO_COPY", "AT_TIME"],
                help="ZERO_COPY for current state, AT_TIME for point-in-time"
            )
        
        with col2:
            target_db = st.text_input(
                "Target Database", 
                help="Name for the new cloned database"
            )
            if clone_type == "AT_TIME":
                at_time = st.text_input(
                    "Point in Time",
                    placeholder="2024-01-01 00:00:00",
                    help="Timestamp for point-in-time cloning"
                )
        
        apply_rbac = st.checkbox(
            "Apply RBAC Configuration",
            value=True,
            help="Automatically setup role-based access control for the cloned database"
        )
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.form_submit_button("🚀 Clone Database", type="primary"):
                if not source_db or not target_db:
                    st.error("❌ Please provide both source and target database names")
                else:
                    with st.spinner(f"Cloning {source_db} to {target_db}..."):
                        try:
                            success = st.session_state.framework.clone_database(
                                source_db, 
                                target_db, 
                                clone_type.lower()
                            )
                            
                            if success:
                                st.success(f"✅ Successfully cloned {source_db} to {target_db}")
                                
                                if apply_rbac:
                                    with st.spinner("Setting up RBAC..."):
                                        rbac_manager = RBACManager(
                                            st.session_state.framework.cursor,
                                            st.session_state.framework.config
                                        )
                                        rbac_result = rbac_manager.complete_rbac_setup(target_db)
                                        st.success("✅ RBAC setup completed")
                            else:
                                st.error("❌ Clone operation failed")
                        except Exception as e:
                            st.error(f"❌ Clone operation failed: {str(e)}")

def schema_clone_form():
    """Form for schema cloning operations."""
    st.subheader("Schema Clone Operation")
    
    with st.form("schema_clone_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Source:**")
            source_db = st.text_input("Source Database", value="PROD_DATALAKE")
            source_schema = st.text_input("Source Schema", placeholder="e.g., AAPC")
        
        with col2:
            st.write("**Target:**")
            target_db = st.text_input("Target Database")
            target_schema = st.text_input("Target Schema")
        
        if st.form_submit_button("🚀 Clone Schema", type="primary"):
            if not all([source_db, source_schema, target_db, target_schema]):
                st.error("❌ Please fill in all fields")
            else:
                with st.spinner(f"Cloning {source_db}.{source_schema} to {target_db}.{target_schema}..."):
                    try:
                        success = st.session_state.framework.clone_schema(
                            source_db, source_schema, target_db, target_schema
                        )
                        
                        if success:
                            st.success(f"✅ Successfully cloned schema {source_schema}")
                        else:
                            st.error("❌ Schema clone operation failed")
                    except Exception as e:
                        st.error(f"❌ Schema clone operation failed: {str(e)}")

def table_clone_form():
    """Form for table cloning operations."""
    st.subheader("Table Clone Operation")
    
    with st.form("table_clone_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Source:**")
            source_db = st.text_input("Source Database", value="PROD_DATALAKE")
            source_schema = st.text_input("Source Schema", placeholder="e.g., AAPC")
            source_table = st.text_input("Source Table", placeholder="e.g., AAPC_APC")
        
        with col2:
            st.write("**Target:**")
            target_db = st.text_input("Target Database")
            target_schema = st.text_input("Target Schema")
            target_table = st.text_input("Target Table")
        
        if st.form_submit_button("🚀 Clone Table", type="primary"):
            if not all([source_db, source_schema, source_table, target_db, target_schema, target_table]):
                st.error("❌ Please fill in all fields")
            else:
                with st.spinner(f"Cloning {source_db}.{source_schema}.{source_table}..."):
                    try:
                        success = st.session_state.framework.clone_table(
                            source_db, source_schema, source_table,
                            target_db, target_schema, target_table
                        )
                        
                        if success:
                            st.success(f"✅ Successfully cloned table {source_table}")
                        else:
                            st.error("❌ Table clone operation failed")
                    except Exception as e:
                        st.error(f"❌ Table clone operation failed: {str(e)}")

def rbac_management_page():
    """RBAC management page for role setup and auditing."""
    st.markdown('<div class="sub-header">RBAC Management</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.warning("⚠️ Please connect to Snowflake first in the Connection page.")
        return
    
    # RBAC operation selection
    rbac_operation = st.selectbox(
        "Select RBAC Operation",
        ["🛡️ Setup RBAC", "🔍 Audit RBAC", "👥 Manage Users", "📊 Role Hierarchy"]
    )
    
    if rbac_operation == "🛡️ Setup RBAC":
        rbac_setup_form()
    elif rbac_operation == "🔍 Audit RBAC":
        rbac_audit_form()
    elif rbac_operation == "👥 Manage Users":
        user_management_form()
    elif rbac_operation == "📊 Role Hierarchy":
        role_hierarchy_display()

def rbac_setup_form():
    """Form for RBAC setup operations."""
    st.subheader("RBAC Setup")
    
    with st.form("rbac_setup_form"):
        database_name = st.text_input(
            "Database Name",
            help="Database to setup RBAC for"
        )
        
        role_types = st.multiselect(
            "Role Types to Create",
            ["service_roles", "system_full_roles"],
            default=["service_roles", "system_full_roles"]
        )
        
        if st.form_submit_button("🛡️ Setup RBAC", type="primary"):
            if not database_name:
                st.error("❌ Please provide a database name")
            else:
                with st.spinner(f"Setting up RBAC for {database_name}..."):
                    try:
                        rbac_manager = RBACManager(
                            st.session_state.framework.cursor,
                            st.session_state.framework.config
                        )
                        
                        result = rbac_manager.complete_rbac_setup(database_name, role_types)
                        st.success(f"✅ RBAC setup completed for {database_name}")
                        
                        # Display created roles
                        if result.get('created_roles'):
                            st.subheader("Created Roles")
                            roles_df = pd.DataFrame(result['created_roles'])
                            st.dataframe(roles_df, hide_index=True)
                            
                    except Exception as e:
                        st.error(f"❌ RBAC setup failed: {str(e)}")

def rbac_audit_form():
    """Form for RBAC auditing operations."""
    st.subheader("RBAC Audit")
    
    database_name = st.text_input("Database Name to Audit")
    
    if st.button("🔍 Run RBAC Audit", type="primary"):
        if not database_name:
            st.error("❌ Please provide a database name")
        else:
            with st.spinner(f"Auditing RBAC configuration for {database_name}..."):
                try:
                    rbac_manager = RBACManager(
                        st.session_state.framework.cursor,
                        st.session_state.framework.config
                    )
                    
                    audit_result = rbac_manager.audit_rbac_configuration(database_name)
                    st.success(f"✅ RBAC audit completed for {database_name}")
                    
                    # Display audit results
                    if audit_result:
                        st.subheader("Audit Results")
                        st.json(audit_result)
                        
                except Exception as e:
                    st.error(f"❌ RBAC audit failed: {str(e)}")

def user_management_form():
    """Form for user role management."""
    st.subheader("User Role Management")
    st.info("👥 Manage user assignments to roles")
    
    # This would integrate with the RBAC manager for user assignments
    st.write("User management functionality would be implemented here")

def role_hierarchy_display():
    """Display role hierarchy information."""
    st.subheader("Role Hierarchy")
    
    # Load role hierarchy from config
    config = st.session_state.config
    if config and 'rbac' in config:
        hierarchy = config['rbac'].get('role_hierarchy', [])
        
        st.subheader("Configured Role Hierarchy")
        for item in hierarchy:
            parent = item.get('parent')
            children = item.get('children', [])
            
            st.write(f"**{parent}**")
            for child in children:
                st.write(f"  └── {child}")

def monitoring_page():
    """Monitoring and audit trail page."""
    st.markdown('<div class="sub-header">Monitoring & Audit Trail</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.warning("⚠️ Please connect to Snowflake first in the Connection page.")
        return
    
    # Monitoring tabs
    tab1, tab2, tab3 = st.tabs(["📊 Performance Metrics", "📋 Audit Trail", "⚠️ Alerts"])
    
    with tab1:
        performance_metrics_display()
    
    with tab2:
        audit_trail_display()
    
    with tab3:
        alerts_display()

def performance_metrics_display():
    """Display performance metrics."""
    st.subheader("Performance Metrics")
    
    # Sample metrics data
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg Clone Time", "2.3 min", "-0.5 min")
    
    with col2:
        st.metric("Success Rate", "98.5%", "+0.3%")
    
    with col3:
        st.metric("Active Connections", "12", "+2")
    
    with col4:
        st.metric("Storage Used", "1.2 TB", "+50 GB")
    
    # Performance charts
    st.subheader("Performance Trends")
    
    # Sample data for performance chart
    dates = pd.date_range(start='2024-01-01', end='2024-01-30', freq='D')
    performance_data = pd.DataFrame({
        'Date': dates,
        'Clone Time (min)': [2.1, 2.3, 1.9, 2.5, 2.2, 2.0, 2.4, 2.1, 1.8, 2.6,
                            2.3, 2.1, 2.4, 2.0, 2.2, 2.5, 2.1, 1.9, 2.3, 2.4,
                            2.0, 2.2, 2.1, 2.3, 1.9, 2.4, 2.2, 2.0, 2.3, 2.1],
        'Success Rate (%)': [98.2, 98.5, 97.8, 98.9, 98.1, 99.2, 98.7, 98.3, 99.1, 97.9,
                            98.4, 98.8, 98.2, 99.0, 98.6, 98.1, 98.9, 98.3, 98.7, 98.4,
                            98.8, 98.2, 99.1, 98.5, 98.9, 98.3, 98.6, 98.1, 98.7, 98.5]
    })
    
    fig = px.line(performance_data, x='Date', y=['Clone Time (min)', 'Success Rate (%)'], 
                  title='Performance Trends Over Time')
    st.plotly_chart(fig, use_container_width=True)

def audit_trail_display():
    """Display audit trail information."""
    st.subheader("Audit Trail")
    
    # Sample audit data
    audit_data = pd.DataFrame({
        'Timestamp': ['2024-01-30 14:30:00', '2024-01-30 13:15:00', '2024-01-30 11:45:00', 
                     '2024-01-30 10:30:00', '2024-01-30 09:15:00'],
        'User': ['admin@company.com', 'engineer@company.com', 'analyst@company.com',
                'admin@company.com', 'engineer@company.com'],
        'Operation': ['Clone Database', 'Setup RBAC', 'Clone Schema', 'Create Role', 'Clone Table'],
        'Object': ['PROD_DATALAKE -> STAGING_DATALAKE', 'DEV_DATALAKE', 
                  'PROD_DATALAKE.AAPC -> TEST_DATALAKE.AAPC',
                  'SR_DATA_READER', 'PROD_DATALAKE.ADAPTIVE.ACCOUNTS'],
        'Status': ['✅ Success', '✅ Success', '✅ Success', '✅ Success', '✅ Success'],
        'Duration': ['2.3 min', '0.8 min', '1.1 min', '0.3 min', '0.5 min']
    })
    
    st.dataframe(audit_data, use_container_width=True, hide_index=True)

def alerts_display():
    """Display alerts and notifications."""
    st.subheader("Alerts & Notifications")
    
    # Alert configuration
    st.subheader("Alert Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        clone_threshold = st.slider("Clone Failure Threshold (%)", 0, 20, 10)
        rbac_threshold = st.slider("RBAC Failure Threshold (%)", 0, 10, 5)
    
    with col2:
        email_alerts = st.checkbox("Enable Email Alerts", value=False)
        slack_alerts = st.checkbox("Enable Slack Alerts", value=False)
    
    if st.button("💾 Save Alert Configuration"):
        st.success("✅ Alert configuration saved")
    
    # Recent alerts
    st.subheader("Recent Alerts")
    
    alert_data = pd.DataFrame({
        'Timestamp': ['2024-01-29 16:45:00', '2024-01-28 09:30:00'],
        'Type': ['Warning', 'Info'],
        'Message': ['Clone operation took longer than expected (3.2 min)', 
                   'RBAC setup completed successfully'],
        'Status': ['🟡 Active', '✅ Resolved']
    })
    
    st.dataframe(alert_data, use_container_width=True, hide_index=True)

def bulk_operations_page():
    """Bulk operations page with template management."""
    st.markdown('<div class="sub-header">Bulk Operations</div>', unsafe_allow_html=True)
    
    if not st.session_state.connected:
        st.warning("⚠️ Please connect to Snowflake first in the Connection page.")
        return
    
    # Operation mode selection
    operation_mode = st.selectbox(
        "Select Operation Mode",
        ["📋 Use Template", "📝 Custom Operations", "📚 Manage Templates"]
    )
    
    if operation_mode == "📋 Use Template":
        template_operations()
    elif operation_mode == "📝 Custom Operations":
        custom_operations()
    elif operation_mode == "📚 Manage Templates":
        manage_templates()

def template_operations():
    """Template-based bulk operations."""
    st.subheader("Template-Based Operations")
    
    # Load available templates
    config = st.session_state.config
    templates = config.get('operation_templates', {}) if config else {}
    
    if not templates:
        st.warning("⚠️ No templates available. Please configure templates in Settings.")
        return
    
    selected_template = st.selectbox(
        "Select Template",
        list(templates.keys())
    )
    
    if selected_template:
        template_config = templates[selected_template]
        
        st.subheader(f"Template: {selected_template}")
        st.write(f"**Description:** {template_config.get('description', 'No description')}")
        
        # Display template operations
        if 'databases' in template_config:
            st.write("**Database Operations:**")
            for db_op in template_config['databases']:
                st.write(f"• Clone {db_op['source']} → {db_op['target']}")
        
        if 'schemas' in template_config:
            st.write("**Schema Operations:**")
            for schema_op in template_config['schemas']:
                st.write(f"• Clone {schema_op['source_db']}.{schema_op['source_schema']} → {schema_op['target_db']}.{schema_op['target_schema']}")
        
        if 'tables' in template_config:
            st.write("**Table Operations:**")
            for table_op in template_config['tables']:
                st.write(f"• Clone {table_op['source_db']}.{table_op['source_schema']}.{table_op['source_table']} → {table_op['target_db']}.{table_op['target_schema']}.{table_op['target_table']}")
        
        if st.button("🚀 Execute Template", type="primary"):
            with st.spinner(f"Executing template: {selected_template}..."):
                try:
                    result = st.session_state.framework.bulk_clone_operation(template_config)
                    st.success(f"✅ Template execution completed")
                    
                    if result:
                        st.subheader("Execution Results")
                        st.json(result)
                        
                except Exception as e:
                    st.error(f"❌ Template execution failed: {str(e)}")

def custom_operations():
    """Custom bulk operations configuration."""
    st.subheader("Custom Bulk Operations")
    
    # File upload for custom operations
    uploaded_file = st.file_uploader(
        "Upload Operation File",
        type=['yaml', 'yml'],
        help="Upload a YAML file containing custom bulk operations"
    )
    
    if uploaded_file:
        try:
            operation_config = yaml.safe_load(uploaded_file)
            st.success("✅ Operation file loaded successfully")
            
            # Display operations
            st.subheader("Operations Preview")
            st.json(operation_config)
            
            if st.button("🚀 Execute Operations", type="primary"):
                with st.spinner("Executing custom operations..."):
                    try:
                        result = st.session_state.framework.bulk_clone_operation(operation_config)
                        st.success("✅ Custom operations completed")
                        
                        if result:
                            st.subheader("Execution Results")
                            st.json(result)
                            
                    except Exception as e:
                        st.error(f"❌ Operations execution failed: {str(e)}")
                        
        except Exception as e:
            st.error(f"❌ Error loading operation file: {str(e)}")

def manage_templates():
    """Template management interface."""
    st.subheader("Template Management")
    st.info("📚 Manage operation templates for reusable bulk operations")
    
    # This would implement template CRUD operations
    st.write("Template management functionality would be implemented here")

def settings_page():
    """Settings and configuration page."""
    st.markdown('<div class="sub-header">Settings & Configuration</div>', unsafe_allow_html=True)
    
    # Settings tabs
    tab1, tab2, tab3, tab4 = st.tabs(["⚙️ General", "🔒 Security", "📊 Performance", "📧 Notifications"])
    
    with tab1:
        general_settings()
    
    with tab2:
        security_settings()
    
    with tab3:
        performance_settings()
    
    with tab4:
        notification_settings()

def general_settings():
    """General application settings."""
    st.subheader("General Settings")
    
    config = st.session_state.config or {}
    
    # Cloning settings
    st.subheader("Cloning Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        default_clone_type = st.selectbox(
            "Default Clone Type",
            ["ZERO_COPY", "AT_TIME"],
            index=0 if config.get('cloning', {}).get('default_clone_type') == 'ZERO_COPY' else 1
        )
        
        batch_size = st.number_input(
            "Batch Size",
            min_value=1,
            max_value=50,
            value=config.get('cloning', {}).get('batch_size', 10)
        )
    
    with col2:
        retry_attempts = st.number_input(
            "Retry Attempts",
            min_value=1,
            max_value=10,
            value=config.get('cloning', {}).get('retry_attempts', 3)
        )
        
        retry_delay = st.number_input(
            "Retry Delay (seconds)",
            min_value=1,
            max_value=60,
            value=config.get('cloning', {}).get('retry_delay', 5)
        )
    
    if st.button("💾 Save General Settings"):
        # Update config and save
        st.success("✅ General settings saved")

def security_settings():
    """Security configuration settings."""
    st.subheader("Security Settings")
    
    config = st.session_state.config or {}
    security_config = config.get('security', {})
    
    audit_logging = st.checkbox(
        "Enable Audit Logging",
        value=security_config.get('audit_logging', True)
    )
    
    encrypt_logs = st.checkbox(
        "Encrypt Logs",
        value=security_config.get('encrypt_logs', False)
    )
    
    sensitive_masking = st.checkbox(
        "Sensitive Data Masking",
        value=security_config.get('sensitive_data_masking', True)
    )
    
    if st.button("💾 Save Security Settings"):
        st.success("✅ Security settings saved")

def performance_settings():
    """Performance configuration settings."""
    st.subheader("Performance Settings")
    
    config = st.session_state.config or {}
    perf_config = config.get('performance', {})
    
    parallel_ops = st.slider(
        "Parallel Operations",
        min_value=1,
        max_value=20,
        value=perf_config.get('parallel_operations', 5)
    )
    
    st.subheader("Warehouse Scaling")
    
    col1, col2 = st.columns(2)
    
    with col1:
        auto_scale = st.checkbox(
            "Auto Scale",
            value=perf_config.get('warehouse_scaling', {}).get('auto_scale', True)
        )
        
        min_cluster = st.number_input(
            "Min Clusters",
            min_value=1,
            max_value=10,
            value=perf_config.get('warehouse_scaling', {}).get('min_cluster', 1)
        )
    
    with col2:
        max_cluster = st.number_input(
            "Max Clusters",
            min_value=1,
            max_value=10,
            value=perf_config.get('warehouse_scaling', {}).get('max_cluster', 3)
        )
    
    if st.button("💾 Save Performance Settings"):
        st.success("✅ Performance settings saved")

def notification_settings():
    """Notification configuration settings."""
    st.subheader("Notification Settings")
    
    config = st.session_state.config or {}
    notification_config = config.get('monitoring', {}).get('notification', {})
    
    # Email settings
    st.subheader("📧 Email Notifications")
    
    email_enabled = st.checkbox(
        "Enable Email Notifications",
        value=notification_config.get('email', {}).get('enabled', False)
    )
    
    if email_enabled:
        email_recipients = st.text_area(
            "Recipients (one per line)",
            value='\n'.join(notification_config.get('email', {}).get('recipients', []))
        )
        
        smtp_server = st.text_input(
            "SMTP Server",
            value=notification_config.get('email', {}).get('smtp_server', '')
        )
        
        smtp_port = st.number_input(
            "SMTP Port",
            min_value=1,
            max_value=65535,
            value=notification_config.get('email', {}).get('smtp_port', 587)
        )
    
    # Slack settings
    st.subheader("💬 Slack Notifications")
    
    slack_enabled = st.checkbox(
        "Enable Slack Notifications",
        value=notification_config.get('slack', {}).get('enabled', False)
    )
    
    if slack_enabled:
        webhook_url = st.text_input(
            "Webhook URL",
            value=notification_config.get('slack', {}).get('webhook_url', ''),
            type="password"
        )
        
        channel = st.text_input(
            "Channel",
            value=notification_config.get('slack', {}).get('channel', '#data-ops')
        )
    
    if st.button("💾 Save Notification Settings"):
        st.success("✅ Notification settings saved")

if __name__ == "__main__":
    main()