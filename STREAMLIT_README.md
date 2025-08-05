# Snowflake Clone Framework - Web Interface

A modern, user-friendly web interface for the Snowflake Zero-Copy Cloning Framework built with Streamlit.

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Access to a Snowflake account
- Required Python packages (see `requirements.txt`)

### Installation & Setup

1. **Clone and navigate to the project directory**:
   ```bash
   cd snowflake-clone-framework
   ```

2. **Run the application**:

   **Linux/macOS:**
   ```bash
   chmod +x run_streamlit.sh
   ./run_streamlit.sh
   ```

   **Windows:**
   ```cmd
   run_streamlit.bat
   ```

   **Manual start:**
   ```bash
   # Create virtual environment
   python -m venv venv
   
   # Activate virtual environment
   # Linux/macOS:
   source venv/bin/activate
   # Windows:
   venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Run Streamlit app
   streamlit run streamlit_app.py
   ```

3. **Open your browser** and navigate to `http://localhost:8501`

## 📱 User Interface Overview

### 🏠 Dashboard
- **Overview metrics**: Total clones, success rates, active databases
- **Performance charts**: Clone operations over time, success rates by type
- **Recent activity**: Latest cloning and RBAC operations
- **Quick insights**: System health and usage statistics

### 🔌 Connection Management
- **Snowflake configuration**: Account, credentials, warehouse settings
- **Connection testing**: Validate credentials before use
- **Multiple authentication methods**: Username/password, SSO, key-pair
- **Connection status**: Real-time connection monitoring

### 🔍 Database Discovery
- **Structure exploration**: Browse databases, schemas, tables, and views
- **Metadata insights**: Row counts, table sizes, object types
- **Interactive navigation**: Expandable tree view of database structure
- **Search capabilities**: Find specific objects quickly

### 📋 Clone Operations
- **Database cloning**: Full database zero-copy cloning
- **Schema cloning**: Individual schema cloning with options
- **Table cloning**: Granular table-level cloning
- **Clone types**: Zero-copy and point-in-time cloning options
- **Progress tracking**: Real-time operation status and results

### 👥 RBAC Management
- **Role setup**: Automated Service Role (SR) and System Full (SFULL) creation
- **Privilege management**: Granular permission assignment
- **User assignments**: Map users to appropriate roles
- **Audit capabilities**: Review and validate RBAC configurations
- **Role hierarchy**: Visualize and manage role relationships

### 📊 Monitoring & Audit
- **Performance metrics**: Clone times, success rates, resource usage
- **Audit trail**: Complete history of operations and changes
- **Alert management**: Configure thresholds and notifications
- **Visual analytics**: Charts and graphs for trend analysis

### ⚡ Bulk Operations
- **Template execution**: Use predefined operation templates
- **Custom operations**: Upload and execute custom YAML configurations
- **Batch processing**: Execute multiple operations simultaneously
- **Progress tracking**: Monitor bulk operation status

### ⚙️ Settings
- **General configuration**: Clone settings, retry policies, naming conventions
- **Security settings**: Audit logging, encryption, data masking
- **Performance tuning**: Parallel operations, warehouse scaling
- **Notifications**: Email and Slack alert configuration

## 🎯 Key Features

### Zero-Copy Cloning
- **Instant cloning**: Leverage Snowflake's zero-copy technology
- **Cost-effective**: No data duplication, minimal storage overhead
- **Point-in-time options**: Clone from specific timestamps
- **Validation**: Automatic verification of clone operations

### RBAC Integration
- **Automated setup**: One-click role and privilege configuration
- **Best practices**: Pre-configured role templates
- **Compliance**: Audit-ready role management
- **Flexibility**: Customizable role hierarchies

### User Experience
- **Intuitive design**: Clean, modern interface
- **Responsive layout**: Works on desktop, tablet, and mobile
- **Real-time feedback**: Immediate status updates and progress indicators
- **Error handling**: Clear error messages and troubleshooting guidance

### Enterprise Ready
- **Security focused**: Secure credential handling and audit logging
- **Scalable**: Handle large databases and complex operations
- **Monitoring**: Comprehensive operational visibility
- **Integration**: API-ready for automation and integration

## 🔧 Configuration

### Initial Setup

1. **Configure Snowflake Connection**:
   - Navigate to the Connection page
   - Enter your Snowflake account details
   - Test the connection
   - Save the configuration

2. **Review Settings**:
   - Check default clone settings
   - Configure security preferences
   - Set up monitoring and alerts
   - Customize RBAC templates

### Configuration Files

The application uses `config.yaml` for configuration. Key sections include:

- **snowflake**: Connection parameters
- **rbac**: Role and privilege templates
- **cloning**: Default clone settings
- **monitoring**: Metrics and alerting
- **security**: Audit and privacy settings

## 🛡️ Security Considerations

### Credential Management
- **Secure storage**: Passwords encrypted in configuration
- **Multiple auth methods**: Support for SSO and key-pair authentication
- **Session management**: Automatic session timeouts
- **Audit logging**: Complete access and operation logging

### Network Security
- **Local deployment**: Runs locally for maximum security
- **No external dependencies**: All processing happens locally
- **Configurable access**: Control IP and time-based restrictions

### Data Privacy
- **Sensitive data masking**: Option to mask sensitive information
- **Audit compliance**: Complete operation audit trails
- **Role-based access**: Granular permission controls

## 🚨 Troubleshooting

### Common Issues

1. **Connection Failures**:
   - Verify Snowflake account name format
   - Check network connectivity
   - Validate credentials and permissions
   - Review warehouse accessibility

2. **Clone Operation Errors**:
   - Ensure source objects exist
   - Check target naming conventions
   - Verify sufficient privileges
   - Monitor warehouse capacity

3. **RBAC Setup Issues**:
   - Confirm SYSADMIN or higher role
   - Check role creation permissions
   - Validate privilege grant capabilities
   - Review role hierarchy conflicts

4. **Performance Issues**:
   - Adjust parallel operation settings
   - Scale warehouse appropriately
   - Monitor resource usage
   - Optimize batch sizes

### Getting Help

1. **Check logs**: Review application logs for detailed error information
2. **Validate configuration**: Ensure all settings are correct
3. **Test connectivity**: Use the built-in connection test feature
4. **Review documentation**: Check the main README and configuration guides

## 🔄 Updates and Maintenance

### Regular Maintenance
- **Update dependencies**: Keep Python packages current
- **Review configurations**: Validate settings periodically
- **Monitor performance**: Track operation metrics
- **Audit security**: Review access logs and permissions

### Version Updates
- **Backup configurations**: Save current settings before updates
- **Test in development**: Validate updates in non-production environment
- **Monitor changes**: Review change logs and release notes

## 📞 Support

For technical support and questions:

1. **Documentation**: Check the main README and configuration guides
2. **Logs**: Review application and Snowflake logs
3. **Configuration**: Validate all settings and connections
4. **Testing**: Use the built-in test features

## 🎉 Getting Started Tips

1. **Start with discovery**: Explore your database structure first
2. **Test with small objects**: Begin with schema or table clones
3. **Use templates**: Leverage pre-built operation templates
4. **Monitor operations**: Watch the dashboard for insights
5. **Configure alerts**: Set up notifications for important events

Happy cloning! 🎯