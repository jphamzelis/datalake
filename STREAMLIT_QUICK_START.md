# 🚀 Snowflake Clone Framework - Streamlit Quick Start

This guide will get your Snowflake Clone Framework web interface up and running in minutes!

## ⚡ Super Quick Start

### Option 1: Automated Setup (Recommended)

**Linux/macOS:**
```bash
chmod +x run_streamlit.sh
./run_streamlit.sh
```

**Windows:**
```cmd
run_streamlit.bat
```

### Option 2: Manual Setup

```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate it
source venv/bin/activate  # Linux/macOS
# OR
venv\Scripts\activate     # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the app
streamlit run streamlit_app.py
```

## 🌐 Access the Application

Once started, open your browser and go to: **http://localhost:8501**

## 🎯 First Steps in the App

1. **📱 Connection Setup**
   - Go to "🔌 Connection" page
   - Enter your Snowflake credentials
   - Test and save connection

2. **🔍 Explore Your Data**
   - Visit "🔍 Discovery" page
   - Enter database name (e.g., "PROD_DATALAKE")
   - Click "Discover Structure"

3. **📋 Start Cloning**
   - Navigate to "📋 Clone Operations"
   - Choose operation type (Database/Schema/Table)
   - Fill in source and target details
   - Execute clone operation

## 🎮 Key Features Available

| Page | Description | Key Actions |
|------|-------------|-------------|
| 🏠 Dashboard | Overview & metrics | View clone statistics, recent activity |
| 🔌 Connection | Snowflake setup | Configure credentials, test connection |
| 🔍 Discovery | Database exploration | Browse schemas, tables, metadata |
| 📋 Clone Operations | Create clones | Database, schema, table cloning |
| 👥 RBAC Management | Role management | Setup roles, audit permissions |
| 📊 Monitoring | Performance tracking | View metrics, audit trails, alerts |
| ⚡ Bulk Operations | Batch processing | Template execution, custom operations |
| ⚙️ Settings | Configuration | General, security, performance settings |

## 🛠️ Requirements

- **Python**: 3.8 or higher
- **Snowflake Account**: With appropriate permissions
- **Dependencies**: Automatically installed via `requirements.txt`

## 🔒 Security Notes

- **Credentials**: Stored locally in `config.yaml`
- **Network**: Application runs locally (localhost:8501)
- **Data**: No data leaves your environment

## 🆘 Troubleshooting

**Connection Issues?**
- Verify Snowflake account format: `account.snowflakecomputing.com`
- Check username/password
- Ensure network connectivity

**App Won't Start?**
- Check Python version: `python --version`
- Install dependencies: `pip install -r requirements.txt`
- Try different port: `streamlit run streamlit_app.py --server.port 8502`

**Missing Features?**
- Make sure you're connected to Snowflake
- Check role permissions
- Review configuration settings

## 📚 Learn More

- **Full Documentation**: See `STREAMLIT_README.md`
- **Framework Details**: Check main `README.md`
- **Configuration**: Review `config.yaml` structure

## 🎉 You're Ready!

Your Snowflake Clone Framework web interface is now ready to use. Enjoy the modern, intuitive experience for managing your zero-copy cloning operations!

**Happy Cloning! ❄️**