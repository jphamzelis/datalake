@echo off
REM Snowflake Clone Framework - Streamlit Web Interface Launcher (Windows)

echo 🚀 Starting Snowflake Clone Framework Web Interface...
echo ==================================================

REM Check if virtual environment exists
if not exist "venv" (
    echo 📦 Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
echo 🔄 Activating virtual environment...
call venv\Scripts\activate.bat

REM Install/upgrade dependencies
echo 📦 Installing dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Check if config file exists
if not exist "config.yaml" (
    echo ⚠️  Warning: config.yaml not found. You'll need to configure Snowflake connection in the app.
)

echo 🌐 Starting Streamlit application...
echo 📱 Open your browser and navigate to: http://localhost:8501
echo ⏹️  Press Ctrl+C to stop the application
echo.

REM Run Streamlit app
streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0

pause