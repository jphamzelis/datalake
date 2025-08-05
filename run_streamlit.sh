#!/bin/bash
# Snowflake Clone Framework - Streamlit Web Interface Launcher

echo "🚀 Starting Snowflake Clone Framework Web Interface..."
echo "=================================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Install/upgrade dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Check if config file exists
if [ ! -f "config.yaml" ]; then
    echo "⚠️  Warning: config.yaml not found. You'll need to configure Snowflake connection in the app."
fi

# Set Streamlit configuration
export STREAMLIT_SERVER_HEADLESS=true
export STREAMLIT_SERVER_ENABLE_CORS=false

echo "🌐 Starting Streamlit application..."
echo "📱 Open your browser and navigate to: http://localhost:8501"
echo "⏹️  Press Ctrl+C to stop the application"
echo ""

# Run Streamlit app
streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0