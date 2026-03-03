#!/bin/bash

# Simple launcher for the dbt Freshness & Run Status Analyzer
# 
# Usage: ./start.sh

echo "🔍 Starting dbt Freshness & Run Status Analyzer..."
echo ""
echo "The app will open in your browser at http://localhost:8501"
echo "Press Ctrl+C to stop the server"
echo ""

# Check if streamlit is installed
if ! command -v streamlit &> /dev/null
then
    echo "❌ Streamlit is not installed."
    echo ""
    echo "Install dependencies with:"
    echo "  pip install -r requirements.txt"
    exit 1
fi

# Run the app using python3 -m for better import handling
PYTHONDONTWRITEBYTECODE=1 python3 -m streamlit run app.py



