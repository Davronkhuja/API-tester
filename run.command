#!/bin/bash
cd "$(dirname "$0")"

# Eski instance bo'lsa o'chir
lsof -ti :5050 | xargs kill -9 2>/dev/null || true

# Kutubxonalar yo'q bo'lsa o'rnat
pip3 install -r requirements.txt -q 2>/dev/null

# Server ishga tushadi va brauzer avtomatik ochiladi
python3 api_runner.py
