@echo off
cd /d %~dp0

REM اصنع بيئة افتراضية أول مرة
if not exist .venv (
  py -m venv .venv
)

REM فعّل البيئة ونصّب المتطلبات وشغّل السيرفر
call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt

uvicorn server:app --host 0.0.0.0 --port 8000
