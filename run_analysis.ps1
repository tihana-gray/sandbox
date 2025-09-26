Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
& "$env:USERPROFILE\.venvs\sandbox\Scripts\Activate.ps1"
python ".\ga4_insights\analyse.py"
