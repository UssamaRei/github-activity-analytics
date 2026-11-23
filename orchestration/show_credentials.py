"""Run explicitly in your terminal to see local development UI credentials."""

from pathlib import Path

print('Username: student')
print('Password: ' + Path('/run/il05-secrets/ui_password').read_text().strip())
