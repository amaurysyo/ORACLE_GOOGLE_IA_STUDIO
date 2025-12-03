# run_env_doctor_deribit_from_spyder.py
# Runner mínimo recomendado (desde Spyder) usando el CLI oficial
from scripts.cli import cli
cli.main(["env","doctor","deribit","--count","3","--upsert"], standalone_mode=False)
print("OK: env doctor deribit")
