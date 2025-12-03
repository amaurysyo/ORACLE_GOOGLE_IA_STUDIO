# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 19:27:26 2025

@author: AMAURY
"""

# README.md  # why: guía rápida
# 1) conda create -n oraculo python=3.11 -y && conda activate oraculo
# 2) pip install -r requirements.txt
# 3) cp .env.example .env  # y edita PG_DSN + tokens
# 4) python scripts/cli.py health
# 5) python scripts/cli.py db:migrate core|hotfix|bt
# 6) python scripts/cli.py db:refresh-caggs --from "6 hours"
# 7) python scripts/cli.py telegram:test "Hola"