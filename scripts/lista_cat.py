# -*- coding: utf-8 -*-
"""
lista_cat.py — Examina a LISTAGEM de recursos do dataset CAT no CKAN/INSS.
Objetivo: encontrar competências DUPLICADAS (mesma competência publicada
mais de uma vez — causa provável dos picos impossíveis na conferência) e
competências AUSENTES (causa provável dos vales). Só consulta a API;
não baixa ZIPs, não grava nada.
"""

import re
from collections import defaultdict
from datetime import datetime

import requests

CKAN = ("https://dadosabertos.inss.gov.br/api/3/action/package_show"
        "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025")

r = requests.get(CKAN, timeout=60)
r.raise_for_status()
recursos = r.json()["result"]["resources"]
zips = [x for x in recursos if str(x.get("url", "")).upper().endswith(".ZIP")]

print(f"Total de recursos no dataset: {len(recursos)} | ZIPs: {len(zips)}\n")

def chave(rec):
    nome = rec.get("url", "") + rec.get("name", "")
    d = [s for s in nome.replace(".", " ").split() if s.isdigit() and len(s) == 6]
    return d[-1] if d else "SEM_COMPETENCIA"

por_comp = defaultdict(list)
for rec in zips:
    por_comp[chave(rec)].append(rec)

print("=== LISTAGEM COMPLETA (competência | criado | modificado | nome | pasta da URL) ===")
for comp in sorted(por_comp):
    for rec in por_comp[comp]:
        url = rec.get("url", "")
        pasta = "/".join(url.split("/")[3:-1])[:60]
        print(f"{comp} | criado {str(rec.get('created'))[:10]} | modif {str(rec.get('last_modified'))[:10]} "
              f"| {rec.get('name','?')[:45]} | .../{pasta}")

print("\n=== COMPETÊNCIAS DUPLICADAS (mesma competência, mais de um ZIP) ===")
dups = {c: v for c, v in por_comp.items() if len(v) > 1}
if not dups:
    print("Nenhuma.")
for comp, lista in sorted(dups.items()):
    print(f"{comp}: {len(lista)} recursos")
    for rec in lista:
        print(f"   - {rec.get('name','?')} | {rec.get('url','')}")

print("\n=== COMPETÊNCIAS AUSENTES (jun/2023 até o mês atual) ===")
agora = datetime.utcnow()
esperadas = []
ano, mes = 2023, 6
while (ano, mes) <= (agora.year, agora.month):
    esperadas.append(f"{ano}{mes:02d}")
    mes += 1
    if mes == 13:
        ano, mes = ano + 1, 1
faltando = [c for c in esperadas if c not in por_comp]
print(", ".join(faltando) if faltando else "Nenhuma.")
