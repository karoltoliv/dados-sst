# -*- coding: utf-8 -*-
"""
diagnostico_cat.py — Diagnóstico da anomalia detectada pela conferência.
Baixa 4 arquivos-amostra do PDA/INSS e responde duas perguntas:
(1) Cada ZIP contém quantos CSVs? (o pipeline lê só o primeiro)
(2) Qual é a distribuição de meses de CADA coluna de data ('Data Acidente'
    vs 'Data Acidente.1' vs 'Data Emissão CAT')? Qual delas se comporta
    como data de acidente de verdade?
Leitura pura: não grava nada, não publica nada.
"""

import csv
import io
import re
import sys
import zipfile
from collections import Counter

import pandas as pd
import requests

CKAN = ("https://dadosabertos.inss.gov.br/api/3/action/package_show"
        "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025")

COMPETENCIAS_ALVO = ["202308", "202405", "202508", "202605"]

COLUNAS_DATA = ["Data Acidente", "Data Acidente.1", "Data Emissão CAT",
                "Data  Afastamento", "Data Despacho Benefício"]


def log(m):
    print(m, flush=True)


def mes_de(serie):
    s = serie.astype(str).str.strip()
    m = s.str.extract(r"^(\d{2})/(\d{2})/(\d{4})")
    return (m[2] + "-" + m[1]).fillna("SEM DATA/OUTRO FORMATO")


r = requests.get(CKAN, timeout=60)
r.raise_for_status()
recursos = [x for x in r.json()["result"]["resources"]
            if str(x.get("url", "")).upper().endswith(".ZIP")]

def chave(rec):
    nome = rec.get("url", "") + rec.get("name", "")
    d = [s for s in nome.replace(".", " ").split() if s.isdigit() and len(s) == 6]
    return d[-1] if d else "000000"

por_comp = {chave(rec): rec for rec in recursos}

for comp in COMPETENCIAS_ALVO:
    rec = por_comp.get(comp)
    log(f"\n{'='*70}\nARQUIVO DA COMPETÊNCIA {comp}")
    if not rec:
        log("  NÃO ENCONTRADO na listagem do CKAN.")
        continue
    resp = requests.get(rec["url"], timeout=600)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    membros = zf.namelist()
    log(f"  Conteúdo do ZIP ({len(membros)} membro(s)):")
    for nome in membros:
        log(f"    - {nome} ({zf.getinfo(nome).file_size} bytes)")
    csvs = [n for n in membros if n.lower().endswith(".csv")]
    for i, nome_csv in enumerate(csvs):
        bruto = zf.read(nome_csv)
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                texto = bruto.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        try:
            sep = csv.Sniffer().sniff("\n".join(texto.splitlines()[:5]), delimiters=";,|\t").delimiter
        except csv.Error:
            sep = ";"
        df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        marca = " (LIDO PELO PIPELINE)" if i == 0 else " (IGNORADO PELO PIPELINE!)"
        log(f"\n  CSV {i+1}: {nome_csv}{marca} — {len(df)} linhas")
        for col in COLUNAS_DATA:
            if col not in df.columns:
                log(f"    [{col}]: COLUNA INEXISTENTE")
                continue
            cont = Counter(mes_de(df[col]))
            topo = ", ".join(f"{k}:{v}" for k, v in sorted(cont.items())[:14])
            log(f"    [{col}] meses -> {topo}")

log("\nInterpretação: a coluna cuja distribuição de meses for concentrada "
    "PRÓXIMA da competência do arquivo (com cauda para meses anteriores) é a "
    "data de acidente verdadeira. Uma coluna com meses espalhados ou "
    "concentrados em outro período é data administrativa (emissão/despacho).")
