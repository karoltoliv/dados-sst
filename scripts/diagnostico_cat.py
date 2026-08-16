# -*- coding: utf-8 -*-
"""
diagnostico_cat.py (v2) — Investiga a hipótese de que a distribuição anômala
vem do COMPORTAMENTO DA FONTE (registros que entram por lotes administrativos
de concessão de benefício), e não de erro do pipeline.

Para cada mês-alvo (epicentros dos picos e vales da conferência), imprime:
- total de linhas do arquivo;
- distribuição de meses da Data Acidente;
- distribuição de 'Origem de Cadastramento CAT' (canal de entrada);
- distribuição de 'Emitente CAT' e 'Espécie do benefício'.
Leitura pura: não grava nada, não publica nada.
"""

import csv
import io
import zipfile
from collections import Counter

import pandas as pd
import requests

CKAN = ("https://dadosabertos.inss.gov.br/api/3/action/package_show"
        "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025")

# Epicentros: vales de 2024 (08–12), picos de 2025 (09–10 são os candidatos a
# carregar o lote atrasado de jul–ago/2025), vales de 2025 (11–12) e jan/2026.
COMPETENCIAS_ALVO = ["202409", "202412", "202509", "202510", "202511", "202512", "202601"]

COLUNAS_PERFIL = ["Origem de Cadastramento CAT", "Emitente CAT", "Espécie do benefício"]


def log(m):
    print(m, flush=True)


def mes_de(serie):
    s = serie.astype(str).str.strip()
    m = s.str.extract(r"^(\d{2})/(\d{2})/(\d{4})")
    return (m[2] + "-" + m[1]).fillna("SEM DATA")


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
        log("  NÃO ENCONTRADO na listagem.")
        continue
    resp = requests.get(rec["url"], timeout=600)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    nome_csv = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
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
    log(f"  Linhas: {len(df)}")
    if "Data Acidente" in df.columns:
        cont = Counter(mes_de(df["Data Acidente"]))
        log("  [Data Acidente] meses -> " + ", ".join(f"{k}:{v}" for k, v in sorted(cont.items())[:16]))
    for col in COLUNAS_PERFIL:
        if col not in df.columns:
            log(f"  [{col}]: COLUNA INEXISTENTE")
            continue
        cont = df[col].astype(str).str.strip().value_counts(dropna=False).head(8)
        log(f"  [{col}] -> " + ", ".join(f"{k}:{v}" for k, v in cont.items()))

log("\nLeitura: se os vales forem arquivos pequenos e os meses seguintes carregarem "
    "lotes grandes com acidentes dos meses anteriores, a causa é o ritmo administrativo "
    "da fonte (cadastramento/concessão), não o pipeline. A 'Origem de Cadastramento' "
    "mostra por qual canal os registros entram em cada lote.")
