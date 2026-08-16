# -*- coding: utf-8 -*-
"""
agrega_cat.py — Pipeline CAT/INSS (dados abertos, licença CC-BY)
App: Saúde, Trabalho & Território (TCC Fiocruz)

REGRAS INVIOLÁVEIS (especificação de 15/08/2026):
- Saída contém APENAS agregados: contagem de CAT e de óbitos por
  município do empregador × seção CNAE × mês. Nenhum microdado é publicado.
- Nada é fixado sem verificação: enquanto o mapeamento de colunas abaixo
  não for CONFIRMADO contra o cabeçalho real, o script roda apenas em
  modo inspeção e NÃO publica agregados.
- Nenhum dado fictício, nunca. Se algo não for encontrado, o script para
  com mensagem clara — jamais estima ou inventa.

MODO INSPEÇÃO (INSPECAO=1):
  Baixa o recurso mais recente, detecta separador/encoding, imprime o
  cabeçalho real e grava docs/inspecao_cat.json. Não agrega, não publica.

MODO NORMAL (INSPECAO=0):
  Só roda se COLUNAS_CONFIRMADAS = True (ajustar manualmente após a
  inspeção, com os nomes reais das colunas).
"""

import csv
import io
import json
import os
import sys
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

# ----------------------------------------------------------------------
# CONFIGURAÇÃO — ⚠️ preencher/confirmar APÓS o primeiro run em modo inspeção
# ----------------------------------------------------------------------

CKAN_PACKAGE_SHOW = (
    "https://dadosabertos.inss.gov.br/api/3/action/package_show"
    "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025"
)

# ⚠️ Trocar para True SOMENTE depois de conferir os nomes reais das colunas
# no log do modo inspeção e preencher MAPA_COLUNAS com os nomes exatos.
COLUNAS_CONFIRMADAS = False

# Nomes EXATOS das colunas no CSV (preencher após inspeção; não presumir).
MAPA_COLUNAS = {
    "municipio_empregador": "",  # ex.: a coluna real de município do empregador
    "cnae_codigo": "",           # código CNAE 2.0
    "indicador_obito": "",       # indicador de óbito
    "data_acidente": "",         # data do acidente (para o mês de referência)
}

# Valor que representa óbito na coluna indicador_obito (confirmar na inspeção).
VALOR_OBITO = ""

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
ARQ_AGREGADO = os.path.join(DOCS, "cat_agregado.json")
ARQ_META = os.path.join(DOCS, "cat_meta.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_cat.json")

# CNAE 2.0: divisão (2 dígitos) -> seção (fixo, classificação oficial IBGE)
def secao_cnae(codigo: str) -> str:
    try:
        div = int(str(codigo).strip().replace(".", "").replace("-", "")[:2])
    except (ValueError, TypeError):
        return "SEM CNAE"
    faixas = [
        (1, 3, "A"), (5, 9, "B"), (10, 33, "C"), (35, 35, "D"), (36, 39, "E"),
        (41, 43, "F"), (45, 47, "G"), (49, 53, "H"), (55, 56, "I"), (58, 63, "J"),
        (64, 66, "K"), (68, 68, "L"), (69, 75, "M"), (77, 82, "N"), (84, 84, "O"),
        (85, 85, "P"), (86, 88, "Q"), (90, 93, "R"), (94, 96, "S"), (97, 97, "T"),
        (99, 99, "U"),
    ]
    for ini, fim, sec in faixas:
        if ini <= div <= fim:
            return sec
    return "SEM CNAE"


def log(msg: str) -> None:
    print(f"[agrega_cat] {msg}", flush=True)


def listar_recursos():
    r = requests.get(CKAN_PACKAGE_SHOW, timeout=60)
    r.raise_for_status()
    dados = r.json()
    if not dados.get("success"):
        sys.exit("ERRO: package_show retornou success=false. Verificar id do dataset.")
    recursos = dados["result"].get("resources", [])
    zips = [x for x in recursos if str(x.get("url", "")).upper().endswith(".ZIP")]
    if not zips:
        sys.exit("ERRO: nenhum recurso .ZIP listado no dataset. Verificar manualmente no portal.")
    # Ordena pelo padrão AAAAMM presente no nome D.SDA.PDA.005.CAT.AAAAMM.ZIP
    def chave(rec):
        nome = rec.get("url", "") + rec.get("name", "")
        digitos = [s for s in nome.replace(".", " ").split() if s.isdigit() and len(s) == 6]
        return digitos[-1] if digitos else "000000"
    zips.sort(key=chave)
    return zips, chave


def baixar_csv(url: str):
    log(f"Baixando: {url}")
    r = requests.get(url, timeout=600)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    nomes_csv = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not nomes_csv:
        sys.exit(f"ERRO: ZIP sem CSV. Conteúdo: {zf.namelist()}")
    bruto = zf.read(nomes_csv[0])
    # Detecção de encoding: tenta utf-8, cai para latin-1 (verificado, não presumido)
    encoding_usado = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            texto = bruto.decode(enc)
            encoding_usado = enc
            break
        except UnicodeDecodeError:
            continue
    if encoding_usado is None:
        sys.exit("ERRO: não foi possível decodificar o CSV (utf-8/latin-1 falharam).")
    # Detecção de separador com csv.Sniffer sobre as primeiras linhas
    amostra = "\n".join(texto.splitlines()[:5])
    try:
        sep = csv.Sniffer().sniff(amostra, delimiters=";,|\t").delimiter
    except csv.Error:
        sep = ";"
    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str)
    return df, nomes_csv[0], encoding_usado, sep


def modo_inspecao():
    zips, chave = listar_recursos()
    mais_recente = zips[-1]
    df, nome_csv, enc, sep = baixar_csv(mais_recente["url"])
    achado = {
        "dataInspecao": datetime.now(timezone.utc).isoformat(),
        "recursoMaisRecente": {"url": mais_recente.get("url"), "competencia": chave(mais_recente)},
        "arquivoCSV": nome_csv,
        "encodingDetectado": enc,
        "separadorDetectado": sep,
        "colunasReais": list(df.columns),
        "totalLinhas": int(len(df)),
        "observacao": (
            "Preencher MAPA_COLUNAS e VALOR_OBITO em agrega_cat.py com os nomes/valores "
            "reais acima e mudar COLUNAS_CONFIRMADAS para True. Nada foi agregado nem publicado."
        ),
    }
    # Distribuição de valores da provável coluna de óbito ajuda a fixar VALOR_OBITO
    candidatas_obito = [c for c in df.columns if "OBITO" in c.upper() or "ÓBITO" in c.upper() or "MORTE" in c.upper()]
    if candidatas_obito:
        achado["colunasCandidatasObito"] = {
            c: df[c].value_counts(dropna=False).head(10).to_dict() for c in candidatas_obito
        }
    os.makedirs(DOCS, exist_ok=True)
    with open(ARQ_INSPECAO, "w", encoding="utf-8") as f:
        json.dump(achado, f, ensure_ascii=False, indent=2)
    log("=== RESULTADO DA INSPEÇÃO ===")
    log(json.dumps(achado, ensure_ascii=False, indent=2))
    log("Inspeção gravada em docs/inspecao_cat.json. Copiar este log e enviar ao Claude.")


def carregar_json(caminho, padrao):
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    return padrao


def modo_normal():
    if not COLUNAS_CONFIRMADAS or not all(MAPA_COLUNAS.values()) or not VALOR_OBITO:
        sys.exit(
            "PARADO POR SEGURANÇA: colunas não confirmadas. "
            "Rode primeiro com INSPECAO=1, confira o cabeçalho real e preencha "
            "MAPA_COLUNAS/VALOR_OBITO. Nada será fixado sem verificação."
        )
    zips, chave = listar_recursos()
    meta = carregar_json(ARQ_META, {"ultimoMesProcessado": None})
    agregado = carregar_json(ARQ_AGREGADO, {"schemaVersion": "1.0", "fonte": "CAT/INSS (dados abertos, CC-BY)", "series": {}})

    pendentes = [r for r in zips if meta["ultimoMesProcessado"] is None or chave(r) > meta["ultimoMesProcessado"]]
    if not pendentes:
        log("Nenhum recurso novo. Nada a fazer.")
        return

    for rec in pendentes:
        competencia = chave(rec)
        df, _, _, _ = baixar_csv(rec["url"])
        faltando = [v for v in MAPA_COLUNAS.values() if v not in df.columns]
        if faltando:
            sys.exit(f"ERRO: colunas confirmadas não encontradas neste CSV: {faltando}. "
                     f"Cabeçalho real: {list(df.columns)}. Reexecutar inspeção.")
        c = MAPA_COLUNAS
        df["_secao"] = df[c["cnae_codigo"]].map(secao_cnae)
        df["_mes"] = df[c["data_acidente"]].str[:7] if df[c["data_acidente"]].str.contains("-", na=False).any() else competencia
        df["_obito"] = (df[c["indicador_obito"]].str.strip().str.upper() == VALOR_OBITO.upper()).astype(int)

        grupo = df.groupby([c["municipio_empregador"], "_secao"], dropna=False).agg(
            cat=("_obito", "size"), obitos=("_obito", "sum")
        ).reset_index()

        agregado["series"][competencia] = [
            {
                "municipioEmpregador": str(linha[c["municipio_empregador"]]),
                "secaoCNAE": linha["_secao"],
                "cat": int(linha["cat"]),
                "obitos": int(linha["obitos"]),
                "fonte": "CAT/INSS",
            }
            for _, linha in grupo.iterrows()
        ]
        meta["ultimoMesProcessado"] = competencia
        log(f"Competência {competencia}: {len(grupo)} linhas agregadas (município × seção CNAE).")

    meta.update({
        "dataProcessamento": datetime.now(timezone.utc).isoformat(),
        "fonte": "INSS — Comunicações de Acidente de Trabalho (dados abertos)",
        "licenca": "CC-BY",
        "url": CKAN_PACKAGE_SHOW,
    })
    os.makedirs(DOCS, exist_ok=True)
    with open(ARQ_AGREGADO, "w", encoding="utf-8") as f:
        json.dump(agregado, f, ensure_ascii=False, indent=2)
    with open(ARQ_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    log("Agregados publicados em docs/cat_agregado.json e docs/cat_meta.json.")


if __name__ == "__main__":
    if os.environ.get("INSPECAO", "0") == "1":
        modo_inspecao()
    else:
        modo_normal()
