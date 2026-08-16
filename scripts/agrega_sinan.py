# -*- coding: utf-8 -*-
"""
agrega_sinan.py — Pipeline SINAN/DATASUS (agravos relacionados ao trabalho)
App: Saúde, Trabalho & Território (TCC Fiocruz)

REGRAS INVIOLÁVEIS (especificação de 15/08/2026):
- O JSON publicado contém APENAS agregados (princípio da minimização,
  LGPD art. 6º, III), ainda que o microdado DATASUS seja anonimizado.
- CAT e SINAN são séries paralelas: este arquivo NUNCA é somado ao da CAT.
- Recorte de município — decisão de Karol (15/08/2026):
    VIOL -> ID_MN_OCOR (ocorrência)
    demais agravos (sem campo de ocorrência) -> ID_MUNICIP (notificação)
    MUN_EMP NÃO é recorte; sua taxa de preenchimento é registrada nos
    metadados como evidência empírica (argumento da tese).
- Filtro ocupacional das fichas gerais:
    IEXO -> DOENCA_TRA = Sim   |   VIOL -> REL_TRAB = Sim
- Subnotificação intra-registro: filtro ocupacional positivo E CAT não
  emitida (campo CAT na IEXO; REL_CAT na VIOL).
- Anos ausentes (ex.: VIOLBR26) são tolerados e registrados como lacuna.
- Nada fixado sem verificação: caminho FTP e codificações Sim/Não são
  conferidos no modo inspeção antes do primeiro run normal.

MODO INSPEÇÃO (INSPECAO=1): lista o FTP, baixa amostras de IEXO e VIOL,
imprime colunas e distribuições dos campos de filtro/óbito/município.
Não agrega, não publica.
"""

import ftplib
import hashlib
import io
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pyreaddbc

# ----------------------------------------------------------------------
# CONFIGURAÇÃO — ⚠️ confirmar APÓS o primeiro run em modo inspeção
# ----------------------------------------------------------------------

FTP_HOST = "ftp.datasus.gov.br"
# Caminhos prováveis (padrão conhecido) — CONFIRMAR na inspeção, não presumir:
DIR_BASE = "/dissemin/publicos/SINAN/DADOS"
SUBDIRS = {"FINAIS": "final", "PRELIM": "preliminar"}

AGRAVOS = ["ACGR", "ACBI", "CANC", "DERM", "IEXO", "LERD", "MENT", "PAIR", "PNEU", "VIOL"]
ANO_INICIAL = 2023

# ⚠️ Trocar para True SOMENTE após conferir na inspeção (e no mapa-sinan.json
# do projeto) os valores reais de codificação.
CODIFICACAO_CONFIRMADA = False
COD_SIM = "1"          # valor que representa "Sim" — confirmar
VALORES_OBITO_EVOLUCAO = []  # códigos de EVOLUCAO que significam óbito — confirmar por agravo

CAMPO_FILTRO = {"IEXO": "DOENCA_TRA", "VIOL": "REL_TRAB"}   # verificados em 15/08/2026
CAMPO_CAT = {"IEXO": "CAT", "VIOL": "REL_CAT"}              # verificados em 15/08/2026
CAMPO_MUNICIPIO = {"VIOL": "ID_MN_OCOR"}                    # demais: ID_MUNICIP
MUNICIPIO_PADRAO = "ID_MUNICIP"

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
ARQ_AGREGADO = os.path.join(DOCS, "sinan_agregado.json")
ARQ_META = os.path.join(DOCS, "sinan_meta.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_sinan.json")


def log(msg: str) -> None:
    print(f"[agrega_sinan] {msg}", flush=True)


def conectar():
    ftp = ftplib.FTP(FTP_HOST, timeout=120)
    ftp.login()
    return ftp


def listar_dir(ftp, caminho):
    try:
        return ftp.nlst(caminho)
    except ftplib.error_perm:
        return None


def nome_arquivo(agravo, ano):
    return f"{agravo}BR{str(ano)[2:]}.dbc"


def ler_dbc(tmp_path):
    """Compatibilidade entre versões do pyreaddbc: a função de leitura já se
    chamou read_dbc e readdbc. Detecta qual existe em vez de presumir; se a
    assinatura não aceitar encoding, tenta sem. Se nada existir, para com
    mensagem clara listando o que a biblioteca oferece."""
    fn = getattr(pyreaddbc, "read_dbc", None) or getattr(pyreaddbc, "readdbc", None)
    if fn is None:
        disponiveis = [n for n in dir(pyreaddbc) if not n.startswith("_")]
        sys.exit(f"ERRO: nenhuma função de leitura conhecida em pyreaddbc. "
                 f"Funções disponíveis: {disponiveis}")
    try:
        return fn(tmp_path, encoding="iso-8859-1")
    except TypeError:
        return fn(tmp_path)


def baixar_dbc(ftp, caminho_remoto):
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {caminho_remoto}", buf.write)
    conteudo = buf.getvalue()
    h = hashlib.sha256(conteudo).hexdigest()
    with tempfile.NamedTemporaryFile(suffix=".dbc", delete=False) as tmp:
        tmp.write(conteudo)
        tmp_path = tmp.name
    df = ler_dbc(tmp_path)
    os.unlink(tmp_path)
    return df.astype(str), h, len(conteudo)


def localizar_arquivos(ftp):
    """Mapeia, por agravo×ano, onde o .dbc existe (FINAIS/PRELIM) — sem presumir."""
    mapa, lacunas = {}, []
    listagens = {}
    for sub in SUBDIRS:
        caminho = f"{DIR_BASE}/{sub}"
        listagens[sub] = listar_dir(ftp, caminho)
    ano_atual = datetime.now(timezone.utc).year
    for agravo in AGRAVOS:
        for ano in range(ANO_INICIAL, ano_atual + 1):
            alvo = nome_arquivo(agravo, ano)
            achado = None
            for sub, arquivos in listagens.items():
                if arquivos and any(a.upper().endswith(alvo.upper()) for a in arquivos):
                    achado = (f"{DIR_BASE}/{sub}/{alvo}", SUBDIRS[sub])
                    break
            if achado:
                mapa[(agravo, ano)] = achado
            else:
                lacunas.append({"agravo": agravo, "ano": ano, "arquivo": alvo})
    return mapa, lacunas, {k: (len(v) if v else 0) for k, v in listagens.items()}


def distribuicao(df, col, topo=8):
    if col in df.columns:
        return df[col].value_counts(dropna=False).head(topo).to_dict()
    return "CAMPO INEXISTENTE"


def taxa_preenchimento(df, col):
    if col not in df.columns:
        return "CAMPO INEXISTENTE"
    vazios = df[col].isin(["", "nan", "None", "NaN"]).sum() + df[col].isna().sum()
    return round(100.0 * (len(df) - int(vazios)) / max(len(df), 1), 2)


def modo_inspecao():
    ftp = conectar()
    mapa, lacunas, contagem_dirs = localizar_arquivos(ftp)
    achado = {
        "dataInspecao": datetime.now(timezone.utc).isoformat(),
        "ftp": {"host": FTP_HOST, "dirBase": DIR_BASE, "arquivosPorSubdir": contagem_dirs},
        "arquivosLocalizados": {f"{a}-{ano}": c[0] for (a, ano), c in sorted(mapa.items())},
        "lacunas": lacunas,
        "amostras": {},
        "observacao": (
            "Conferir: (1) caminho FTP real; (2) codificação de 'Sim' nos campos de filtro "
            "(comparar com mapa-sinan.json do projeto); (3) códigos de óbito em EVOLUCAO. "
            "Depois preencher COD_SIM/VALORES_OBITO_EVOLUCAO e mudar CODIFICACAO_CONFIRMADA "
            "para True. Nada foi agregado nem publicado."
        ),
    }
    # Amostras: IEXO e VIOL mais recentes disponíveis
    for agravo in ("IEXO", "VIOL"):
        anos = sorted([ano for (a, ano) in mapa if a == agravo])
        if not anos:
            achado["amostras"][agravo] = "NENHUM ARQUIVO LOCALIZADO"
            continue
        caminho, status = mapa[(agravo, anos[-1])]
        df, h, tam = baixar_dbc(ftp, caminho)
        achado["amostras"][agravo] = {
            "arquivo": caminho, "status": status, "sha256": h[:16], "linhas": len(df),
            "colunas": list(df.columns),
            "distFiltro": {CAMPO_FILTRO[agravo]: distribuicao(df, CAMPO_FILTRO[agravo])},
            "distCAT": {CAMPO_CAT[agravo]: distribuicao(df, CAMPO_CAT[agravo])},
            "distEVOLUCAO": distribuicao(df, "EVOLUCAO"),
            "temDT_OBITO": "DT_OBITO" in df.columns,
            "preenchimentoMunicipio": {
                "ID_MUNICIP": taxa_preenchimento(df, "ID_MUNICIP"),
                "ID_MN_OCOR": taxa_preenchimento(df, "ID_MN_OCOR"),
                "MUN_EMP": taxa_preenchimento(df, "MUN_EMP"),
            },
        }
    ftp.quit()
    os.makedirs(DOCS, exist_ok=True)
    with open(ARQ_INSPECAO, "w", encoding="utf-8") as f:
        json.dump(achado, f, ensure_ascii=False, indent=2)
    log("=== RESULTADO DA INSPEÇÃO ===")
    log(json.dumps(achado, ensure_ascii=False, indent=2))
    log("Inspeção gravada em docs/inspecao_sinan.json. Copiar este log e enviar ao Claude.")


def carregar_json(caminho, padrao):
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    return padrao


def eh_obito(df):
    """Óbito: DT_OBITO preenchida quando o campo existe; senão EVOLUCAO nos códigos confirmados."""
    if "DT_OBITO" in df.columns:
        return (~df["DT_OBITO"].isin(["", "nan", "None"])) & df["DT_OBITO"].notna()
    if "EVOLUCAO" in df.columns and VALORES_OBITO_EVOLUCAO:
        return df["EVOLUCAO"].isin([str(v) for v in VALORES_OBITO_EVOLUCAO])
    return pd.Series(False, index=df.index)


def modo_normal():
    if not CODIFICACAO_CONFIRMADA:
        sys.exit(
            "PARADO POR SEGURANÇA: codificações não confirmadas. Rode primeiro com "
            "INSPECAO=1, confira COD_SIM e VALORES_OBITO_EVOLUCAO contra o mapa-sinan.json "
            "e o log da inspeção. Nada será fixado sem verificação."
        )
    ftp = conectar()
    mapa, lacunas, _ = localizar_arquivos(ftp)
    meta = carregar_json(ARQ_META, {"hashes": {}})
    agregado = carregar_json(ARQ_AGREGADO, {
        "schemaVersion": "1.0",
        "fonte": "SINAN/Ministério da Saúde (DATASUS) — agravos relacionados ao trabalho",
        "series": {},
    })
    status_por_ano, campo_mun_por_agravo, preench_mun_emp = {}, {}, {}

    for (agravo, ano), (caminho, status) in sorted(mapa.items()):
        chave = f"{agravo}-{ano}"
        # Re-baixa anos preliminares; anos finais já processados são pulados por hash
        hash_previo = meta["hashes"].get(chave)
        df, h, _ = baixar_dbc(ftp, caminho)
        if hash_previo == h and status == "final":
            log(f"{chave}: sem mudança (final). Pulado.")
            continue

        # Filtro ocupacional apenas nas fichas gerais (IEXO, VIOL);
        # os demais agravos são notificações específicas de ST — sem filtro.
        subnotif = None
        if agravo in CAMPO_FILTRO:
            campo_f = CAMPO_FILTRO[agravo]
            if campo_f not in df.columns:
                sys.exit(f"ERRO {chave}: campo de filtro {campo_f} inexistente. Colunas: {list(df.columns)}")
            df = df[df[campo_f].str.strip() == COD_SIM].copy()
            campo_cat = CAMPO_CAT[agravo]
            if campo_cat in df.columns:
                subnotif = df[df[campo_cat].str.strip() != COD_SIM]

        campo_mun = CAMPO_MUNICIPIO.get(agravo, MUNICIPIO_PADRAO)
        if campo_mun not in df.columns:
            sys.exit(f"ERRO {chave}: campo de município {campo_mun} inexistente. Colunas: {list(df.columns)}")
        campo_mun_por_agravo[agravo] = campo_mun
        if "MUN_EMP" in df.columns:
            preench_mun_emp[chave] = taxa_preenchimento(df, "MUN_EMP")

        df["_obito"] = eh_obito(df).astype(int)
        grupo = df.groupby(campo_mun, dropna=False).agg(
            notificacoes=("_obito", "size"), obitos=("_obito", "sum")
        ).reset_index()

        linhas = [
            {
                "municipio": str(l[campo_mun]),
                "agravo": agravo,
                "ano": ano,
                "notificacoes": int(l["notificacoes"]),
                "obitos": int(l["obitos"]),
                "preliminar": status == "preliminar",
                "campoMunicipio": campo_mun,
                "fonte": "SINAN/MS",
            }
            for _, l in grupo.iterrows()
        ]
        if subnotif is not None and len(subnotif) > 0:
            sub_grupo = subnotif.groupby(campo_mun, dropna=False).size().reset_index(name="n")
            for _, l in sub_grupo.iterrows():
                linhas.append({
                    "municipio": str(l[campo_mun]),
                    "agravo": agravo,
                    "ano": ano,
                    "indicador": "subnotificacao_intra_registro",
                    "definicao": "casos ocupacionais confirmados sem CAT emitida",
                    "casos": int(l["n"]),
                    "preliminar": status == "preliminar",
                    "campoMunicipio": campo_mun,
                    "fonte": "SINAN/MS",
                })

        agregado["series"][chave] = linhas
        meta["hashes"][chave] = h
        status_por_ano[chave] = status
        log(f"{chave} ({status}): {len(grupo)} municípios agregados.")

    ftp.quit()
    meta.update({
        "anosProcessados": sorted({ano for (_, ano) in mapa}),
        "statusPorAno": status_por_ano,
        "lacunas": lacunas,
        "dataProcessamento": datetime.now(timezone.utc).isoformat(),
        "fonte": "DATASUS — transferência de arquivos SINAN",
        "camposFiltro": CAMPO_FILTRO,
        "campoMunicipioPorAgravo": campo_mun_por_agravo,
        "preenchimentoMUN_EMP": preench_mun_emp,
    })
    os.makedirs(DOCS, exist_ok=True)
    with open(ARQ_AGREGADO, "w", encoding="utf-8") as f:
        json.dump(agregado, f, ensure_ascii=False, indent=2)
    with open(ARQ_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    log("Agregados publicados em docs/sinan_agregado.json e docs/sinan_meta.json.")


if __name__ == "__main__":
    if os.environ.get("INSPECAO", "0") == "1":
        modo_inspecao()
    else:
        modo_normal()
