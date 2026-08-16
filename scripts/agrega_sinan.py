# -*- coding: utf-8 -*-
"""
agrega_sinan.py — Pipeline SINAN/DATASUS (agravos relacionados ao trabalho)
App: Saúde, Trabalho & Território (TCC Fiocruz)

VERSÃO DESTRAVADA em 16/08/2026, após verificação tripla:
(1) inspeção dos microdados reais (docs/inspecao_sinan.json);
(2) mapa-sinan.json do projeto (Dicionário SINAN NET v5.0);
(3) dicionário oficial da ficha de violência (SINAN NET v5.0/Patch 5.1).

REGRAS INVIOLÁVEIS (especificação de 15/08/2026):
- JSON publicado contém APENAS agregados (LGPD art. 6º, III).
- CAT e SINAN são séries paralelas: NUNCA somadas.
- Município: VIOL -> ID_MN_OCOR (ocorrência); demais -> ID_MUNICIP
  (notificação). MUN_EMP não é recorte; sua taxa de preenchimento é
  registrada nos metadados (evidência empírica da tese).
- Anos ausentes (VIOLBR26) tolerados e registrados como lacuna.
- Nenhum dado fictício, nunca.

DEFINIÇÕES VERIFICADAS (16/08/2026):
- Sim = "1" nos filtros DOENCA_TRA (IEXO) e REL_TRAB (VIOL).
- Óbito: NÃO existe código único no SINAN (aviso do mapa-sinan.json).
  Conta-se EVOLUCAO igual ao código de óbito PELO AGRAVO específico:
  ACGR=5, ACBI=5, LERD/PAIR/DERM/PNEU/MENT/CANC=6, IEXO=3.
  Óbitos por outra causa NÃO contam.
- VIOL: a ficha oficial NÃO possui campo de evolução/óbito (dicionário
  v5.0/5.1); EVOLUCAO/DT_OBITO no microdado são residuais (>99% vazios).
  Óbitos de VIOL saem como null — devem ser buscados no SIM.
- Subnotificação intra-registro (definição ESTRITA): filtro ocupacional
  positivo E CAT/REL_CAT = "2" (Não). Exclui "não se aplica" (3 na IEXO,
  8 na VIOL) — trabalhador fora do regime da CAT não está subnotificado —
  e exclui "ignorado"/vazio.
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
# CONFIGURAÇÃO — verificada em 16/08/2026 (tripla checagem)
# ----------------------------------------------------------------------

FTP_HOST = "ftp.datasus.gov.br"
DIR_BASE = "/dissemin/publicos/SINAN/DADOS"   # confirmado na inspeção
SUBDIRS = {"FINAIS": "final", "PRELIM": "preliminar"}

AGRAVOS = ["ACGR", "ACBI", "CANC", "DERM", "IEXO", "LERD", "MENT", "PAIR", "PNEU", "VIOL"]
ANO_INICIAL = 2023

CODIFICACAO_CONFIRMADA = True   # inspeção + mapa-sinan.json + dicionário VIOL
COD_SIM = "1"
VALOR_CAT_NAO_EMITIDA = "2"     # definição estrita de subnotificação

# Código de EVOLUCAO que significa óbito PELO agravo (mapa-sinan.json).
# None = a ficha não possui campo de óbito confiável (caso VIOL).
CODIGO_OBITO_POR_AGRAVO = {
    "ACGR": "5", "ACBI": "5",
    "LERD": "6", "PAIR": "6", "DERM": "6", "PNEU": "6", "MENT": "6", "CANC": "6",
    "IEXO": "3",
    "VIOL": None,
}

CAMPO_FILTRO = {"IEXO": "DOENCA_TRA", "VIOL": "REL_TRAB"}
CAMPO_CAT = {"IEXO": "CAT", "VIOL": "REL_CAT"}
CAMPO_MUNICIPIO = {"VIOL": "ID_MN_OCOR"}
MUNICIPIO_PADRAO = "ID_MUNICIP"

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
ARQ_AGREGADO = os.path.join(DOCS, "sinan_agregado.json")
ARQ_META = os.path.join(DOCS, "sinan_meta.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_sinan.json")


def log(msg: str) -> None:
    print(f"[agrega_sinan] {msg}", flush=True)


def conectar():
    ftp = ftplib.FTP(FTP_HOST, timeout=300)
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
    """Lê um .dbc de forma robusta entre versões do pyreaddbc (função direta,
    submódulo ou fallback dbc2dbf+dbfread). Nunca presume, nunca inventa."""
    sub = getattr(pyreaddbc, "readdbc", None)
    candidatos = []
    f = getattr(pyreaddbc, "read_dbc", None)
    if callable(f):
        candidatos.append(f)
    if callable(sub):
        candidatos.append(sub)
    elif sub is not None:
        for nome in ("read_dbc", "readdbc"):
            g = getattr(sub, nome, None)
            if callable(g):
                candidatos.append(g)
    for fn in candidatos:
        try:
            return fn(tmp_path, encoding="iso-8859-1")
        except TypeError:
            try:
                return fn(tmp_path)
            except TypeError:
                continue
    conv = getattr(pyreaddbc, "dbc2dbf", None)
    if conv is None and sub is not None and not callable(sub):
        conv = getattr(sub, "dbc2dbf", None)
    if callable(conv):
        from dbfread import DBF
        dbf_path = tmp_path[:-4] + ".dbf"
        conv(tmp_path, dbf_path)
        tabela = DBF(dbf_path, encoding="iso-8859-1", char_decode_errors="replace")
        df = pd.DataFrame(iter(tabela))
        os.unlink(dbf_path)
        return df
    disponiveis = [n for n in dir(pyreaddbc) if not n.startswith("_")]
    sys.exit(f"ERRO: não foi possível ler o .dbc. pyreaddbc oferece: {disponiveis}")


def baixar_dbc(ftp, caminho_remoto, colunas_necessarias=None):
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {caminho_remoto}", buf.write)
    conteudo = buf.getvalue()
    h = hashlib.sha256(conteudo).hexdigest()
    with tempfile.NamedTemporaryFile(suffix=".dbc", delete=False) as tmp:
        tmp.write(conteudo)
        tmp_path = tmp.name
    df = ler_dbc(tmp_path)
    os.unlink(tmp_path)
    if colunas_necessarias:
        presentes = [c for c in colunas_necessarias if c in df.columns]
        df = df[presentes]
    return df.astype(str), h, len(conteudo)


def localizar_arquivos(ftp):
    mapa, lacunas = {}, []
    listagens = {}
    for sub in SUBDIRS:
        listagens[sub] = listar_dir(ftp, f"{DIR_BASE}/{sub}")
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
    }
    for agravo in ("IEXO", "VIOL"):
        anos = sorted([ano for (a, ano) in mapa if a == agravo])
        if not anos:
            achado["amostras"][agravo] = "NENHUM ARQUIVO LOCALIZADO"
            continue
        caminho, status = mapa[(agravo, anos[-1])]
        df, h, _ = baixar_dbc(ftp, caminho)
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


def carregar_json(caminho, padrao):
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    return padrao


def rotulo_municipio(serie):
    s = serie.astype(str).str.strip()
    return s.where(~s.isin(["", "nan", "None", "NaN"]), "IGNORADO")


def modo_normal():
    if not CODIFICACAO_CONFIRMADA:
        sys.exit("PARADO POR SEGURANÇA: codificações não confirmadas.")
    ftp = conectar()
    mapa, lacunas, _ = localizar_arquivos(ftp)
    meta = carregar_json(ARQ_META, {"hashes": {}})
    agregado = carregar_json(ARQ_AGREGADO, {
        "schemaVersion": "1.1",
        "fonte": "SINAN/Ministério da Saúde (DATASUS) — agravos relacionados ao trabalho",
        "series": {},
    })
    status_por_ano, campo_mun_por_agravo, preench_mun_emp = {}, {}, {}

    for (agravo, ano), (caminho, status) in sorted(mapa.items()):
        chave = f"{agravo}-{ano}"
        campo_mun = CAMPO_MUNICIPIO.get(agravo, MUNICIPIO_PADRAO)
        cod_obito = CODIGO_OBITO_POR_AGRAVO.get(agravo)

        hash_previo = meta["hashes"].get(chave)
        colunas = [campo_mun, "MUN_EMP", "EVOLUCAO",
                   CAMPO_FILTRO.get(agravo), CAMPO_CAT.get(agravo)]
        colunas = [c for c in colunas if c]
        df, h, _ = baixar_dbc(ftp, caminho, colunas_necessarias=colunas)
        if hash_previo == h and status == "final":
            log(f"{chave}: sem mudança (final). Pulado.")
            continue

        # Filtro ocupacional apenas nas fichas gerais (IEXO, VIOL);
        # os demais agravos são fichas específicas de ST — sem filtro.
        subnotif = None
        if agravo in CAMPO_FILTRO:
            campo_f = CAMPO_FILTRO[agravo]
            if campo_f not in df.columns:
                sys.exit(f"ERRO {chave}: campo de filtro {campo_f} inexistente. "
                         f"Colunas: {list(df.columns)}")
            df = df[df[campo_f].str.strip() == COD_SIM].copy()
            campo_cat = CAMPO_CAT[agravo]
            if campo_cat in df.columns:
                subnotif = df[df[campo_cat].str.strip() == VALOR_CAT_NAO_EMITIDA]

        if campo_mun not in df.columns:
            sys.exit(f"ERRO {chave}: campo de município {campo_mun} inexistente. "
                     f"Colunas: {list(df.columns)}")
        campo_mun_por_agravo[agravo] = campo_mun
        if "MUN_EMP" in df.columns:
            preench_mun_emp[chave] = taxa_preenchimento(df, "MUN_EMP")

        df["_mun"] = rotulo_municipio(df[campo_mun])
        if cod_obito is not None and "EVOLUCAO" in df.columns:
            df["_obito"] = (df["EVOLUCAO"].str.strip() == cod_obito).astype(int)
        else:
            df["_obito"] = 0

        grupo = df.groupby("_mun", dropna=False).agg(
            notificacoes=("_obito", "size"), obitos=("_obito", "sum")
        ).reset_index()

        linhas = [
            {
                "municipio": str(l["_mun"]),
                "agravo": agravo,
                "ano": ano,
                "notificacoes": int(l["notificacoes"]),
                "obitos": (int(l["obitos"]) if cod_obito is not None else None),
                "preliminar": status == "preliminar",
                "campoMunicipio": campo_mun,
                "fonte": "SINAN/MS",
            }
            for _, l in grupo.iterrows()
        ]
        if subnotif is not None and len(subnotif) > 0:
            sub = subnotif.copy()
            sub["_mun"] = rotulo_municipio(sub[campo_mun])
            sub_grupo = sub.groupby("_mun", dropna=False).size().reset_index(name="n")
            for _, l in sub_grupo.iterrows():
                linhas.append({
                    "municipio": str(l["_mun"]),
                    "agravo": agravo,
                    "ano": ano,
                    "indicador": "subnotificacao_intra_registro",
                    "definicao": "filtro ocupacional positivo e CAT nao emitida (codigo 2 - Nao)",
                    "casos": int(l["n"]),
                    "preliminar": status == "preliminar",
                    "campoMunicipio": campo_mun,
                    "fonte": "SINAN/MS",
                })

        agregado["series"][chave] = linhas
        meta["hashes"][chave] = h
        status_por_ano[chave] = status
        log(f"{chave} ({status}): {len(grupo)} municípios agregados"
            + ("" if cod_obito is not None else " — óbitos: null (ficha sem campo de óbito)"))

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
        "definicoes": {
            "codificacaoSim": COD_SIM,
            "obito": "EVOLUCAO igual ao código de óbito PELO agravo específico "
                     "(ACGR/ACBI=5; LERD/PAIR/DERM/PNEU/MENT/CANC=6; IEXO=3), "
                     "conforme dicionários SINAN NET v5.0; óbitos por outra causa não contam.",
            "obitoVIOL": "null — a ficha de violência (dicionário v5.0/5.1) não possui campo "
                         "de evolução/óbito; valores residuais no microdado (>99% vazios) não "
                         "são confiáveis. Óbitos por violência: consultar o SIM.",
            "subnotificacaoIntraRegistro": "filtro ocupacional positivo E CAT/REL_CAT = 2 (Não). "
                                           "Definição estrita: exclui 'não se aplica' (3/8) e "
                                           "'ignorado' (9).",
            "municipio": "código IBGE de 6 dígitos conforme a fonte; vazio = IGNORADO",
        },
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
