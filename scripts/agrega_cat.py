# -*- coding: utf-8 -*-
"""
agrega_cat.py (v2) — Pipeline CAT/INSS em DOIS NÍVEIS de arquivo.
Reestruturação de 17/08/2026 (Bloco C1): o arquivo nacional único de detalhe
fino excedia 50 MB e inviabilizava o consumo em navegador móvel.

SAÍDA:
- docs/cat_nacional.json — 1 registro por município × mês (soma de todas as
  seções CNAE). Campos: municipioEmpregador, ufEmpregador, mes, totalCat,
  totalObitos. Leve, baixado sempre pelo app.
- docs/cat_uf/[SIGLA].json — 27 arquivos, detalhe por município × mês ×
  seção CNAE daquela UF (campos do schema 1.1, sem renomear). Baixado sob
  demanda quando a UF é selecionada.
- docs/cat_meta.json — inclui tamanho em bytes de cada arquivo gerado.

Regras preservadas: mês = data do acidente; somar linhas de mesma chave;
UF desambigua município homônimo; avisos de cobertura parcial (2023 inicial;
set–dez/2024; nov–dez/2025); nenhum dado fictício.

MIGRAÇÃO: se docs/cat_nacional.json não existir, refaz a série completa
(reprocessa todos os meses) e remove o obsoleto docs/cat_agregado.json.
Arquivos gravados MINIFICADOS (sem indentação).
"""

import csv
import io
import json
import os
import sys
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import requests

CKAN_PACKAGE_SHOW = (
    "https://dadosabertos.inss.gov.br/api/3/action/package_show"
    "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025"
)

COLUNAS_CONFIRMADAS = True  # inspeção de 16/08/2026

MAPA_COLUNAS = {
    "municipio_empregador": "Munic Empr",
    "uf_empregador": "UF Munic. Empregador",
    "cnae_codigo": "CNAE2.0 Empregador",
    "cnae_codigo_alt": "CNAE2.0 Empregador.1",
    "indicador_obito": "Indica Óbito Acidente",
    "data_acidente": "Data Acidente",
}
VALOR_OBITO = "Sim"

AVISOS_COBERTURA = [
    "Série inicia em jun/2023: o ano de 2023 é estruturalmente incompleto.",
    "Subcobertura da fonte identificada: set-dez/2024 e nov-dez/2025 (filtro de ano na extração do PDA).",
    "Meses recentes são parciais por natureza (série administrativa com defasagem irregular).",
]

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
DIR_UF = os.path.join(DOCS, "cat_uf")
ARQ_NACIONAL = os.path.join(DOCS, "cat_nacional.json")
ARQ_META = os.path.join(DOCS, "cat_meta.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_cat.json")
ARQ_OBSOLETO = os.path.join(DOCS, "cat_agregado.json")


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


def sigla_uf(valor: str) -> str:
    """Normaliza o valor da UF para nome de arquivo: maiúsculas, sem acentos,
    só letras/números. Vazio/nulo vira IGNORADO. Não presume formato da fonte."""
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none", "{ñ class}"):
        return "IGNORADO"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = "".join(ch for ch in s.upper() if ch.isalnum())
    return s or "IGNORADO"


def listar_recursos():
    r = requests.get(CKAN_PACKAGE_SHOW, timeout=60)
    r.raise_for_status()
    dados = r.json()
    if not dados.get("success"):
        sys.exit("ERRO: package_show retornou success=false.")
    zips = [x for x in dados["result"].get("resources", [])
            if str(x.get("url", "")).upper().endswith(".ZIP")]
    if not zips:
        sys.exit("ERRO: nenhum recurso .ZIP no dataset.")
    def chave(rec):
        nome = rec.get("url", "") + rec.get("name", "")
        d = [s for s in nome.replace(".", " ").split() if s.isdigit() and len(s) == 6]
        return d[-1] if d else "000000"
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
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            texto = bruto.decode(enc)
            encoding_usado = enc
            break
        except UnicodeDecodeError:
            continue
    else:
        sys.exit("ERRO: não foi possível decodificar o CSV.")
    amostra = "\n".join(texto.splitlines()[:5])
    try:
        sep = csv.Sniffer().sniff(amostra, delimiters=";,|\t").delimiter
    except csv.Error:
        sep = ";"
    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str)
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    return df, nomes_csv[0], encoding_usado, sep


def escolher_coluna_cnae(df):
    for col in (MAPA_COLUNAS["cnae_codigo"], MAPA_COLUNAS["cnae_codigo_alt"]):
        if col in df.columns:
            amostra = (df[col].dropna().astype(str).str.strip()
                       .str.replace(".", "", regex=False)
                       .str.replace("-", "", regex=False).head(200))
            if len(amostra) and amostra.str.match(r"^\d{2,}").mean() > 0.8:
                return col
    sys.exit("ERRO: nenhuma coluna CNAE com códigos numéricos em maioria.")


def derivar_mes(serie: pd.Series, competencia: str):
    s = serie.astype(str).str.strip()
    m = s.str.extract(r"^(\d{2})/(\d{2})/(\d{4})")
    if m[0].notna().mean() > 0.8:
        return (m[2] + "-" + m[1]), "data_acidente dd/mm/aaaa"
    m2 = s.str.extract(r"^(\d{4})-(\d{2})")
    if m2[0].notna().mean() > 0.8:
        return (m2[0] + "-" + m2[1]), "data_acidente aaaa-mm"
    return pd.Series(f"{competencia[:4]}-{competencia[4:]}", index=serie.index), \
        "FALLBACK: competência do arquivo"


def modo_inspecao():
    zips, chave = listar_recursos()
    df, nome_csv, enc, sep = baixar_csv(zips[-1]["url"])
    achado = {
        "dataInspecao": datetime.now(timezone.utc).isoformat(),
        "recursoMaisRecente": {"url": zips[-1].get("url"), "competencia": chave(zips[-1])},
        "arquivoCSV": nome_csv, "encodingDetectado": enc, "separadorDetectado": sep,
        "colunasReais": list(df.columns), "totalLinhas": int(len(df)),
        "valoresUFDistintos": sorted(df[MAPA_COLUNAS["uf_empregador"]].unique().tolist())[:40]
        if MAPA_COLUNAS["uf_empregador"] in df.columns else "COLUNA INEXISTENTE",
    }
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


def gravar_minificado(caminho, obj):
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    return os.path.getsize(caminho)


def modo_normal():
    if not COLUNAS_CONFIRMADAS:
        sys.exit("PARADO POR SEGURANÇA: colunas não confirmadas.")
    zips, chave = listar_recursos()
    meta = carregar_json(ARQ_META, {})

    reconstrucao = not os.path.exists(ARQ_NACIONAL)
    if reconstrucao:
        log("MIGRAÇÃO: cat_nacional.json não existe — refazendo a série completa (todos os meses).")
        meta["ultimoMesProcessado"] = None
        nacional_idx = {}
        uf_series = defaultdict(lambda: defaultdict(list))
    else:
        nac = carregar_json(ARQ_NACIONAL, {"registros": []})
        nacional_idx = {(r["municipioEmpregador"], r["ufEmpregador"], r["mes"]):
                        [r["totalCat"], r["totalObitos"]] for r in nac.get("registros", [])}
        uf_series = defaultdict(lambda: defaultdict(list))
        if os.path.isdir(DIR_UF):
            for nome in os.listdir(DIR_UF):
                if nome.endswith(".json"):
                    dado = carregar_json(os.path.join(DIR_UF, nome), {})
                    for mes, linhas in dado.get("series", {}).items():
                        uf_series[nome[:-5]][mes] = linhas

    pendentes = [r for r in zips
                 if meta.get("ultimoMesProcessado") is None or chave(r) > meta["ultimoMesProcessado"]]
    if not pendentes:
        log("Nenhum recurso novo. Nada a fazer.")
        return
    log(f"{len(pendentes)} arquivo(s) mensal(is) a processar.")

    metodos_mes = meta.get("metodoMesPorCompetencia", {})
    c = MAPA_COLUNAS
    for rec in pendentes:
        competencia = chave(rec)
        df, _, _, _ = baixar_csv(rec["url"])
        faltando = [x for x in (c["municipio_empregador"], c["uf_empregador"],
                                c["indicador_obito"], c["data_acidente"]) if x not in df.columns]
        if faltando:
            sys.exit(f"ERRO {competencia}: colunas ausentes {faltando}. "
                     f"Cabeçalho real: {list(df.columns)}.")
        col_cnae = escolher_coluna_cnae(df)
        df["_secao"] = df[col_cnae].map(secao_cnae)
        df["_mes"], metodo = derivar_mes(df[c["data_acidente"]], competencia)
        metodos_mes[competencia] = metodo
        df["_obito"] = (df[c["indicador_obito"]].str.casefold()
                        == VALOR_OBITO.casefold()).astype(int)

        grupo = df.groupby(["_mes", c["municipio_empregador"], c["uf_empregador"], "_secao"],
                           dropna=False).agg(cat=("_obito", "size"),
                                             obitos=("_obito", "sum")).reset_index()
        for _, l in grupo.iterrows():
            mun, uf_bruta = str(l[c["municipio_empregador"]]), str(l[c["uf_empregador"]])
            mes, sig = str(l["_mes"]), sigla_uf(l[c["uf_empregador"]])
            uf_series[sig][mes].append({
                "municipioEmpregador": mun, "ufEmpregador": uf_bruta,
                "secaoCNAE": l["_secao"], "cat": int(l["cat"]),
                "obitos": int(l["obitos"]), "fonte": "CAT/INSS",
            })
            chave_nac = (mun, uf_bruta, mes)
            atual = nacional_idx.setdefault(chave_nac, [0, 0])
            atual[0] += int(l["cat"])
            atual[1] += int(l["obitos"])
        meta["ultimoMesProcessado"] = competencia
        log(f"Competência {competencia}: {len(grupo)} linhas de detalhe processadas ({metodo}).")

    # ---- gravação dos dois níveis ----
    os.makedirs(DIR_UF, exist_ok=True)
    tamanhos = {}
    total_uf_cat = 0
    for sig, series in sorted(uf_series.items()):
        obj = {"schemaVersion": "1.1", "uf": sig,
               "fonte": "CAT/INSS (dados abertos, CC-BY)",
               "cobertura": AVISOS_COBERTURA, "series": series}
        caminho = os.path.join(DIR_UF, f"{sig}.json")
        tamanhos[f"cat_uf/{sig}.json"] = gravar_minificado(caminho, obj)
        total_uf_cat += sum(l["cat"] for linhas in series.values() for l in linhas)

    registros = [{"municipioEmpregador": k[0], "ufEmpregador": k[1], "mes": k[2],
                  "totalCat": v[0], "totalObitos": v[1]}
                 for k, v in sorted(nacional_idx.items(), key=lambda x: (x[0][2], x[0][1], x[0][0]))]
    total_nacional_cat = sum(r["totalCat"] for r in registros)
    obj_nacional = {"schemaVersion": "2.0",
                    "fonte": "CAT/INSS (dados abertos, CC-BY)",
                    "cobertura": AVISOS_COBERTURA,
                    "dataProcessamento": datetime.now(timezone.utc).isoformat(),
                    "registros": registros}
    tamanhos["cat_nacional.json"] = gravar_minificado(ARQ_NACIONAL, obj_nacional)

    if os.path.exists(ARQ_OBSOLETO):
        os.remove(ARQ_OBSOLETO)
        log("Removido arquivo obsoleto docs/cat_agregado.json (substituído pelos dois níveis).")

    meta.update({
        "schemaVersion": "2.0 (nacional) + 1.1 (detalhe por UF)",
        "dataProcessamento": datetime.now(timezone.utc).isoformat(),
        "fonte": "INSS — Comunicações de Acidente de Trabalho (dados abertos)",
        "licenca": "CC-BY", "url": CKAN_PACKAGE_SHOW,
        "metodoMesPorCompetencia": metodos_mes,
        "avisosCobertura": AVISOS_COBERTURA,
        "ufsGeradas": sorted(uf_series.keys()),
        "tamanhoArquivosBytes": tamanhos,
        "sanidade": {"totalCatNacional": total_nacional_cat, "somaTotalCatUFs": total_uf_cat,
                     "iguais": total_nacional_cat == total_uf_cat},
    })
    with open(ARQ_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # ---- relatório de tamanhos no log ----
    log("=== TAMANHOS DOS ARQUIVOS GERADOS ===")
    for nome, b in sorted(tamanhos.items()):
        log(f"  {nome}: {b/1048576:.2f} MB")
    log(f"UFs geradas ({len(uf_series)}): {', '.join(sorted(uf_series.keys()))}")
    log(f"Sanidade: total nacional CAT = {total_nacional_cat} | soma das UFs = {total_uf_cat} | "
        f"{'OK' if total_nacional_cat == total_uf_cat else 'DIVERGÊNCIA!'}")


if __name__ == "__main__":
    if os.environ.get("INSPECAO", "0") == "1":
        modo_inspecao()
    else:
        modo_normal()
