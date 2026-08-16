# -*- coding: utf-8 -*-
"""
agrega_cat.py (v3) — Pipeline CAT/INSS: dois níveis + nacional particionado.
Bloco C3 (17/08/2026): o nível nacional passa a ser particionado POR ANO em
formato compacto (campos declarados uma vez; registros como arrays).

SAÍDA:
- docs/cat_nacional/[AAAA].json — um por ano presente nos dados; formato:
  {"ano": 2024, "campos": ["municipioEmpregador","ufEmpregador","mes",
   "totalCat","totalObitos"], "registros": [["Palmas","TO","2024-01",12,0], ...]}
- docs/cat_uf/[NOME].json — 27 UFs + ZERADO (INALTERADO: schema 1.1).
- docs/cat_index.json — "nacionalPorAno": mapa ano -> {arquivo, bytes};
  o app descobre os arquivos pelo índice, sem lista fixa no código.
- docs/cat_meta.json — tamanhos em bytes de tudo.

Nota: a fonte não possui 2022 (PDA inicia em jun/2023); os anos são
derivados dos dados, dinamicamente.

MIGRAÇÃO: carrega o nacional de cat_nacional/[AAAA].json (novo formato),
ou do legado cat_nacional.json (v2), ou reconstrói do zero se nada existir.
Arquivos legados (cat_agregado.json, cat_nacional.json) são removidos.
Regras preservadas: mês = data do acidente; somar linhas de mesma chave;
avisos de cobertura; sanidade nacional×UFs; nenhum dado fictício.
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

CAMPOS_NACIONAL = ["municipioEmpregador", "ufEmpregador", "mes", "totalCat", "totalObitos"]

AVISOS_COBERTURA = [
    "Série inicia em jun/2023: o ano de 2023 é estruturalmente incompleto.",
    "Subcobertura da fonte identificada: set-dez/2024 e nov-dez/2025 (filtro de ano na extração do PDA).",
    "Meses recentes são parciais por natureza (série administrativa com defasagem irregular).",
]

# Mapa oficial nome-normalizado -> sigla (IBGE), para o índice publicado.
SIGLAS = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARA": "CE", "DISTRITOFEDERAL": "DF", "ESPIRITOSANTO": "ES", "GOIAS": "GO",
    "MARANHAO": "MA", "MATOGROSSO": "MT", "MATOGROSSODOSUL": "MS", "MINASGERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
    "RIODEJANEIRO": "RJ", "RIOGRANDEDONORTE": "RN", "RIOGRANDEDOSUL": "RS",
    "RONDONIA": "RO", "RORAIMA": "RR", "SANTACATARINA": "SC", "SAOPAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO",
}

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
DIR_UF = os.path.join(DOCS, "cat_uf")
DIR_NACIONAL = os.path.join(DOCS, "cat_nacional")
ARQ_META = os.path.join(DOCS, "cat_meta.json")
ARQ_INDEX = os.path.join(DOCS, "cat_index.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_cat.json")
ARQ_LEGADO_V1 = os.path.join(DOCS, "cat_agregado.json")   # 50 MB original
ARQ_LEGADO_V2 = os.path.join(DOCS, "cat_nacional.json")   # nacional único da v2


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


def carregar_nacional_existente():
    """Carrega o índice nacional em memória a partir do formato mais novo
    disponível: partições por ano (v3) > arquivo único (v2) > vazio."""
    idx = {}
    if os.path.isdir(DIR_NACIONAL):
        for nome in sorted(os.listdir(DIR_NACIONAL)):
            if not nome.endswith(".json"):
                continue
            dado = carregar_json(os.path.join(DIR_NACIONAL, nome), {})
            campos = dado.get("campos", CAMPOS_NACIONAL)
            for reg in dado.get("registros", []):
                d = dict(zip(campos, reg))
                idx[(d["municipioEmpregador"], d["ufEmpregador"], d["mes"])] = \
                    [int(d["totalCat"]), int(d["totalObitos"])]
        if idx:
            return idx, "partições por ano (v3)"
    if os.path.exists(ARQ_LEGADO_V2):
        dado = carregar_json(ARQ_LEGADO_V2, {})
        for r in dado.get("registros", []):
            idx[(r["municipioEmpregador"], r["ufEmpregador"], r["mes"])] = \
                [int(r["totalCat"]), int(r["totalObitos"])]
        if idx:
            return idx, "arquivo único legado (v2)"
    return idx, "vazio (reconstrução completa)"


def modo_normal():
    if not COLUNAS_CONFIRMADAS:
        sys.exit("PARADO POR SEGURANÇA: colunas não confirmadas.")
    zips, chave = listar_recursos()
    meta = carregar_json(ARQ_META, {})

    nacional_idx, origem_nac = carregar_nacional_existente()
    reconstrucao = not nacional_idx
    if reconstrucao:
        log("MIGRAÇÃO: nenhum nacional existente — refazendo a série completa.")
        meta["ultimoMesProcessado"] = None
        uf_series = defaultdict(lambda: defaultdict(list))
    else:
        log(f"Nacional carregado de: {origem_nac} ({len(nacional_idx)} chaves).")
        uf_series = defaultdict(lambda: defaultdict(list))
        if os.path.isdir(DIR_UF):
            for nome in os.listdir(DIR_UF):
                if nome.endswith(".json"):
                    dado = carregar_json(os.path.join(DIR_UF, nome), {})
                    for mes, linhas in dado.get("series", {}).items():
                        uf_series[nome[:-5]][mes] = linhas

    pendentes = [r for r in zips
                 if meta.get("ultimoMesProcessado") is None or chave(r) > meta["ultimoMesProcessado"]]
    indice_atual = carregar_json(ARQ_INDEX, {})
    if not pendentes:
        if os.path.isdir(DIR_NACIONAL) and "nacionalPorAno" in indice_atual:
            log("Nenhum recurso novo e estrutura C3 já existe. Nada a fazer.")
            return
        log("Nenhum recurso novo, mas a estrutura C3 (partições por ano + índice) "
            "ainda não existe — regravando saídas a partir dos dados carregados.")
    else:
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
            atual = nacional_idx.setdefault((mun, uf_bruta, mes), [0, 0])
            atual[0] += int(l["cat"])
            atual[1] += int(l["obitos"])
        meta["ultimoMesProcessado"] = competencia
        log(f"Competência {competencia}: {len(grupo)} linhas de detalhe processadas ({metodo}).")

    # ---- nível de detalhe por UF (INALTERADO) ----
    os.makedirs(DIR_UF, exist_ok=True)
    tamanhos = {}
    total_uf_cat = 0
    for sig, series in sorted(uf_series.items()):
        obj = {"schemaVersion": "1.1", "uf": sig,
               "fonte": "CAT/INSS (dados abertos, CC-BY)",
               "cobertura": AVISOS_COBERTURA, "series": series}
        tamanhos[f"cat_uf/{sig}.json"] = gravar_minificado(
            os.path.join(DIR_UF, f"{sig}.json"), obj)
        total_uf_cat += sum(l["cat"] for linhas in series.values() for l in linhas)

    # ---- nível nacional: partições por ano, formato compacto (C3) ----
    os.makedirs(DIR_NACIONAL, exist_ok=True)
    por_ano = defaultdict(list)
    for (mun, uf, mes), (tc, to) in sorted(nacional_idx.items(),
                                           key=lambda x: (x[0][2], x[0][1], x[0][0])):
        por_ano[mes[:4]].append([mun, uf, mes, tc, to])
    agora = datetime.now(timezone.utc).isoformat()
    total_nacional_cat = 0
    nacional_por_ano_idx = {}
    for ano in sorted(por_ano):
        obj = {"ano": int(ano), "schemaVersion": "2.1",
               "fonte": "CAT/INSS (dados abertos, CC-BY)",
               "cobertura": AVISOS_COBERTURA, "dataProcessamento": agora,
               "campos": CAMPOS_NACIONAL, "registros": por_ano[ano]}
        caminho_rel = f"cat_nacional/{ano}.json"
        b = gravar_minificado(os.path.join(DOCS, caminho_rel), obj)
        tamanhos[caminho_rel] = b
        nacional_por_ano_idx[ano] = {"arquivo": caminho_rel, "bytes": b}
        total_nacional_cat += sum(r[3] for r in por_ano[ano])

    # remover legados
    for legado in (ARQ_LEGADO_V1, ARQ_LEGADO_V2):
        if os.path.exists(legado):
            os.remove(legado)
            log(f"Removido arquivo legado {os.path.basename(legado)}.")

    # ---- índice publicado (viewconf): o app lê isto primeiro ----
    ufs_index, nao_mapeadas = {}, {}
    for sig_nome in sorted(uf_series.keys()):
        entrada = {"arquivo": f"cat_uf/{sig_nome}.json",
                   "bytes": tamanhos.get(f"cat_uf/{sig_nome}.json", 0)}
        if sig_nome in SIGLAS:
            ufs_index[SIGLAS[sig_nome]] = entrada
        elif sig_nome in ("ZERADO", "IGNORADO"):
            ufs_index["semUF"] = entrada
        else:
            nao_mapeadas[sig_nome] = entrada
    indice = {
        "schemaVersion": "1.1",
        "dataAtualizacao": agora,
        "fonte": "CAT/INSS (dados abertos, CC-BY)",
        "cobertura": AVISOS_COBERTURA,
        "nacionalPorAno": nacional_por_ano_idx,
        "camposNacional": CAMPOS_NACIONAL,
        "ufs": ufs_index,
        "ufsNaoMapeadas": nao_mapeadas,
        "meta": {"arquivo": "cat_meta.json"},
    }
    with open(ARQ_INDEX, "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)
    tamanhos["cat_index.json"] = os.path.getsize(ARQ_INDEX)
    if nao_mapeadas:
        log(f"ATENÇÃO: valores de UF fora do mapa oficial: {sorted(nao_mapeadas.keys())}")

    meta.update({
        "schemaVersion": "2.1 (nacional por ano, compacto) + 1.1 (detalhe por UF)",
        "dataProcessamento": agora,
        "fonte": "INSS — Comunicações de Acidente de Trabalho (dados abertos)",
        "licenca": "CC-BY", "url": CKAN_PACKAGE_SHOW,
        "metodoMesPorCompetencia": metodos_mes,
        "avisosCobertura": AVISOS_COBERTURA,
        "ufsGeradas": sorted(uf_series.keys()),
        "anosNacional": sorted(por_ano.keys()),
        "tamanhoArquivosBytes": tamanhos,
        "sanidade": {"totalCatNacional": total_nacional_cat, "somaTotalCatUFs": total_uf_cat,
                     "iguais": total_nacional_cat == total_uf_cat},
    })
    with open(ARQ_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log("=== TAMANHOS — NACIONAL POR ANO ===")
    total_nac_bytes = 0
    for ano in sorted(nacional_por_ano_idx):
        b = nacional_por_ano_idx[ano]["bytes"]
        total_nac_bytes += b
        log(f"  cat_nacional/{ano}.json: {b/1048576:.2f} MB")
    log(f"  TOTAL nacional (todos os anos): {total_nac_bytes/1048576:.2f} MB")
    log("=== TAMANHOS — DEMAIS ARQUIVOS ===")
    for nome, b in sorted(tamanhos.items()):
        if not nome.startswith("cat_nacional/"):
            log(f"  {nome}: {b/1048576:.2f} MB")
    log(f"UFs geradas ({len(uf_series)}): {', '.join(sorted(uf_series.keys()))}")
    log(f"Sanidade: total nacional CAT = {total_nacional_cat} | soma das UFs = {total_uf_cat} | "
        f"{'OK' if total_nacional_cat == total_uf_cat else 'DIVERGÊNCIA!'}")


if __name__ == "__main__":
    if os.environ.get("INSPECAO", "0") == "1":
        modo_inspecao()
    else:
        modo_normal()
