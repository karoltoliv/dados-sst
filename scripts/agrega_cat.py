# -*- coding: utf-8 -*-
"""
agrega_cat.py (v4) — Pipeline CAT/INSS: dois níveis + recortes temáticos.
Bloco C4 (17/08/2026): recortes por dimensão, com supressão de células pequenas.

SAÍDA (acréscimos da v4):
- docs/cat_recortes/tipo-acidente.json   (coluna fonte: "Tipo do Acidente")
- docs/cat_recortes/agente-causador.json (coluna fonte: "Agente  Causador  Acidente")
  Formato compacto; granularidade município × UF × ano × categoria; SUPRESSÃO:
  por município×ano, categorias com contagem < 3 são agrupadas em
  "Outros/Não especificado" (valores vazios/"{ñ class}" caem sempre nesse grupo).
- interno/cat_recortes_bruto.json — contagens PRÉ-supressão, necessárias ao
  incremento mensal correto (uma categoria pode cruzar o limiar com dados
  novos). Fica FORA de docs/ (não publicado no site). Justificativa: o
  conteúdo não revela nada além dos CSVs públicos do INSS; a supressão
  protege o produto servido ao app (minimização), não um segredo.
- cat_index.json ganha o bloco "recortes" (dimensão -> {arquivo, bytes}).

Mantido da v3: cat_nacional/[AAAA].json (compacto), cat_uf/[NOME].json,
índice viewconf, avisos de cobertura, sanidade, travas de verificação.
MODO INSPEÇÃO (INSPECAO=1): agora também imprime a distribuição REAL de
valores das duas colunas de recorte (arquivo mais recente + uma amostra
grande), para verificação de rótulos antes de qualquer interpretação no app.
MIGRAÇÃO: se interno/cat_recortes_bruto.json não existir, refaz a série
completa (~10–15 min), reconstruindo todos os níveis de uma vez.
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

# Dimensões de recorte (Bloco C4). Nomes de coluna EXATOS da inspeção de
# 16/08/2026 — atenção: "Agente  Causador  Acidente" tem espaços duplos.
RECORTES = {
    "tipo-acidente": "Tipo do Acidente",
    "agente-causador": "Agente  Causador  Acidente",
}
CATEGORIA_OUTROS = "Outros/Não especificado"
LIMIAR_SUPRESSAO = 3  # contagens < 3 por município×ano são agrupadas
CAMPOS_RECORTE = ["municipioEmpregador", "ufEmpregador", "ano", "categoria", "totalCat", "totalObitos"]
COLUNA_EMISSAO = "Data Emissão CAT"  # ausente nos arquivos de 2023 (verificado 17/08/2026)

CAMPOS_NACIONAL = ["municipioEmpregador", "ufEmpregador", "mes", "totalCat", "totalObitos"]

AVISOS_COBERTURA = [
    "Série inicia em jun/2023: o ano de 2023 é estruturalmente incompleto.",
    "Subcobertura da fonte identificada: set-dez/2024 e nov-dez/2025 (filtro de ano na extração do PDA).",
    "Meses recentes são parciais por natureza (série administrativa com defasagem irregular).",
]

SIGLAS = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARA": "CE", "DISTRITOFEDERAL": "DF", "ESPIRITOSANTO": "ES", "GOIAS": "GO",
    "MARANHAO": "MA", "MATOGROSSO": "MT", "MATOGROSSODOSUL": "MS", "MINASGERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
    "RIODEJANEIRO": "RJ", "RIOGRANDEDONORTE": "RN", "RIOGRANDEDOSUL": "RS",
    "RONDONIA": "RO", "RORAIMA": "RR", "SANTACATARINA": "SC", "SAOPAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO",
}

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(RAIZ, "docs")
DIR_UF = os.path.join(DOCS, "cat_uf")
DIR_NACIONAL = os.path.join(DOCS, "cat_nacional")
DIR_RECORTES = os.path.join(DOCS, "cat_recortes")
DIR_INTERNO = os.path.join(RAIZ, "interno")
ARQ_BRUTO = os.path.join(DIR_INTERNO, "cat_recortes_bruto.json")
ARQ_META = os.path.join(DOCS, "cat_meta.json")
ARQ_INDEX = os.path.join(DOCS, "cat_index.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_cat.json")
ARQ_LEGADO_V1 = os.path.join(DOCS, "cat_agregado.json")
ARQ_LEGADO_V2 = os.path.join(DOCS, "cat_nacional.json")


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


def categoria_limpa(valor: str) -> str:
    """Categoria verbatim da fonte; vazios e não classificados caem em Outros."""
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none") or s == "{ñ class}":
        return CATEGORIA_OUTROS
    return s


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
    alvos = [zips[-1]]
    if len(zips) > 10:
        alvos.append(zips[-10])  # amostra maior, meses atrás
    achado = {"dataInspecao": datetime.now(timezone.utc).isoformat(), "amostras": {}}
    for rec in alvos:
        df, nome_csv, enc, sep = baixar_csv(rec["url"])
        amostra = {
            "arquivoCSV": nome_csv, "linhas": int(len(df)),
            "encoding": enc, "separador": sep,
        }
        for dim, col in RECORTES.items():
            if col in df.columns:
                amostra[f"dist[{dim}]"] = (df[col].astype(str).str.strip()
                                           .value_counts(dropna=False).head(40).to_dict())
            else:
                amostra[f"dist[{dim}]"] = f"COLUNA '{col}' INEXISTENTE — colunas: {list(df.columns)}"
        achado["amostras"][chave(rec)] = amostra
    os.makedirs(DOCS, exist_ok=True)
    with open(ARQ_INSPECAO, "w", encoding="utf-8") as f:
        json.dump(achado, f, ensure_ascii=False, indent=2)
    log("=== RESULTADO DA INSPEÇÃO (recortes C4) ===")
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
            return idx, "partições por ano (v3+)"
    if os.path.exists(ARQ_LEGADO_V2):
        dado = carregar_json(ARQ_LEGADO_V2, {})
        for r in dado.get("registros", []):
            idx[(r["municipioEmpregador"], r["ufEmpregador"], r["mes"])] = \
                [int(r["totalCat"]), int(r["totalObitos"])]
        if idx:
            return idx, "arquivo único legado (v2)"
    return idx, "vazio"


def modo_normal():
    if not COLUNAS_CONFIRMADAS:
        sys.exit("PARADO POR SEGURANÇA: colunas não confirmadas.")
    zips, chave = listar_recursos()
    meta = carregar_json(ARQ_META, {})

    # Bruto dos recortes: obrigatório para incremento correto da supressão.
    bruto = carregar_json(ARQ_BRUTO, None)
    oportunidade = meta.get("oportunidadeRegistro", {})
    if bruto is None:
        log("MIGRAÇÃO C4: interno/cat_recortes_bruto.json não existe — "
            "refazendo a série completa (todos os meses) para construir os recortes.")
        meta["ultimoMesProcessado"] = None
        oportunidade = {}
        nacional_idx = {}
        uf_series = defaultdict(lambda: defaultdict(list))
        recortes_raw = {dim: {} for dim in RECORTES}
    else:
        nacional_idx, origem_nac = carregar_nacional_existente()
        log(f"Nacional carregado de: {origem_nac} ({len(nacional_idx)} chaves).")
        uf_series = defaultdict(lambda: defaultdict(list))
        if os.path.isdir(DIR_UF):
            for nome in os.listdir(DIR_UF):
                if nome.endswith(".json"):
                    dado = carregar_json(os.path.join(DIR_UF, nome), {})
                    for mes, linhas in dado.get("series", {}).items():
                        uf_series[nome[:-5]][mes] = linhas
        recortes_raw = {dim: {} for dim in RECORTES}
        for dim, mapa in bruto.items():
            if dim in recortes_raw:
                for k, v in mapa.items():
                    recortes_raw[dim][k] = [int(v), 0] if isinstance(v, int) else [int(v[0]), int(v[1])]
        log(f"Bruto de recortes carregado: "
            + ", ".join(f"{d}={len(m)} células" for d, m in recortes_raw.items()))

    pendentes = [r for r in zips
                 if meta.get("ultimoMesProcessado") is None or chave(r) > meta["ultimoMesProcessado"]]
    indice_atual = carregar_json(ARQ_INDEX, {})
    if not pendentes:
        if os.path.isdir(DIR_RECORTES) and "recortes" in indice_atual:
            log("Nenhum recurso novo e estrutura C4 já existe. Nada a fazer.")
            return
        log("Nenhum recurso novo, mas a estrutura C4 ainda não existe — regravando saídas.")
    else:
        log(f"{len(pendentes)} arquivo(s) mensal(is) a processar.")

    metodos_mes = meta.get("metodoMesPorCompetencia", {})
    c = MAPA_COLUNAS
    SEP = "\x1f"  # separador interno das chaves do bruto (não aparece nos dados)
    for rec in pendentes:
        competencia = chave(rec)
        df, _, _, _ = baixar_csv(rec["url"])
        obrig = [c["municipio_empregador"], c["uf_empregador"],
                 c["indicador_obito"], c["data_acidente"]] + list(RECORTES.values())
        faltando = [x for x in obrig if x not in df.columns]
        if faltando:
            sys.exit(f"ERRO {competencia}: colunas ausentes {faltando}. "
                     f"Cabeçalho real: {list(df.columns)}.")
        col_cnae = escolher_coluna_cnae(df)
        df["_secao"] = df[col_cnae].map(secao_cnae)
        df["_mes"], metodo = derivar_mes(df[c["data_acidente"]], competencia)
        metodos_mes[competencia] = metodo
        df["_ano"] = df["_mes"].str[:4]
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

        # recortes temáticos (C4 + óbitos, cf. Guia VISAT Tab. 11):
        # município × UF × ano × categoria -> [CATs, óbitos]
        for dim, col in RECORTES.items():
            df["_cat_dim"] = df[col].map(categoria_limpa)
            g = df.groupby([c["municipio_empregador"], c["uf_empregador"], "_ano", "_cat_dim"],
                           dropna=False).agg(n=("_obito", "size"),
                                             ob=("_obito", "sum")).reset_index()
            for _, l in g.iterrows():
                k = SEP.join([str(l[c["municipio_empregador"]]), str(l[c["uf_empregador"]]),
                              str(l["_ano"]), str(l["_cat_dim"])])
                atual_r = recortes_raw[dim].setdefault(k, [0, 0])
                atual_r[0] += int(l["n"])
                atual_r[1] += int(l["ob"])

        # oportunidade do registro: defasagem em meses entre acidente e emissão
        if COLUNA_EMISSAO in df.columns:
            em = df[COLUNA_EMISSAO].str.extract(r"^(\d{2})/(\d{2})/(\d{4})")
            idx_em = (em[2].astype(float) * 12 + em[1].astype(float))
            ac = df["_mes"].str.extract(r"^(\d{4})-(\d{2})")
            idx_ac = (ac[0].astype(float) * 12 + ac[1].astype(float))
            defas = (idx_em - idx_ac)
            buckets = defas.map(lambda d: "invalida" if pd.isna(d) or d < 0
                                else ("3+" if d >= 3 else str(int(d))))
            oportunidade[competencia] = buckets.value_counts().to_dict()
        else:
            oportunidade[competencia] = "SEM COLUNA (arquivos de 2023 não trazem Data Emissão CAT)"
        meta["ultimoMesProcessado"] = competencia
        log(f"Competência {competencia}: {len(grupo)} linhas de detalhe processadas ({metodo}).")

    # ---- detalhe por UF (inalterado) ----
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

    # ---- nacional por ano, compacto (inalterado da v3) ----
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

    # ---- recortes temáticos com supressão (C4) ----
    os.makedirs(DIR_RECORTES, exist_ok=True)
    os.makedirs(DIR_INTERNO, exist_ok=True)
    recortes_idx = {}
    stats_supressao = {}
    totais_recorte = {}
    for dim in RECORTES:
        por_celula = defaultdict(dict)
        for k, par in recortes_raw[dim].items():
            mun, uf, ano, cat = k.split(SEP)
            atual = por_celula[(mun, uf, ano)].setdefault(cat, [0, 0])
            atual[0] += par[0]
            atual[1] += par[1]
        registros = []
        celulas_agrupadas = 0
        total_dim = 0
        for (mun, uf, ano), cats in por_celula.items():
            outros = cats.pop(CATEGORIA_OUTROS, [0, 0])
            for cat, par in cats.items():
                if par[0] < LIMIAR_SUPRESSAO:
                    outros[0] += par[0]
                    outros[1] += par[1]
                    celulas_agrupadas += 1
                else:
                    registros.append([mun, uf, ano, cat, par[0], par[1]])
                    total_dim += par[0]
            if outros[0] > 0:
                registros.append([mun, uf, ano, CATEGORIA_OUTROS, outros[0], outros[1]])
                total_dim += outros[0]
        registros.sort(key=lambda r: (r[2], r[1], r[0], r[3]))
        obj = {"dimensao": dim, "colunaFonte": RECORTES[dim], "schemaVersion": "1.0",
               "fonte": "CAT/INSS (dados abertos, CC-BY)", "cobertura": AVISOS_COBERTURA,
               "supressao": f"por município×ano, categorias com contagem < {LIMIAR_SUPRESSAO} "
                            f"agrupadas em '{CATEGORIA_OUTROS}' (inclui vazios/não classificados)",
               "dataProcessamento": agora, "campos": CAMPOS_RECORTE, "registros": registros}
        caminho_rel = f"cat_recortes/{dim}.json"
        b = gravar_minificado(os.path.join(DOCS, caminho_rel), obj)
        tamanhos[caminho_rel] = b
        recortes_idx[dim] = {"arquivo": caminho_rel, "bytes": b}
        stats_supressao[dim] = celulas_agrupadas
        totais_recorte[dim] = total_dim

    # bruto interno (fora de docs/) para incrementos futuros
    gravar_minificado(ARQ_BRUTO, {dim: dict(m) for dim, m in recortes_raw.items()})

    for legado in (ARQ_LEGADO_V1, ARQ_LEGADO_V2):
        if os.path.exists(legado):
            os.remove(legado)
            log(f"Removido arquivo legado {os.path.basename(legado)}.")

    # ---- índice (viewconf) ----
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
        "schemaVersion": "1.2",
        "dataAtualizacao": agora,
        "fonte": "CAT/INSS (dados abertos, CC-BY)",
        "cobertura": AVISOS_COBERTURA,
        "nacionalPorAno": nacional_por_ano_idx,
        "camposNacional": CAMPOS_NACIONAL,
        "recortes": recortes_idx,
        "camposRecortes": CAMPOS_RECORTE,
        "supressaoRecortes": f"contagens < {LIMIAR_SUPRESSAO} por município×ano agrupadas em "
                             f"'{CATEGORIA_OUTROS}'",
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
        "schemaVersion": "2.1 (nacional) + 1.1 (UF) + recortes 1.0 (C4)",
        "dataProcessamento": agora,
        "fonte": "INSS — Comunicações de Acidente de Trabalho (dados abertos)",
        "licenca": "CC-BY", "url": CKAN_PACKAGE_SHOW,
        "metodoMesPorCompetencia": metodos_mes,
        "oportunidadeRegistro": oportunidade,
        "oportunidadeRegistroNota": "distribuição da defasagem em meses entre a data do acidente "
                                    "e a emissão da CAT, por lote (0 = mesmo mês; '3+' = três ou "
                                    "mais; 'invalida' = data ilegível/negativa). Indicador de "
                                    "oportunidade do registro (cf. Guia VISAT). Arquivos de 2023 "
                                    "não trazem a coluna de emissão.",
        "avisosCobertura": AVISOS_COBERTURA,
        "ufsGeradas": sorted(uf_series.keys()),
        "anosNacional": sorted(por_ano.keys()),
        "recortes": {"dimensoes": {d: RECORTES[d] for d in RECORTES},
                     "limiarSupressao": LIMIAR_SUPRESSAO,
                     "celulasAgrupadas": stats_supressao,
                     "notaBrutoInterno": "contagens pré-supressão em interno/ (fora do site) "
                                         "para incremento mensal correto; conteúdo não excede "
                                         "o que os CSVs públicos do INSS já revelam"},
        "tamanhoArquivosBytes": tamanhos,
        "sanidade": {"totalCatNacional": total_nacional_cat, "somaTotalCatUFs": total_uf_cat,
                     "totalPorRecorte": totais_recorte,
                     "iguais": (total_nacional_cat == total_uf_cat ==
                                totais_recorte.get("tipo-acidente") ==
                                totais_recorte.get("agente-causador"))},
    })
    with open(ARQ_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    log("=== TAMANHOS — RECORTES (C4) ===")
    for dim in sorted(recortes_idx):
        log(f"  cat_recortes/{dim}.json: {recortes_idx[dim]['bytes']/1048576:.2f} MB "
            f"| células agrupadas na supressão: {stats_supressao[dim]}")
    log("=== TAMANHOS — NACIONAL POR ANO ===")
    for ano in sorted(nacional_por_ano_idx):
        log(f"  cat_nacional/{ano}.json: {nacional_por_ano_idx[ano]['bytes']/1048576:.2f} MB")
    log("=== TAMANHOS — DEMAIS ===")
    for nome, b in sorted(tamanhos.items()):
        if not nome.startswith(("cat_nacional/", "cat_recortes/")):
            log(f"  {nome}: {b/1048576:.2f} MB")
    log(f"UFs geradas ({len(uf_series)}): {', '.join(sorted(uf_series.keys()))}")
    ok = meta["sanidade"]["iguais"]
    log(f"Sanidade: nacional={total_nacional_cat} | UFs={total_uf_cat} | "
        f"tipo-acidente={totais_recorte.get('tipo-acidente')} | "
        f"agente-causador={totais_recorte.get('agente-causador')} | "
        f"{'OK — tudo igual' if ok else 'DIVERGÊNCIA!'}")


if __name__ == "__main__":
    if os.environ.get("INSPECAO", "0") == "1":
        modo_inspecao()
    else:
        modo_normal()
