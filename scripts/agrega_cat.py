# -*- coding: utf-8 -*-
"""
agrega_cat.py — Pipeline CAT/INSS (dados abertos, licença CC-BY)
App: Saúde, Trabalho & Território (TCC Fiocruz)

VERSÃO DESTRAVADA em 16/08/2026, após inspeção real do arquivo
D.SDA.PDA.005.CAT.202605.csv (encoding latin-1, separador ';',
valores com espaços à direita — largura fixa).

REGRAS INVIOLÁVEIS (especificação de 15/08/2026):
- Saída contém APENAS agregados: contagem de CAT e de óbitos por
  município do empregador (com UF) × seção CNAE × mês. Nenhum microdado.
- CAT e SINAN nunca se somam: este arquivo é série própria, com rótulo de fonte.
- Nenhum dado fictício: onde algo não for reconhecido, o script para
  com mensagem clara ou registra a limitação nos metadados — nunca estima.

VERIFICAÇÕES EM TEMPO DE EXECUÇÃO (ambiguidades reveladas pela inspeção):
- Há DUAS colunas 'CNAE2.0 Empregador' (cabeçalho repetido na fonte);
  o script testa o conteúdo e usa a que contém códigos numéricos.
- O formato de 'Data Acidente' é detectado (dd/mm/aaaa ou aaaa-mm-dd);
  se irreconhecível, usa-se a competência do arquivo e isso fica
  registrado em cat_meta.json.
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
# CONFIGURAÇÃO — confirmada pela inspeção de 16/08/2026
# ----------------------------------------------------------------------

CKAN_PACKAGE_SHOW = (
    "https://dadosabertos.inss.gov.br/api/3/action/package_show"
    "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025"
)

COLUNAS_CONFIRMADAS = True  # inspeção de 16/08/2026 (docs/inspecao_cat.json)

MAPA_COLUNAS = {
    "municipio_empregador": "Munic Empr",
    "uf_empregador": "UF Munic. Empregador",
    "cnae_codigo": "CNAE2.0 Empregador",            # candidata 1 (testada em execução)
    "cnae_codigo_alt": "CNAE2.0 Empregador.1",      # candidata 2 (testada em execução)
    "indicador_obito": "Indica Óbito Acidente",
    "data_acidente": "Data Acidente",
}

VALOR_OBITO = "Sim"  # confirmado: valores 'Sim'/'Não'/'{ñ class}'; só 'Sim' conta óbito

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
ARQ_AGREGADO = os.path.join(DOCS, "cat_agregado.json")
ARQ_META = os.path.join(DOCS, "cat_meta.json")
ARQ_INSPECAO = os.path.join(DOCS, "inspecao_cat.json")

# CNAE 2.0: divisão (2 dígitos) -> seção (classificação oficial IBGE)
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
    amostra = "\n".join(texto.splitlines()[:5])
    try:
        sep = csv.Sniffer().sniff(amostra, delimiters=";,|\t").delimiter
    except csv.Error:
        sep = ";"
    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str)
    # Largura fixa: limpar espaços em todas as células e nos nomes de colunas
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    return df, nomes_csv[0], encoding_usado, sep


def escolher_coluna_cnae(df):
    """Duas colunas homônimas na fonte; usa a que contém códigos numéricos.
    Verificação em tempo de execução — não presumimos qual é qual."""
    for col in (MAPA_COLUNAS["cnae_codigo"], MAPA_COLUNAS["cnae_codigo_alt"]):
        if col in df.columns:
            amostra = (df[col].dropna().astype(str).str.strip()
                       .str.replace(".", "", regex=False)
                       .str.replace("-", "", regex=False).head(200))
            if len(amostra) and amostra.str.match(r"^\d{2,}").mean() > 0.8:
                return col
    sys.exit("ERRO: nenhuma das colunas CNAE contém códigos numéricos em maioria. "
             "Inspecionar os valores reais antes de prosseguir.")


def derivar_mes(serie: pd.Series, competencia: str):
    """Detecta o formato da data em execução. Retorna (série AAAA-MM, método)."""
    s = serie.astype(str).str.strip()
    m = s.str.extract(r"^(\d{2})/(\d{2})/(\d{4})")
    if m[0].notna().mean() > 0.8:
        return (m[2] + "-" + m[1]), "data_acidente dd/mm/aaaa"
    m2 = s.str.extract(r"^(\d{4})-(\d{2})")
    if m2[0].notna().mean() > 0.8:
        return (m2[0] + "-" + m2[1]), "data_acidente aaaa-mm"
    # Fallback declarado: competência do arquivo (registrado nos metadados)
    return pd.Series(f"{competencia[:4]}-{competencia[4:]}", index=serie.index), \
        "FALLBACK: competência do arquivo (formato de data não reconhecido)"


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
    }
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


def carregar_json(caminho, padrao):
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    return padrao


def modo_normal():
    if not COLUNAS_CONFIRMADAS:
        sys.exit("PARADO POR SEGURANÇA: colunas não confirmadas.")
    zips, chave = listar_recursos()
    meta = carregar_json(ARQ_META, {"ultimoMesProcessado": None})
    agregado = carregar_json(ARQ_AGREGADO, {
        "schemaVersion": "1.1",
        "fonte": "CAT/INSS (dados abertos, CC-BY)",
        "series": {},
    })
    metodos_mes = {}

    pendentes = [r for r in zips if meta.get("ultimoMesProcessado") is None or chave(r) > meta["ultimoMesProcessado"]]
    if not pendentes:
        log("Nenhum recurso novo. Nada a fazer.")
        return

    log(f"{len(pendentes)} arquivo(s) mensal(is) a processar. "
        "No primeiro run isso cobre a série histórica completa — pode demorar.")

    for rec in pendentes:
        competencia = chave(rec)
        df, _, _, _ = baixar_csv(rec["url"])
        obrigatorias = [MAPA_COLUNAS["municipio_empregador"], MAPA_COLUNAS["uf_empregador"],
                        MAPA_COLUNAS["indicador_obito"], MAPA_COLUNAS["data_acidente"]]
        faltando = [c for c in obrigatorias if c not in df.columns]
        if faltando:
            sys.exit(f"ERRO na competência {competencia}: colunas ausentes {faltando}. "
                     f"Cabeçalho real: {list(df.columns)}. Reexecutar inspeção.")
        col_cnae = escolher_coluna_cnae(df)
        c = MAPA_COLUNAS
        df["_secao"] = df[col_cnae].map(secao_cnae)
        df["_mes"], metodo = derivar_mes(df[c["data_acidente"]], competencia)
        metodos_mes[competencia] = metodo
        df["_obito"] = (df[c["indicador_obito"]].str.strip().str.casefold()
                        == VALOR_OBITO.casefold()).astype(int)

        grupo = df.groupby(["_mes", c["municipio_empregador"], c["uf_empregador"], "_secao"],
                           dropna=False).agg(cat=("_obito", "size"),
                                             obitos=("_obito", "sum")).reset_index()

        for mes, sub in grupo.groupby("_mes"):
            linhas = agregado["series"].setdefault(str(mes), [])
            linhas.extend([
                {
                    "municipioEmpregador": str(l[c["municipio_empregador"]]),
                    "ufEmpregador": str(l[c["uf_empregador"]]),
                    "secaoCNAE": l["_secao"],
                    "cat": int(l["cat"]),
                    "obitos": int(l["obitos"]),
                    "fonte": "CAT/INSS",
                }
                for _, l in sub.iterrows()
            ])
        meta["ultimoMesProcessado"] = competencia
        log(f"Competência {competencia}: {len(grupo)} linhas agregadas "
            f"(mês × município × UF × seção CNAE); método do mês: {metodo}.")

    meta.update({
        "dataProcessamento": datetime.now(timezone.utc).isoformat(),
        "fonte": "INSS — Comunicações de Acidente de Trabalho (dados abertos)",
        "licenca": "CC-BY",
        "url": CKAN_PACKAGE_SHOW,
        "colunaCNAEUtilizada": "detectada por conteúdo em cada arquivo",
        "metodoMesPorCompetencia": metodos_mes,
        "observacaoMunicipio": "valor textual conforme a fonte (coluna 'Munic Empr'), "
                               "acompanhado de 'UF Munic. Empregador'",
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
