# -*- coding: utf-8 -*-
"""
confere_cat.py — Conferência de sanidade dos agregados da CAT.
Leitura pura: soma docs/cat_agregado.json e imprime totais anuais e mensais,
para comparação com os totais oficiais (AEPS/AEAT — acidentes COM CAT
registrada). Não baixa nada, não grava nada, não altera nada.

Notas de interpretação:
- O dataset do PDA inicia em jun/2023: o ano de 2023 é estruturalmente
  incompleto (jan–mai/2023 só via CATs emitidas com atraso). O ano limpo
  para comparação é 2024 em diante.
- Meses recentes crescem retroativamente (CATs atrasadas): totais de meses
  próximos ao fim da série são parciais por natureza.
- Referência oficial 2023 (AEPS): 651.476 acidentes com CAT registrada.
"""

import json
import os
import sys
from collections import defaultdict

DOCS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
ARQ = os.path.join(DOCS, "cat_agregado.json")

if not os.path.exists(ARQ):
    sys.exit("ERRO: docs/cat_agregado.json não existe. Rodar o cat-sync em modo normal antes.")

with open(ARQ, "r", encoding="utf-8") as f:
    dados = json.load(f)

series = dados.get("series", {})
por_ano = defaultdict(lambda: {"cat": 0, "obitos": 0})
print("=== CONFERÊNCIA CAT — totais por mês ===")
print(f"{'mês':<10}{'CATs':>10}{'óbitos':>10}{'linhas':>10}{'chaves repetidas':>18}")
for mes in sorted(series):
    linhas = series[mes]
    total = sum(l.get("cat", 0) for l in linhas)
    obitos = sum(l.get("obitos", 0) for l in linhas)
    chaves = defaultdict(int)
    for l in linhas:
        chaves[(l.get("municipioEmpregador"), l.get("ufEmpregador"), l.get("secaoCNAE"))] += 1
    repetidas = sum(1 for v in chaves.values() if v > 1)
    ano = str(mes)[:4]
    por_ano[ano]["cat"] += total
    por_ano[ano]["obitos"] += obitos
    print(f"{mes:<10}{total:>10}{obitos:>10}{len(linhas):>10}{repetidas:>18}")

print()
print("=== CONFERÊNCIA CAT — totais por ano ===")
print(f"{'ano':<8}{'CATs':>12}{'óbitos':>10}")
for ano in sorted(por_ano):
    print(f"{ano:<8}{por_ano[ano]['cat']:>12}{por_ano[ano]['obitos']:>10}")

print()
print("Referência oficial (AEPS 2023): 651.476 acidentes COM CAT registrada no ano de 2023.")
print("ATENÇÃO: 2023 do pipeline é incompleto (dataset inicia em jun/2023); comparar 2024.")
print("'Chaves repetidas' > 0 é ESPERADO: mesma chave município×UF×seção vinda de arquivos")
print("mensais diferentes (CATs atrasadas). O app deve SOMAR linhas de mesma chave.")
