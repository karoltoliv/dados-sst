# Dados agregados — Saúde, Trabalho & Território

Pipeline de dados públicos agregados para o aplicativo **Saúde, Trabalho & Território** (artefato de TCC — Especialização em Transformação Digital em Saúde, EGF/Fiocruz Brasília).

## O que este repositório faz

Automação **sem servidor** (GitHub Actions) que baixa dados públicos oficiais, agrega e publica arquivos JSON estáticos via GitHub Pages. O aplicativo apenas lê esses JSONs — não há backend.

| Arquivo publicado | Fonte | Periodicidade | Licença da fonte |
|---|---|---|---|
| `docs/cat_agregado.json` | CAT/INSS (dadosabertos.inss.gov.br) | Mensal (defasagem ~2–3 meses) | CC-BY |
| `docs/sinan_agregado.json` | SINAN/DATASUS (Ministério da Saúde) | Anual (anos recentes preliminares) | Dado público |

## Regras de dados (invioláveis)

- Os arquivos publicados contêm **apenas agregados** (contagens por município × recorte × período). Nenhum microdado é republicado, pelo princípio da minimização (LGPD, art. 6º, III).
- **CAT e SINAN nunca são somados num mesmo indicador**: são séries paralelas, cada número carrega rótulo de fonte; a diferença entre elas é medida indireta de subnotificação.
- Anos SINAN não consolidados carregam a marcação `preliminar: true`.
- Nenhum dado fictício: onde a fonte falha, o pipeline registra a lacuna — nunca estima.

## Replicação e responsabilidade sobre dados (LGPD)

> Este software é distribuído como está, sem coleta, acesso ou tratamento de dados pessoais pelo autor. O ente (secretaria de saúde, órgão público ou instituição) que optar por hospedar sua própria instância, ou por arquivar e consolidar relatórios gerados por seus agentes, assume integralmente o papel de **controlador de dados** nos termos da Lei nº 13.709/2018 (LGPD), cabendo-lhe definir finalidade, base legal, retenção, segurança (art. 46) e demais obrigações aplicáveis, inclusive a elaboração de Relatório de Impacto quando cabível. O autor do software não integra a cadeia de tratamento de dados dessas instâncias e não presta serviços de hospedagem, suporte com acesso a dados ou atualização remota.

O ente que fizer **fork** deste repositório herda o pipeline completo e passa a controlar sua própria cadeia de atualização.

## Estrutura

```
.github/workflows/cat-sync.yml    # roda dia 5 de cada mês (e manualmente)
.github/workflows/sinan-sync.yml  # roda dia 6 de cada mês (e manualmente)
scripts/agrega_cat.py
scripts/agrega_sinan.py
docs/                             # publicado via GitHub Pages
```

## Primeiro uso: modo inspeção obrigatório

Antes do primeiro run normal, os dois workflows devem ser executados manualmente com `inspecao = 1` (aba **Actions → Run workflow**). Esse modo apenas verifica colunas reais, caminhos e codificações — **nada é fixado sem verificação**. Os resultados ficam em `docs/inspecao_cat.json` e `docs/inspecao_sinan.json` e no log da execução.
