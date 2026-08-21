# dados-sst — Pipeline de dados públicos agregados (CAT/INSS e SINAN/DATASUS)

Automação **sem servidor** (GitHub Actions) que baixa dados públicos oficiais de saúde do trabalhador, agrega e publica arquivos JSON estáticos via GitHub Pages. Não há backend, banco de dados remoto nem coleta de dados de usuários.

| Arquivo publicado | Fonte | Periodicidade | Licença da fonte |
|---|---|---|---|
| `docs/cat_agregado.json` | CAT/INSS (dadosabertos.inss.gov.br) | Mensal (defasagem administrativa irregular) | CC-BY |
| `docs/sinan_agregado.json` | SINAN/DATASUS (Ministério da Saúde) | Anual (anos recentes preliminares) | Dado público |

Os metadados de cada série (`cat_meta.json`, `sinan_meta.json`) registram data de processamento, definições adotadas, lacunas e limitações conhecidas das fontes.

## Regras de dados (invioláveis)

- Os arquivos publicados contêm **apenas agregados** (contagens por município × recorte × período). Nenhum microdado é republicado, pelo princípio da minimização (LGPD, art. 6º, III).
- **CAT e SINAN nunca são somados num mesmo indicador**: são séries paralelas e cada número carrega rótulo de fonte; a diferença entre elas serve como medida indireta de subnotificação.
- Anos SINAN não consolidados carregam a marcação `preliminar: true`.
- Nenhum dado fictício: onde a fonte falha, o pipeline registra a lacuna — nunca estima.

## Estrutura

```
.github/workflows/   # cat-sync (mensal), sinan-sync (mensal) e utilitários de auditoria
scripts/             # agregação, conferência e diagnóstico
docs/                # arquivos publicados via GitHub Pages (agregados, metadados e inspeções)
```

Os scripts possuem **modo inspeção** (`inspecao = 1` na execução manual): verificam colunas reais, caminhos e codificações das fontes sem publicar nada — nada é fixado sem verificação.

## Direitos

© 2026 [Karol Teixeira de Oliveira]. Código licenciado sob [PolyForm Noncommercial 1.0.0] (https://github.com/karoltoliv/dados-sst/blob/main/LICENSE): uso, cópia e modificação permitidos para fins não comerciais, com atribuição. Os dados agregados derivam de fontes públicas e seguem as licenças das respectivas origens (CC-BY/INSS; dado público/DATASUS).
