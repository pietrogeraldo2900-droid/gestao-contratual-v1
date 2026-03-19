# CHANGELOG V7.2

## Novidades
- Base mestra acumulada com anti-duplicidade
- Novo comando `consolidate-master`
- Novo parâmetro `--master-dir` nos comandos `parse` e `batch`
- Novo arquivo `processar_arquivo_e_atualizar_base_mestra.bat`
- Geração automática de `base_mestra_execucao.csv`, `base_mestra_ocorrencias.csv`, `base_mestra_frentes.csv` e `base_gerencial.xlsx` na base mestra

## Uso recomendado
- Dia a dia: processar 1 TXT e atualizar a base mestra
- Mensal: consolidar todas as saídas em uma base única e analisar o `base_gerencial.xlsx`
