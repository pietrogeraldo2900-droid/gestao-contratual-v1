# Design System Premium

Este documento descreve a base visual compartilhada do sistema e serve como referência para manter consistência entre telas.

## Arquivos-base

- `static/css/design-system/tokens.css`
- `static/css/design-system/foundations.css`
- `templates/design_system/components.html`

## Papel de cada arquivo

### `tokens.css`

Define os tokens semânticos e de fundação visual:
- cores de fundo, superfície, borda e texto
- acentos, estados e gradientes
- tipografia e escala de fontes
- espaçamento, raios e sombras
- medidas estruturais do shell

### `foundations.css`

Aplica a linguagem visual base sobre o HTML da aplicação:
- layout global do shell
- sidebar, topbar e área de conteúdo
- cards, botões, chips e inputs
- tabelas, badges e cards de KPI
- responsividade da fundação

### `components.html`

Centraliza componentes reutilizáveis em templates:
- shell da aplicação
- hero header
- cards de KPI e de insights
- painéis premium
- chips, badges e botões
- campos de formulário e tabelas

## Regras de uso

- sempre preferir tokens em vez de valores hardcoded
- evitar estilos inline quando existir classe compartilhada
- reutilizar componentes antes de criar variações novas
- manter a dashboard e o shell alinhados ao mesmo vocabulário visual
- não introduzir redesign fora do escopo pedido

## Estrutura atual relacionada

- `static/css/app-shell.css` para o shell autenticado
- `static/css/auth.css` para ajustes de autenticação e flashes
- `static/css/pages/dashboard.css` para a dashboard premium
- `templates/base.html` como shell comum
- `templates/dashboard/dashboard.html` como tela principal premium

## Regra operacional

Antes de criar uma nova tela ou refinar uma existente, verificar se:
- já existe token apropriado
- já existe componente reutilizável
- o novo estilo realmente precisa morar fora da base compartilhada

