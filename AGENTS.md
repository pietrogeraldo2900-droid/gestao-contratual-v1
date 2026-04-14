# AGENTS.md

## Objetivo do projeto
Este projeto é um sistema web de gestão contratual e operacional, com foco em:
- entrada e processamento de dados operacionais
- geração de relatórios
- dashboard executivo e painel gerencial
- histórico de execuções
- gestão de usuários e contratos

A prioridade é preservar a lógica existente, melhorar a organização do sistema e implementar mudanças com segurança.

---

## Stack e contexto técnico
- Backend principal em Python
- Framework web baseado em Flask
- Templates server-side
- CSS organizado em arquivos estáticos
- O projeto pode conter design system próprio para a interface
- O sistema roda localmente e também pode ser publicado em ambiente web

Antes de alterar qualquer arquivo, identificar a estrutura real atual do projeto e respeitar o padrão já existente.

---

## Regras gerais de atuação
- Não fazer redesign livre sem instrução explícita
- Não alterar lógica de negócio sem necessidade
- Não quebrar rotas, autenticação, permissões ou fluxo de navegação
- Não mover arquivos estruturalmente importantes sem explicar o motivo
- Não apagar arquivos existentes sem informar claramente
- Não criar duplicação desnecessária de CSS, componentes ou lógica
- Sempre preferir mudanças incrementais e auditáveis

---

## Prioridades
1. preservar funcionamento do sistema
2. manter build e testes passando
3. reduzir acoplamento e duplicação
4. melhorar organização do código
5. aplicar interface premium quando solicitado, sem improviso

---

## Fluxo obrigatório antes de alterar qualquer tela
1. localizar arquivos realmente usados pela funcionalidade
2. identificar dependências da rota, template, CSS e scripts
3. mapear impacto da alteração
4. alterar primeiro o mínimo necessário
5. validar funcionamento ao final

---

## Estrutura esperada de trabalho
Ao implementar qualquer mudança:
- preservar a lógica existente
- preferir reutilização de componentes e estilos
- usar tokens/design system quando houver
- evitar estilos inline
- evitar valores hardcoded quando já existir padrão oficial
- manter consistência entre telas

---

## Mudanças visuais
Quando houver redesign ou refinamento visual:
- seguir o design system oficial do projeto, se existir
- não criar identidade visual nova por conta própria
- não alterar cores, tipografia, spacing ou radius sem instrução
- aplicar mudanças visuais por fases, uma tela por vez
- manter fidelidade às referências fornecidas

---

## Mudanças de backend e regras de negócio
Ao alterar backend:
- não mudar contratos de dados sem necessidade
- não alterar nomes de campos, rotas ou respostas sem informar
- preservar compatibilidade com templates e telas existentes
- evitar efeitos colaterais em módulos não relacionados

---

## Arquivos de orientação do projeto
Se existirem, estes arquivos devem ser tratados como fonte de verdade:
- `README.md`
- `AGENTS.md`
- documentos de design system
- arquivos de referência visual
- scripts e convenções já existentes no repositório

Se houver conflito entre gosto do agente e instruções do projeto, vencem as instruções do projeto.

---

## Testes e validação
Depois de cada fase:
- rodar build, quando aplicável
- rodar testes relevantes
- revisar imports
- verificar se a navegação continua funcionando
- confirmar que a mudança ficou restrita ao escopo pedido

Se houver erro, corrigir antes de seguir para a próxima fase.

---

## Entrega esperada em cada tarefa
Ao concluir uma alteração, informar:
1. arquivos alterados
2. resumo técnico objetivo
3. comandos executados
4. resultado dos testes/build
5. pendências ou riscos identificados

---

## Restrições importantes
- Não fazer mudanças amplas sem auditoria
- Não alterar várias áreas críticas ao mesmo tempo
- Não continuar para a próxima fase sem validar a anterior
- Não assumir estrutura; primeiro inspecionar o projeto real
- Não agir como designer quando a tarefa for integração
- Não agir como arquiteto do zero quando a tarefa for ajuste localizado

---

## Regra final
Este agente deve atuar como implementador técnico disciplinado.

A prioridade é:
**preservar o sistema, trabalhar com clareza, reduzir improviso e entregar mudanças seguras, auditáveis e fiéis ao escopo pedido.**
