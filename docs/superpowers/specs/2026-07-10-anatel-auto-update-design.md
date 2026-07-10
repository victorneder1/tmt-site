# Atualização automática dos dados da Anatel — Design

**Data:** 2026-07-10
**Status:** Aprovado para planejamento de implementação

## Contexto

O blueprint `/telecom` lê `data/anatel.db` (SQLite) para alimentar os dashboards de
market share de banda larga e telefonia móvel. Hoje esse banco é atualizado por um
pipeline 100% manual:

1. Download manual dos zips de dados abertos da Anatel (broadband ~1GB, mobile ~3GB,
   portabilidade ~7.6MB).
2. `process_data_telecom.py` faz o parsing (sempre em modo full rebuild — a Anatel
   revisa meses retroativamente, então um rebuild incremental por ano não é confiável).
3. `update_telecom.py` orquestra o processamento e faz `POST` do `anatel.db` inteiro
   para `{SERVER_URL}/api/upload-anatel` no Railway.

Além disso, existe um quarto conjunto de dados — assinantes por MVNO/credenciada
(ex: NuCel hospedada na Claro) — que vem de um painel Qlik Sense
(`https://informacoes.anatel.gov.br/paineis/acessos/telefonia-movel`), acessado via
websocket replicando o botão "Exportar Dados" do painel. Hoje esse dado é exportado
manualmente mês a mês para `Database/MVNO/MVNO_Credenciadas_YYYY-MM.csv`
(jun/2025–mai/2026 já coletados) e **não tem nenhum consumidor no app**.

O objetivo desta automação é que, no mesmo dia em que a Anatel publicar dados novos
em qualquer uma das 4 fontes, o usuário seja notificado e o site reflita o dado novo
sem trabalho manual de descoberta — mantendo um humano no laço apenas para rodar o
processamento pesado e revisar casos suspeitos.

O repo já tem um precedente direto para "monitorar fonte externa + notificar":
`corporate/sec_monitor.py` roda uma thread em background dentro do próprio processo
Flask (padrão `SECMonitor`), fazendo polling periódico e notificando via
`corporate/notifier.py` (`CompositeNotifier`: email SMTP + Teams webhook,
configuráveis por env var ou `settings.local.json`). Essa automação reaproveita esse
padrão em vez de criar um mecanismo novo.

## Objetivos

- Detectar no mesmo dia (ou com atraso de poucas horas) quando a Anatel publica dado
  novo em qualquer uma das 4 fontes: broadband, mobile, portabilidade, MVNO.
- Notificar o usuário (email + Teams) tanto quando há dado novo quanto quando a
  automação falha de forma inesperada.
- Automatizar o download + processamento + upload para produção, com validações de
  sanidade e backup, mantendo aprovação manual apenas para os casos que falharem
  validação.
- Trazer o MVNO para o mesmo nível dos outros 3 conjuntos: tabela no `anatel.db`,
  rota em `telecom.py`, visualização no frontend.

## Não-objetivos

- Não migrar o processamento pesado (download de zips + parsing pandas) para rodar
  no Railway — fica na máquina Windows local, como hoje.
- Não implementar disparo automático do processamento local (ex: Task Scheduler que
  roda sozinho) — o gatilho é sempre manual, após o usuário ler a notificação.
- Não adicionar granularidade de UF ao MVNO (o dado de origem não tem essa coluna).
- Não adicionar nenhum gate/consentimento extra para o acesso não-interativo ao
  endpoint Qlik além do que já foi autorizado nesta sessão — é a mesma ação que o
  botão "Exportar Dados" do painel público da Anatel permite a qualquer usuário
  manualmente.

## Arquitetura

```
Railway (site ao vivo, 24/7)                    Sua máquina (Windows)
┌───────────────────────────────┐               ┌──────────────────────────┐
│ Flask app                      │               │ update_telecom.py (novo) │
│  └─ AnatelMonitor (thread)      │  email/Teams  │  1. checa cada fonte      │
│     • roda 1x/dia (~8h,         │──────────────>│     (anatel_checker.py) │
│       configurável)             │  "saiu dado   │  2. baixa só o que mudou │
│     • HEAD request x3           │   novo em X"  │  3. reprocessa (full     │
│       (broadband/mobile/port.)  │  ou erro      │     rebuild da fonte)    │
│     • websocket Qlik (MVNO)     │               │  4. valida sanidade      │
│     • persiste estado em        │               │  5. backup .db anterior  │
│       data/anatel_state.json    │               │  6. upload automático OU │
│       (volume Railway)          │               │     bloqueia + notifica  │
└───────────────────────────────┘               └──────────────────────────┘
```

A lógica de "isso mudou desde a última vez que eu vi?" vive em um módulo
compartilhado, `anatel_checker.py`, usado tanto pela thread no Railway (compara
contra o state file no volume do Railway) quanto pelo script local (compara contra
um state file local separado, ex: `data/anatel_state_local.json`). Cada lado mantém
seu próprio "última versão vista": o Railway nunca baixa nada pesado, e o script
local sempre sabe sozinho quais das 4 fontes precisa buscar — não depende do usuário
lembrar qual notificação leu.

## Componentes

### `anatel_checker.py` (módulo novo, compartilhado)

- `check_all_sources(state: dict) -> list[SourceChange]`: recebe o estado anterior
  (dict com `last_modified` + `content_length` por fonte HTTP, e `max_data` do Qlik
  para o MVNO), faz as checagens, retorna o que mudou.
- Broadband/mobile/portabilidade: `HEAD` request nas URLs fixas dos zips, compara
  `Last-Modified` e `Content-Length` contra o estado salvo.
- MVNO: abre a conexão websocket ao Qlik Engine (app
  `b00f5b60-c868-4b2e-b235-74ffc5c04a5a`, `dados.anatel.gov.br:443/qap/`) e lê o
  campo de data máxima disponível (candidato: `maxData`, a confirmar na
  implementação) — sem exportar a hypercube inteira do objeto `DPbTpjM`.
- Sem dependência de pandas/Excel — roda tanto no dyno do Railway quanto localmente.

### `AnatelMonitor` (thread em background no Flask app, Railway)

- Mesmo esqueleto do `SECMonitor`: `threading.Thread` daemon, loop com
  `stop_event.wait(interval)`, lock para estado compartilhado, expõe
  `last_check_at` / `last_success_at` / `last_error` para debug.
- Intervalo: 1x/dia, horário configurável por env var (ex: `ANATEL_CHECK_HOUR`,
  default 8h) — não é polling curto, dado que a meta é "no mesmo dia", não em tempo
  real.
- Ao detectar mudança em uma fonte: monta e envia uma notificação por fonte via
  `CompositeNotifier` (novo builder de payload, ex: `build_anatel_payload`, seguindo
  o padrão de `build_teams_payload` / `build_email_message` em
  `corporate/notifier.py`), e persiste o novo estado em `data/anatel_state.json`.
- Erros inesperados durante a checagem (timeout, resposta que não é o zip esperado,
  mudança de estrutura no Qlik) também disparam notificação imediata pelo mesmo
  canal, com a mensagem da exceção e qual fonte falhou.

### Script local (evolução de `update_telecom.py`)

Disparo sempre manual, após o usuário ler a notificação. Passo a passo:

1. Chama `anatel_checker.check_all_sources()` com o state file local — descobre
   sozinho quais das 4 fontes mudaram.
2. Para cada fonte marcada como mudada:
   - Faz backup de um snapshot único dos CSVs/arquivos atuais antes de sobrescrever
     (sobrescrevendo o backup anterior — não acumula histórico por rodada).
   - Baixa o zip (ou exporta os meses novos via Qlik, no caso do MVNO — só os meses
     que ainda não têm CSV arquivado em `Database/MVNO/`).
   - Extrai e roda o parsing full-rebuild já existente para essa fonte
     (`process_broadband` / `process_mobile` / `process_portability` / novo
     `process_mvno`).
3. Roda as validações de sanidade sobre o `anatel.db` resultante (ver seção
   "Validação" abaixo).
4. Se tudo passa:
   - Faz backup do `anatel.db` de produção atual como `anatel.db.bak_<timestamp>`
     (mantém só o backup mais recente, não acumula).
   - Faz upload automático via `POST {SERVER_URL}/api/upload-anatel`.
   - Notifica sucesso (email + Teams).
   - Atualiza o state file local só depois do upload confirmar sucesso — evita
     marcar como "processado" algo que na verdade falhou no meio do caminho.
5. Se alguma validação falha:
   - **Não** sobe nada — produção continua servindo o `anatel.db` anterior.
   - O `anatel.db` novo fica salvo localmente como pendente (ex:
     `data/anatel_pending.db`).
   - Notifica detalhando qual validação falhou e os números suspeitos, para decisão
     manual (subir mesmo assim rodando um comando de força, ou investigar antes).

A correção hardcoded do TIM (nov/2018) continua rodando dentro de
`process_broadband`, sem mudança.

### Validação de sanidade (antes do upload automático)

Três checagens, todas precisam passar:

1. **Tabelas não vazias e mês esperado presente** — cada tabela (`broadband`,
   `mobile`, `portability`, `mvno`) tem linhas, e o mês mais recente presente bate
   com o mês que a checagem detectou como novo.
2. **Sem queda abrupta mês a mês por operador** — compara o total de acessos do mês
   novo vs. o mês anterior, por operador; queda acima de um limiar (ex: -30%, a
   calibrar na implementação) marca a fonte como suspeita.
3. **Tamanho do `anatel.db` dentro de uma faixa esperada** — se o `.db` novo for
   menor que ~80% do tamanho do `.db` de produção atual, é indício de truncamento no
   processamento.

Qualquer falha bloqueia o upload automático (ver passo 5 acima).

### Ingestão do MVNO

**Fonte:** os CSVs mensais já exportados manualmente
(`Database/MVNO/MVNO_Credenciadas_YYYY-MM.csv`, jun/2025–mai/2026) servem de base
histórica inicial. A automação exporta apenas os meses novos daí em diante.

**Schema — nova tabela `mvno` em `anatel.db`:**

| coluna | origem no CSV |
|---|---|
| `credenciada` | `Credenciada` |
| `cnpj_credenciada` | `CNPJ_Credenciada` |
| `operadora_hospedeira` | `Operadora_Hospedeira` (normalizado com o mesmo mapeamento de operadoras usado no mobile) |
| `cnpj_operadora` | `CNPJ_Operadora` |
| `month` | `Periodo` (YYYY-MM) |
| `accesses` | `Acessos` |

Índices em `month`, `credenciada`, `operadora_hospedeira`, seguindo o padrão das
outras tabelas (`CREATE INDEX` em `save_to_db`).

**Backend:** novo `process_mvno()` em `process_data_telecom.py` (lê os CSVs
arquivados, normaliza operadora hospedeira, agrega). Novas rotas em `telecom.py`:
`/telecom/api/mvno` (filtrável por mês / credenciada / operadora hospedeira) e
`/telecom/api/mvno/months`, seguindo o mesmo padrão de `broadband` / `mobile`.

**Frontend:** novo arquivo `static/telecom/mvno.js` + aba/seção no dashboard
telecom — tabela de credenciadas com acessos por mês, agrupável por operadora
hospedeira, no mesmo estilo visual das telas existentes. Sem mapa de UF (o dado de
origem não tem essa granularidade).

## Notificações

Reaproveita `corporate/notifier.py` (`CompositeNotifier`: email SMTP + Teams
webhook). Três tipos de mensagem, todas nos dois canais:

1. **Dado novo detectado** (thread no Railway): fonte, data detectada, link de
   referência.
2. **Erro inesperado** (thread no Railway ou script local): fonte, mensagem da
   exceção — cobre falha de rede, layout de CSV mudado, estrutura do Qlik mudada.
3. **Upload bloqueado por validação** (script local): qual checagem falhou e os
   números suspeitos — para decisão manual.
4. **Upload concluído com sucesso** (script local): confirmação simples.

## Segurança

O acesso ao endpoint Qlik (websocket, Engine JSON-RPC API) roda sem gate adicional
de consentimento interativo. Essa automação replica exatamente a ação que o botão
"Exportar Dados" do painel público
(`https://informacoes.anatel.gov.br/paineis/acessos/telefonia-movel`) permite a
qualquer usuário manualmente — não é acesso a dado restrito nem escalação de
privilégio, apenas automação de uma ação pública já permitida.

## Riscos e itens a confirmar na implementação

- O nome exato do campo de data máxima no Qlik (candidato `maxData`) precisa ser
  confirmado explorando o field list do app Qlik antes de implementar o checker do
  MVNO.
- O limiar de "queda abrupta mês a mês" (validação 2) precisa ser calibrado olhando
  a variância histórica real dos dados — um valor fixo de -30% é um ponto de partida,
  não um número validado.
- `DATA_DIR` no Railway precisa ser um volume persistente (não efêmero) para que
  `anatel_state.json` sobreviva a redeploys — confirmar que já é esse o caso hoje
  (mesma pasta onde `anatel.db` já vive em produção).

## Decisões tomadas durante o brainstorming

| Decisão | Escolha |
|---|---|
| Onde rodar a checagem | Leve no Railway (thread in-process), pesado local |
| Canal de notificação | Email + Teams (Composite, reaproveitando infra existente) |
| Upload para produção | Automático, com backup do `.db` anterior |
| Frequência de checagem | 1x/dia, horário fixo |
| Checagem do MVNO | Junto da checagem diária no Railway (websocket Qlik) |
| Escopo do MVNO | Ingestão completa (tabela + rota + frontend), não só arquivamento |
| Safeguard de acesso ao Qlik | Nenhuma salvaguarda extra além da já concedida |
| Tratamento de erro | Notificação imediata pelo mesmo canal |
| Validação antes do upload automático | 3 checagens (tabelas/mês, queda por operador, tamanho do `.db`); falha bloqueia upload |
| Histórico de CSVs baixados | Backup único (sobrescrito a cada rodada), não histórico acumulado |
| Gatilho do processamento local | Sempre manual, após o usuário ler a notificação |
