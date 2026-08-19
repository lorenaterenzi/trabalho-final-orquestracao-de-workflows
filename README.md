# Pipeline Financeiro de Cotações de Criptomoedas

## 1. Contexto e Descrição do Problema
Este projeto é um pipeline de dados financeiro automatizado que realiza a extração de cotações via API pública da CoinGecko, tratando e persistindo as informações em um banco relacional PostgreSQL.

## 2. Arquitetura da Solução
- **Fonte de Dados:** API REST pública (CoinGecko).
- **Orquestração:** Prefect 3.0 com retries e monitoramento.
- **Transformação:** Pandas processando os payloads e garantindo tipagem.
- **Armazenamento:** PostgreSQL 15.

## 3. Justificativa das Escolhas Técnicas
- **Prefect 3.0:** Sintaxe Python nativa com observabilidade e retries automáticos.
- **PostgreSQL:** Persistência relacional robusta.
- **Docker Compose:** Execução completa da infraestrutura em um comando.

## 4. Instruções de Execução
```bash
docker compose up --build
```
Acesse a interface em http://localhost:4200.

## 5. Idempotência e Resiliência
- **Resiliência:** Task de extração com 3 tentativas (retries=3).

- **Idempotência:** Gravação com if_exists="replace" na tabela consolidada.

## Vídeo de Apresentação
- **Link do Vídeo:** [INSERIR_LINK_AQUI]