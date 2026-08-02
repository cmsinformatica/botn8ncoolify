# MoneyPrinterTurbo no Coolify

## Configuração do recurso

1. Crie um recurso **Docker Compose** apontando para este repositório.
2. Use `docker-compose.coolify.yml` como arquivo Compose.
3. Crie dois domínios no Coolify:
   - domínio da API apontando para o serviço `api`, porta `8080`;
   - domínio da interface apontando para o serviço `webui`, porta `8501`.
4. Configure as variáveis de `.env.coolify.example`.
5. Mantenha o volume nomeado `mpt_data`; ele guarda configuração, materiais temporários e vídeos.

## Variáveis mínimas

- `MPT_API_KEY`: chave privada usada pelo n8n para chamar a API.
- `MPT_LLM_PROVIDER`: use `openai` para provedores compatíveis.
- `MPT_OPENAI_API_KEY`, `MPT_OPENAI_BASE_URL` e `MPT_OPENAI_MODEL`.
- Uma fonte de materiais: `MPT_PEXELS_API_KEY` ou `MPT_PIXABAY_API_KEY`.

O Edge TTS é o padrão e não precisa de chave.

## Verificação

- API: `https://SEU-DOMINIO-API/docs`
- WebUI: `https://SEU-DOMINIO-WEBUI`
- Saúde da API: o Coolify consulta `/docs`.
- Saúde da WebUI: o Coolify consulta `/_stcore/health`.

## Segurança

- Não exponha `MPT_API_KEY` no navegador ou no Telegram.
- Restrinja a API no firewall ou no proxy para o servidor do n8n quando possível.
- Desative publicação automática durante os primeiros testes.
- Não use a tag de imagem `latest`; esta implantação constrói o commit fixado no repositório.

## Idempotência da criação de vídeos

`POST /api/v1/videos` aceita `Idempotency-Key` e, quando esse header não é
enviado, usa `X-Request-ID` como fallback. Repetir a mesma chave com o mesmo
payload devolve o mesmo `task_id` sem agendar uma segunda geração. Reutilizar a
chave com payload diferente retorna HTTP 409. As chaves recebidas não são
gravadas diretamente: somente um hash SHA-256 é usado como chave interna.

Com `enable_redis = false`, a exclusão atômica usa o lock do
`InMemoryTaskManager` e vale apenas dentro do mesmo processo Python. Isso cobre
o serviço API padrão deste Compose enquanto ele executar um único processo, mas
não oferece garantia entre réplicas ou múltiplos workers. Para esse cenário,
habilite Redis e faça todas as instâncias compartilharem o mesmo database; o
claim então usa `SET NX` atômico. Redis deve permanecer privado à aplicação.

Uma reserva recém-criada fica em `pending` até a tarefa entrar na fila; só então
é promovida por compare-and-set para `committed`. Se a promoção for ambígua
depois do enqueue, a API responde 503 e preserva tanto a tarefa quanto a
reserva: repita exclusivamente com a mesma `Idempotency-Key`. Não remova a
chave nem envie uma nova identidade. Para recuperação manual, consulte a chave
SHA-256 `moneyprinter:idempotency:videos:<hash>` e o `task_id` associado antes
de qualquer intervenção; um registro `pending` pode corresponder a uma tarefa
já aceita cuja confirmação Redis falhou.
