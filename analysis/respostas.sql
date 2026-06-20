-- ==============================================================================
-- Resposta 1: Média de valor total recebido em um mês
-- ==============================================================================
-- O Data Mart de Receita Mensal (Gold Layer) já consolidou essa métrica na 
-- fase de engenharia, evitando que ferramentas de BI precisem escanear a base bruta.
SELECT
    ano,
    mes,
    avg_total_amount AS media_valor_total_recebido
FROM default.gold_nyc_tlc_monthly_revenue
ORDER BY ano, mes;


-- ==============================================================================
-- Resposta 2: Média de passageiros por cada hora do dia no mês de maio
-- ==============================================================================
-- O Data Mart Horário já agregou as viagens no nível de granularidade solicitado.
-- Para responder especificamente à pergunta, filtramos apenas o mês 05.
SELECT
    hora AS hora_do_dia,
    avg_passenger_count AS media_passageiros
FROM default.gold_nyc_tlc_hourly_passengers
WHERE ano = '2023' AND mes = '05'
ORDER BY hora;
