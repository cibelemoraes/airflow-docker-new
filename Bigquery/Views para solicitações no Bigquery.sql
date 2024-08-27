--- Produtividade por Técnico por Mês
CREATE OR REPLACE VIEW `desafio-tecnico-stone.pedrapagamentos.produtividade_tecnico` AS
SELECT
    technician_email,
    EXTRACT(YEAR FROM arrival_date) AS ano,
    EXTRACT(MONTH FROM arrival_date) AS mes,
    COUNT(*) AS quantidade_atendimentos
FROM
    `desafio-tecnico-stone.pedrapagamentos.lotes`
GROUP BY
    technician_email, ano, mes;

--- Quantidade Total de Atendimentos
CREATE OR REPLACE VIEW `desafio-tecnico-stone.pedrapagamentos.total_atendimentos` AS
SELECT
    COUNT(*) AS total_atendimentos
FROM
    `desafio-tecnico-stone.pedrapagamentos.lotes`;

--- Quantidade de Atendimentos Dentro do Prazo
CREATE OR REPLACE VIEW `desafio-tecnico-stone.pedrapagamentos.atendimentos_dentro_prazo` AS
SELECT
    COUNT(*) AS atendimentos_dentro_prazo
FROM
    `desafio-tecnico-stone.pedrapagamentos.lotes`
WHERE
    arrival_date <= deadline_date;

--- Quantidade Individual de Atendimentos por Técnico
CREATE OR REPLACE VIEW `desafio-tecnico-stone.pedrapagamentos.quantidade_individual_atendimentos` AS
SELECT
    technician_email,
    COUNT(*) AS quantidade_atendimentos
FROM
    `desafio-tecnico-stone.pedrapagamentos.lotes`
GROUP BY
    technician_email;

--- Média de Atendimentos por Mês
CREATE OR REPLACE VIEW `desafio-tecnico-stone.pedrapagamentos.media_atendimentos_mes` AS
SELECT
    ano,
    mes,
    AVG(quantidade_atendimentos) AS media_atendimentos
FROM (
    SELECT
        EXTRACT(YEAR FROM arrival_date) AS ano,
        EXTRACT(MONTH FROM arrival_date) AS mes,
        COUNT(*) AS quantidade_atendimentos
    FROM
        `desafio-tecnico-stone.pedrapagamentos.lotes`
    GROUP BY
        ano, mes
) AS subquery
GROUP BY
    ano, mes;



