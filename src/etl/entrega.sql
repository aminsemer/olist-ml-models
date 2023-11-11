-- Databricks notebook source
WITH tb_pedido AS (
  SELECT t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega,
         ROUND(SUM(vlFrete),2) AS totalFrete

  FROM silver.olist.pedido AS t1
  LEFT JOIN silver.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '2018-01-01'
    AND dtPedido >= add_months('2018-01-01', -6 )

  GROUP BY t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega
)

SELECT idVendedor,
       COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' AND date(coalesce(dtEntregue, '2018-01-01')) > date(dtEstimativaEntrega) THEN idPedido END)
        / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
       COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) 
        / COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
        avg(totalFrete) AS avgFrete,
        percentile(totalFrete, 0.5) AS medianFrete,
        max(totalFrete) AS maxFrete,
        min(totalFrete) AS minFrete,
        avg(date_diff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
        avg(date_diff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
        avg(date_diff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) AS qtdDiasEntregaPromessa 

FROM tb_pedido
GROUP BY 1


