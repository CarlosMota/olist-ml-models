with tb_join as (
    select DISTINCT t1.order_id as idPedido,
        t1.customer_id as idCliente,
        t2.seller_id as idVendedor,
        t3.customer_state as descUF
    from tb_orders t1
        left join tb_order_items t2 on t1.order_id = t2.order_id
        left join tb_customers t3 on t1.customer_id = t3.customer_id
    where date(order_purchase_timestamp) BETWEEN date('2018-01-01', '-6 months') and date('2017-12-31')
        and t2.seller_id is not null
),
tb_grouped as (
    select idVendedor,
        cast(
            count(
                distinct case
                    when descUF = 'AC' then idPedido
                end
            ) as real
        ) / count(DISTINCT idPedido) as pctPedidoAC,
        cast(
            count(
                distinct case
                    when descUF = 'AL' then idPedido
                end
            ) as real
        ) / count(DISTINCT idPedido) as pctPedidoAL,
        cast(
            count(
                distinct case
                    when descUF = 'AM' then idPedido
                end
            ) as real
        ) / count(DISTINCT idPedido) as pctPedidoAM,
        cast(
            count(
                distinct case
                    when descUF = 'AP' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoAP,
        cast(
            count(
                distinct case
                    when descUF = 'BA' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoBA,
        cast(
            count(
                distinct case
                    when descUF = 'CE' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoCE,
        cast(
            count(
                distinct case
                    when descUF = 'DF' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoDF,
        cast(
            count(
                distinct case
                    when descUF = 'ES' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoES,
        cast(
            count(
                distinct case
                    when descUF = 'GO' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoGO,
        cast(
            count(
                distinct case
                    when descUF = 'MA' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoMA,
        cast(
            count(
                distinct case
                    when descUF = 'MG' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoMG,
        cast(
            count(
                distinct case
                    when descUF = 'MS' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoMS,
        cast(
            count(
                distinct case
                    when descUF = 'MT' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoMT,
        cast(
            count(
                distinct case
                    when descUF = 'PA' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoPA,
        cast(
            count(
                distinct case
                    when descUF = 'PB' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoPB,
        cast(
            count(
                distinct case
                    when descUF = 'PE' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoPE,
        cast(
            count(
                distinct case
                    when descUF = 'PI' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoPI,
        cast(
            count(
                distinct case
                    when descUF = 'PR' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoPR,
        cast(
            count(
                distinct case
                    when descUF = 'RJ' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoRJ,
        cast(
            count(
                distinct case
                    when descUF = 'RN' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoRN,
        cast(
            count(
                distinct case
                    when descUF = 'RO' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoRO,
        cast(
            count(
                distinct case
                    when descUF = 'RR' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoRR,
        cast(
            count(
                distinct case
                    when descUF = 'RS' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoRS,
        cast(
            count(
                distinct case
                    when descUF = 'SC' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoSC,
        cast(
            count(
                distinct case
                    when descUF = 'SE' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoSE,
        cast(
            count(
                distinct case
                    when descUF = 'SP' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoSP,
        cast(
            count(
                distinct case
                    when descUF = 'TO' then idPedido
                end
            ) AS REAL
        ) / count(DISTINCT idPedido) as pctPedidoTO
    from tb_join
    group by idVendedor
    order by 1
)
select '2018-01-01' as dtReference,
    date('now') as dtIngestion,
    *
from tb_grouped