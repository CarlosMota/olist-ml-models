with tb_pedidos as (
    select DISTINCT t1.order_id,
        t2.seller_id
    from tb_orders t1
        left join tb_order_items t2 on t1.order_id = t2.order_id
    WHERE date(order_purchase_timestamp) BETWEEN date('2018-01-01', '-6 months') and date('2017-12-31')
        and t2.seller_id is not null
),
tb_join as (
    select t1.seller_id,
        t2.*
    from tb_pedidos t1
        left join tb_order_payments t2 on t1.order_id = t2.order_id
),
tb_num_vendas_ordenada as (
    select seller_id as idVendedor,
        payment_installments numParcelas,
        row_number() over (
            PARTITION BY seller_id
            order by payment_installments
        ) as rn,
        count(*) over (PARTITION BY seller_id) as total_num_parcela_por_vendedor -- Calcula o total de linhas pra cada vendedor
    from tb_join
),
tb_median_num_parcela_por_vendedor as (
    select idVendedor,
        avg(numParcelas) as medianQtdeParcelas
    from tb_num_vendas_ordenada
    where rn in (
            (total_num_parcela_por_vendedor + 1) / 2,
            (total_num_parcela_por_vendedor + 2) / 2
        )
    group by idVendedor
    order by idVendedor
),
tb_group as (
    select seller_id as idVendedor,
        payment_type as descTipoPagamento,
        count(DISTINCT order_id) as qtdPedidoMeioPagamento,
        sum(payment_value) as vlPedidoMeioPagamento
    from tb_join
    GROUP BY seller_id,
        payment_type
    order by seller_id,
        payment_type
),
tb_summary as (
    select idVendedor,
        sum(
            case
                when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento
                else 0
            end
        ) as qtde_credit_card,
        sum(
            case
                when descTipoPagamento = 'boleto' then qtdPedidoMeioPagamento
                else 0
            end
        ) as qtde_boleto,
        sum(
            case
                when descTipoPagamento = 'debit_card' then qtdPedidoMeioPagamento
                else 0
            end
        ) as qtde_debit_card,
        sum(
            case
                when descTipoPagamento = 'voucher' then qtdPedidoMeioPagamento
                else 0
            end
        ) as qtde_voucher,
        sum(
            case
                when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento
                else 0
            end
        ) as valor_credit_card,
        sum(
            case
                when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento
                else 0
            end
        ) as valor_boleto,
        sum(
            case
                when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento
                else 0
            end
        ) as valor_debit_card,
        sum(
            case
                when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento
                else 0
            end
        ) as valor_voucher,
        sum(
            case
                when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento
                else 0
            end
        ) / sum(qtdPedidoMeioPagamento) as pct_qtd_credit_card,
        sum(
            case
                when descTipoPagamento = 'boleto' then qtdPedidoMeioPagamento
                else 0
            end
        ) / sum(qtdPedidoMeioPagamento) as pct_qtd_boleto,
        sum(
            case
                when descTipoPagamento = 'debit_card' then qtdPedidoMeioPagamento
                else 0
            end
        ) / sum(qtdPedidoMeioPagamento) as pct_qtd_debit_card,
        sum(
            case
                when descTipoPagamento = 'voucher' then qtdPedidoMeioPagamento
                else 0
            end
        ) / sum(qtdPedidoMeioPagamento) as pct_qtd_voucher,
        sum(
            case
                when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento
                else 0
            end
        ) / sum(vlPedidoMeioPagamento) as pct_valor_boleto,
        sum(
            case
                when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento
                else 0
            end
        ) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card,
        sum(
            case
                when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento
                else 0
            end
        ) / sum(vlPedidoMeioPagamento) as pct_valor_voucher,
        sum(
            case
                when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento
                else 0
            end
        ) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card
    from tb_group
    group BY idvendedor
),
tb_cartao as (
    select seller_id as IdVendedor,
        AVG(payment_installments) AS avgQtdeParcelas,
        t2.medianQtdeParcelas,
        MAX(payment_installments) AS maxQtdeParcelas,
        MIN(payment_installments) as minQtdeParcelas
    from tb_join t1
        left join tb_median_num_parcela_por_vendedor t2 on t1.seller_id = t2.idVendedor
    where payment_type = 'credit_card'
    group BY seller_id
    order BY seller_id
)
select '2018-01-01' as dtReference,
    date('now') as dtIngestion,
    t1.*,
    t2.avgQtdeParcelas,
    t2.medianQtdeParcelas,
    t2.maxQtdeParcelas,
    t2.minQtdeParcelas
from tb_summary t1
    left join tb_cartao as t2 on t1.idVendedor = t2.idVendedor