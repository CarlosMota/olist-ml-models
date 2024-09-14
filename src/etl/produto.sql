with tb_join as (
    select DISTINCT t2.product_id as IdProduto,
        t2.seller_id as IdVendedor,
        t3.*,
        (
            t3.product_length_cm * t3.product_height_cm * t3.product_width_cm
        ) as Volume
    from tb_orders as t1
        left join tb_order_items as t2 on t1.order_id = t2.order_id
        left join tb_products as t3 on t2.product_id = t3.product_id
    where date(order_purchase_timestamp) BETWEEN date('2018-01-01', '-6 months') and date('2017-12-31')
        and t2.seller_id is not null
),
tb_volume_ordenador_por_vendedor as (
    select idVendedor,
        Volume,
        row_number() over (
            PARTITION BY IdVendedor
            ORDER BY Volume
        ) as rn,
        count(*) OVER (PARTITION BY IdVendedor) as total_volume_por_vendedor
    from tb_join
),
tb_mediana_volume_produto as(
    select idVendedor,
        AVG(volume) as medianVolumeProduto
    from tb_volume_ordenador_por_vendedor
    where rn in (
            (total_volume_por_vendedor) + 1 / 2,
            (total_volume_por_vendedor + 2) / 2
        )
    group by idVendedor
    order BY idVendedor
),
tb_summary as (
    select t1.IdVendedor,
        avg(coalesce(t1.product_photos_qty, 0)) as avgFotos,
        avg(t1.Volume) as avgVolumeProduto,
        t2.medianVolumeProduto,
        min(t1.Volume) as minVolumeProduto,
        max(t1.Volume) as maxVolumeProduto,
        cast(
            count(
                case
                    when product_category_name = "cama_mesa_banho" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_cama_mesa_banho,
        cast(
            count(
                case
                    when product_category_name = "esporte_lazer" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_esporte_lazer,
        cast(
            count(
                case
                    when product_category_name = "moveis_decoracao" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_moveis_decoracao,
        cast(
            count(
                case
                    when product_category_name = "beleza_saude" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_beleza_saude,
        cast(
            count(
                case
                    when product_category_name = "utilidades_domesticas" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_utilidades_domesticas,
        cast(
            count(
                case
                    when product_category_name = "automotivo" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_automotivo,
        cast(
            count(
                case
                    when product_category_name = "informatica_acessorios" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_informatica_acessorios,
        cast(
            count(
                case
                    when product_category_name = "brinquedos" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_brinquedos,
        cast(
            count(
                case
                    when product_category_name = "relogios_presentes" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_relogios_presentes,
        cast(
            count(
                case
                    when product_category_name = "telefonia" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_telefonia,
        cast(
            count(
                case
                    when product_category_name = "bebes" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_bebes,
        cast(
            count(
                case
                    when product_category_name = "perfumaria" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_perfumaria,
        cast(
            count(
                case
                    when product_category_name = "papelaria" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_papelaria,
        cast(
            count(
                case
                    when product_category_name = "fashion_bolsas_e_acessorios" then product_id
                end
            ) as Real
        ) / count(distinct product_id) pct_fashion_bolsas_e_acessorios
    from tb_join t1
        left join tb_mediana_volume_produto t2 on t1.idVendedor = t2.idVendedor
    group BY t1.idVendedor
)
select '2018-01-01' as dtReference,
    date('now') as dtIngestion,
    t1.*
from tb_summary t1