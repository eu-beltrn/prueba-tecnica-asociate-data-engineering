-- Fase 2: Análisis de Anomalías (Detección de picos de gasto)
WITH transacciones_ordenadas AS (
    SELECT 
        id_cliente,
        id_transaccion,
        fecha_hora,
        monto_usd,
        estado_transaccion,
        -- Traemos el monto de la transacción INMEDIATAMENTE ANTERIOR del mismo cliente
        LAG(monto_usd) OVER(
            PARTITION BY id_cliente 
            ORDER BY fecha_hora ASC, id_transaccion ASC
        ) AS monto_anterior
    FROM transacciones_diarias_limpias
    -- REGLA DE NEGOCIO 4: Evaluar solo transacciones aprobadas
    WHERE LOWER(estado_transaccion) = 'aprobada'
)
SELECT 
    id_cliente,
    id_transaccion,
    fecha_hora,
    monto_anterior,
    monto_usd AS monto_actual,
    -- Calculamos cuántas veces es mayor el monto actual frente al anterior
    ROUND((monto_usd / monto_anterior)::numeric, 2) AS veces_mayor
FROM transacciones_ordenadas
-- Filtro crítico: El monto actual debe ser al menos 5 veces mayor al anterior
WHERE monto_anterior IS NOT NULL 
  AND monto_usd >= (5 * monto_anterior)
ORDER BY veces_mayor DESC;