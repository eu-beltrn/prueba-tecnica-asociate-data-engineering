-- Fase 2: Análisis de Anomalías (Detección de picos de gasto)
WITH historial_transacciones AS (
    SELECT 
        id_cliente,
        id_transaccion,
        fecha_hora,
        monto_usd,
        estado_transaccion,
        -- Window Function para obtener el monto de la transacción aprobada anterior del mismo cliente
        LAG(monto_usd, 1) OVER(
            PARTITION BY id_cliente 
            ORDER BY fecha_hora ASC
        ) AS monto_transaccion_anterior
    FROM public.transacciones_diarias_limpias
    -- Regla de Negocio 4: Evaluar estrictamente transacciones aprobadas
    WHERE estado_transaccion = 'aprobada'
)
SELECT 
    id_cliente,
    id_transaccion,
    fecha_hora,
    monto_transaccion_anterior AS monto_anterior,
    monto_usd AS monto_actual,
    -- Cálculo del multiplicador de consumo
    ROUND((monto_usd / monto_transaccion_anterior)::numeric, 2) AS multiplicador_veces_mayor
FROM historial_transacciones
-- Filtro analítico: transacciones que sean al menos 5 veces mayores a la anterior
WHERE monto_transaccion_anterior IS NOT NULL 
  AND monto_usd >= (5 * monto_transaccion_anterior)
ORDER BY multiplicador_veces_mayor DESC;