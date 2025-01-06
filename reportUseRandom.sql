USE [TableroV4_Hyundai_Angelopolis]
GO

/****** Object:  View [dbo].[vv_ordenes_cotizadas]    Script Date: 05/01/2025 08:53:02 p. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[vv_ordenes_cotizadas]
AS

SELECT DISTINCT
	fecha,
	noorden
FROM
	dbo.vfv_productividad_ssl
WHERE
	recomendado = 1
	AND noorden NOT IN (0)

UNION
SELECT DISTINCT
	fecha,
	noorden
FROM
	dbo.vfv_productividad_ssl
WHERE
	inmediato = 1
	AND noorden NOT IN (0)
GO



-------------------------------------------------
USE [TableroV4_Hyundai_Angelopolis]
GO

/****** Object:  View [dbo].[vv_reporte_uso_global]    Script Date: 05/01/2025 08:46:54 p. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[vv_reporte_uso_global] -- SELECT * FROM dbo.vv_reporte_uso_global ORDER BY anio, mes;
AS

WITH tb_entradas_show AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS entradas_show
	FROM 
		dbo.v_citas_show_noshow
	WHERE
		show = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_entradas_sin_cita AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS entradas_sin_cita
	FROM 
		dbo.v_citas_show_noshow
	WHERE
		sin_cita = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_citas_grabadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(cita) AS citas_grabadas
	FROM 
		dbo.v_citas_show_noshow
	WHERE
		cita = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_citas_no_grabadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS citas_no_grabadas
	FROM 
		dbo.fv_control_citas
	WHERE
		estado = 'No Grabada'
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_ordenes_enlazadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_enlazadas
	FROM 
		dbo.vfv_productividad_asesor
	WHERE
		enlazada = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_ordenes_show AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_show
	FROM 
		dbo.vfv_productividad_asesor
	WHERE
		show = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

/*
	 tb_ordenes_no_show AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_no_show
	FROM 
		dbo.vfv_productividad_asesor
	WHERE
		noshow = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),
*/

	 tb_ordenes_mantenimiento AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_mantenimiento
	FROM 
		dbo.tb_citas
	WHERE
		noorden <> 0
		AND tipoCliente = 'mantenimiento'
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_ordenes_programadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_programadas
	FROM 
		dbo.v_tiempo_operacion_real
	WHERE
		YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_ordenes_finalizadas_lavado AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_finalizadas_lavado
	FROM 
		dbo.vfv_productividad_lavado
	WHERE
		entregado = 1
		AND YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 aux_tb_inspecciones_realizadas AS (
	SELECT 
		no_orden,
		fecha = fecha_hora_ingreso,
		ROW_NUMBER() OVER (PARTITION BY no_orden ORDER BY fecha_hora_ingreso DESC) AS rn
	FROM 
		[capnet-apps-hyundai-angelopolis].dbo.informacion
	WHERE
		no_orden IN (SELECT a.no_orden FROM [capnet-apps-hyundai-angelopolis].dbo.actividades_tecnico a)
		AND YEAR(fecha_hora_ingreso) >= 2024
		AND no_orden NOT IN (0)
),

	 tb_inspecciones_realizadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS inspecciones_realizadas
	FROM 
		aux_tb_inspecciones_realizadas
	WHERE 
		rn = 1
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 aux_tb_ordenes_cotizadas AS (
	SELECT 
		noorden,
		fecha,
		ROW_NUMBER() OVER (PARTITION BY noorden ORDER BY fecha DESC) AS rn
	FROM 
		dbo.vv_ordenes_cotizadas
	WHERE
		YEAR(fecha) >= 2024
),

	 tb_ordenes_cotizadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_cotizadas
	FROM 
		aux_tb_ordenes_cotizadas
	WHERE 
		rn = 1
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 aux_tb_ordenes_enviadas AS (
	SELECT 
		no_orden,
		fecha = fecha_hora_ingreso,
		ROW_NUMBER() OVER (PARTITION BY no_orden ORDER BY fecha_hora_ingreso DESC) AS rn
	FROM 
		[capnet-apps-hyundai-angelopolis].dbo.informacion
	WHERE
		no_orden IN (SELECT a.no_orden FROM [capnet-apps-hyundai-angelopolis].dbo.log_envios a)
		AND YEAR(fecha_hora_ingreso) >= 2024
		AND no_orden NOT IN (0)
),

	 tb_ordenes_enviadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_enviadas
	FROM 
		aux_tb_ordenes_enviadas
	WHERE 
		rn = 1
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_ordenes_calidad AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_calidad
	FROM 
		dbo.fv_productividad_calidad
	WHERE
		YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

	 tb_encuestas_realizadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS encuestas_realizadas
	FROM 
		dbo.v_kpis_encuestas
	WHERE
		YEAR(fecha) >= 2024
	GROUP BY
		YEAR(fecha),
		MONTH(fecha)
),

---------- ORIGIN REPORT ----------
	 reporte_origen AS (
	SELECT
		'Hyundai Angelopolis' AS concesionario,
		'Excelencia' AS grupo,
		a.anio,
		a.mes,
		total_ingresos = a.entradas_show + b.entradas_sin_cita,
		total_citas = c.citas_grabadas + d.citas_no_grabadas,
		c.citas_grabadas,
		d.citas_no_grabadas,
		ingresos_sin_cita = b.entradas_sin_cita,
		ordenes_enlazadas,
		total_ordenes = f.ordenes_show,
		h.ordenes_mantenimiento,
		i.ordenes_programadas,
		j.ordenes_finalizadas_lavado,
		k.inspecciones_realizadas,
		l.ordenes_cotizadas,
		m.ordenes_enviadas,
		n.ordenes_calidad,
		o.encuestas_realizadas
	FROM
		tb_entradas_show a
	LEFT JOIN
		tb_entradas_sin_cita b ON a.anio = b.anio AND a.mes = b.mes
	LEFT JOIN
		tb_citas_grabadas c ON a.anio = c.anio AND a.mes = c.mes
	LEFT JOIN
		tb_citas_no_grabadas d ON a.anio = d.anio AND a.mes = d.mes
	LEFT JOIN
		tb_ordenes_enlazadas e ON a.anio = e.anio AND a.mes = e.mes
	LEFT JOIN
		tb_ordenes_show f ON a.anio = f.anio AND a.mes = f.mes
	--LEFT JOIN
	--	tb_ordenes_no_show g ON a.anio = g.anio AND a.mes = g.mes
	LEFT JOIN
		tb_ordenes_mantenimiento h ON a.anio = h.anio AND a.mes = h.mes
	LEFT JOIN
		tb_ordenes_programadas i ON a.anio = i.anio AND a.mes = i.mes
	LEFT JOIN
		tb_ordenes_finalizadas_lavado j ON a.anio = j.anio AND a.mes = j.mes
	LEFT JOIN
		tb_inspecciones_realizadas k ON a.anio = k.anio AND a.mes = k.mes
	LEFT JOIN
		tb_ordenes_cotizadas l ON a.anio = l.anio AND a.mes = l.mes
	LEFT JOIN
		tb_ordenes_enviadas m ON a.anio = m.anio AND a.mes = m.mes
	LEFT JOIN
		tb_ordenes_calidad n ON a.anio = n.anio AND a.mes = n.mes
	LEFT JOIN
		tb_encuestas_realizadas o ON a.anio = o.anio AND a.mes = o.mes
	),

---------- PREV REPORT ----------
		 reporte_previo AS (
	SELECT
		concesionario,
		grupo,
		anio,
		mes,
		total_ingresos = CASE WHEN total_ingresos > 0 THEN ISNULL(total_ingresos, 0) ELSE 0 END,
		total_citas = CASE WHEN total_citas > 0 THEN ISNULL(total_citas, 0) ELSE 0 END,
		citas_grabadas = CASE WHEN citas_grabadas > 0 THEN ISNULL(citas_grabadas, 0) ELSE 0 END,
		citas_no_grabadas = CASE WHEN citas_no_grabadas > 0 THEN ISNULL(citas_no_grabadas, 0) ELSE 0 END,
		ingresos_con_cita = CASE WHEN (total_ingresos - ingresos_sin_cita) > 0 THEN ISNULL((total_ingresos - ingresos_sin_cita), 0) ELSE 0 END,
		ingresos_sin_cita = CASE WHEN ingresos_sin_cita > 0 THEN ISNULL(ingresos_sin_cita, 0) ELSE 0 END,
		ordenes_enlazadas = CASE WHEN ordenes_enlazadas > 0 THEN ISNULL(ordenes_enlazadas, 0) ELSE 0 END,
		total_ordenes = CASE WHEN total_ordenes > 0 THEN ISNULL(total_ordenes, 0) ELSE 0 END,
		ordenes_mantenimiento = CASE WHEN ordenes_mantenimiento > 0 THEN ISNULL(ordenes_mantenimiento, 0) ELSE 0 END,
		ordenes_programadas = CASE WHEN ordenes_programadas > 0 THEN ISNULL(ordenes_programadas, 0) ELSE 0 END,
		ordenes_finalizadas_lavado = CASE WHEN ordenes_finalizadas_lavado > 0 THEN ISNULL(ordenes_finalizadas_lavado, 0) ELSE 0 END,
		inspecciones_realizadas = CASE WHEN inspecciones_realizadas > 0 THEN ISNULL(inspecciones_realizadas, 0) ELSE 0 END,
		ordenes_cotizadas = CASE WHEN ordenes_cotizadas > 0 THEN ISNULL(ordenes_cotizadas, 0) ELSE 0 END,
		ordenes_enviadas = CASE WHEN ordenes_enviadas > 0 THEN ISNULL(ordenes_enviadas, 0) ELSE 0 END,
		ordenes_calidad = CASE WHEN ordenes_calidad > 0 THEN ISNULL(ordenes_calidad, 0) ELSE 0 END,
		encuestas_realizadas = CASE WHEN encuestas_realizadas > 0 THEN ISNULL(encuestas_realizadas, 0) ELSE 0 END
	FROM
		reporte_origen
)

SELECT
	concesionario,
	grupo,
	anio,
	mes,
	total_ingresos,
	total_citas,
	citas_grabadas,
	citas_no_grabadas,
	ingresos_con_cita,
	ingresos_sin_cita,
	manejo_cita = CASE WHEN total_citas > 0 THEN ISNULL((ingresos_con_cita * 100.0) / total_citas, 0) ELSE 0 END,
	ordenes_enlazadas,
	asesoria = CASE WHEN total_ordenes > 0 THEN ISNULL((ordenes_enlazadas * 100.0) / total_ordenes, 0) ELSE 0 END,
	total_ordenes,
	ordenes_mantenimiento,
	ordenes_programadas,
	asignacion_trabajo = CASE WHEN ordenes_programadas > 0 THEN ISNULL((total_ordenes * 100.0) / ordenes_programadas, 0) ELSE 0 END,
	ordenes_finalizadas_lavado,
	manejo_estatus_ordenes_servicio = CASE WHEN ordenes_programadas > 0 THEN ISNULL((ordenes_finalizadas_lavado * 100.0) / ordenes_programadas, 0) ELSE 0 END,
	manejo_contenido_ordenes_servicio = CASE WHEN ordenes_enlazadas > 0 THEN ISNULL((ordenes_finalizadas_lavado * 100.0) / ordenes_enlazadas, 0) ELSE 0 END,
	inspecciones_realizadas,
	inspeccion_vhc = CASE WHEN ordenes_mantenimiento > 0 THEN ISNULL((inspecciones_realizadas * 100.0) / ordenes_mantenimiento, 0) ELSE 0 END,
	ordenes_cotizadas,
	ordenes_enviadas,
	cotizacion = CASE WHEN ordenes_cotizadas > 0 THEN ISNULL((ordenes_enviadas * 100.0) / ordenes_cotizadas, 0) ELSE 0 END,
	ordenes_calidad,
	inspeccion_final = CASE WHEN total_ordenes > 0 THEN ISNULL((ordenes_calidad * 100.0) / total_ordenes, 0) ELSE 0 END,
	encuestas_realizadas,
	encuestas_salida = CASE WHEN total_ordenes > 0 THEN ISNULL((encuestas_realizadas * 100.0) / total_ordenes, 0) ELSE 0 END
FROM
	reporte_previo;
GO



-------------------------------------------------
select fecha,
	DATEPART(yy, fecha) as Year,