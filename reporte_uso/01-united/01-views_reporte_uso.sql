USE [TableroV4_Hyundai_Pedregal]
GO

/****** Object:  View [dbo].[v_control_citas]    Script Date: 2/11/2025 9:59:36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[v_control_citas]
as
with control_citas_gr as (
	select distinct
	 year(fecha) as anio 
	,right('0'+rtrim(month(fecha)),2) as mes
	,right('0'+rtrim(day(fecha)),2) as dia
	,USUARIO
	,NUMCITA
	,noPlacas
	,fecha
	,min(horaCita) as horaCita
	,1 as 'citas_grabadas',0 as 'citas_nograbadas' 
   from tb_citas
	where NUMCITA not in (0)
	and horaCita is not null
	--and fecha <= getdate()-360
	group by USUARIO
			,NUMCITA
			,noPlacas
			,fecha
			,idServicio
),

	 control_citas_nogr as (
	select distinct
	 year(fecha) as anio 
	,right('0'+rtrim(month(fecha)),2) as mes
	,right('0'+rtrim(day(fecha)),2) as dia
	,convert(varchar(50),USUARIO) as USUARIO
	,NUMCITA as NUMCITA
	,noPlacas
	,fecha
	,min(horaCita) as horaCita,0 as 'citas_grabadas',
1 as 'citas_nograbadas'

   from v_interfaz_dms_citas_hist
	where NUMCITA not in (0)
	and horaCita is not null
	--and fecha <= getdate()
	group by USUARIO
			,NUMCITA
			,noPlacas
			,fecha
			

)
select*,sum(citas_grabadas+citas_nograbadas)total ,	case when 
sum(case WHEN citas_grabadas<>0
		THEN 1
		ELSE 0
	END ) = 0 then 0 else (sum(case WHEN citas_grabadas <>0
		THEN 1
		ELSE 0
	END )+.00)/(sum(case WHEN citas_grabadas<>0
		THEN 1
		ELSE 0
	END )+.00)*100 end as Porcentaje from(
select
 a.anio
,a.mes
,a.dia
,a.fecha
,case when a.USUARIO is null then 'SIN_ASIGNAR'
	  when a.USUARIO = '' then 'SIN_ASIGNAR'
 else a.USUARIO end as usuario
,a.noPlacas  as noplacas,
citas_grabadas,
citas_nograbadas 
--,a.idServicio
,estado = 'Grabada'
from control_citas_gr a
where fecha>='20240101'



union
select
 b.anio
,b.mes
,b.dia
,b.fecha
,case when b.USUARIO is null then 'SIN_ASIGNAR'
	  when b.USUARIO = '' then 'SIN_ASIGNAR'
 else b.USUARIO end as usuario
, b.noPlacas  as noplacas,
citas_grabadas,
citas_nograbadas,
--, convert (varchar,b.idServicio) as idservicio
estado = 'No Grabada'
from control_citas_nogr b
where  fecha>='20240101' and b.NUMCITA not in (select a.NUMCITA from control_citas_gr a )
							--where   a.idServicio = convert(varchar,convert(int,b.idServicio))

) as ta
							group by anio,mes,dia,usuario,
		estado
			,noPlacas
			,fecha,citas_grabadas,
citas_nograbadas 
GO

/****** Object:  View [dbo].[vv_control_citas]    Script Date: 2/11/2025 9:59:36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[vv_control_citas] -- SELECT * FROM vv_control_citas;
AS

WITH control_citas_grabadas_unificadas AS (
    SELECT
        anio,
        mes,
        dia,
        CAST(fecha AS DATE) AS fecha,
        usuario,
        noplacas,
        citas_grabadas,
        citas_nograbadas,
        estado,
        total,
        porcentaje
    FROM fv_control_citas
    WHERE citas_grabadas = 1
),

	 control_citas_nograbadas_unificadas AS (
    SELECT
        anio,
        mes,
		dia,
        CAST(fecha AS DATE) AS fecha,
        usuario,
        noplacas,
        citas_grabadas,
        citas_nograbadas,
        estado,
        total,
        porcentaje
    FROM fv_control_citas
    WHERE citas_grabadas = 0
),

	 control_citas_total_unificadas AS (
	SELECT * 
	FROM control_citas_grabadas_unificadas

	UNION

	SELECT *
	FROM control_citas_nograbadas_unificadas
	WHERE NOT EXISTS (
		SELECT 1 
		FROM control_citas_grabadas_unificadas g
		WHERE g.fecha = control_citas_nograbadas_unificadas.fecha
		AND g.noplacas = control_citas_nograbadas_unificadas.noplacas
	)
)

SELECT *
FROM control_citas_total_unificadas
GO

/****** Object:  View [dbo].[vfv_productividad_tecnico]    Script Date: 2/11/2025 9:59:36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[vfv_productividad_tecnico]
as
select
c.id,
case when c.fecha_Hora_ini_Oper is null then a.fecha else c.fecha_Hora_ini_Oper end as  fecha,
anio = year(case when c.fecha_Hora_ini_Oper is null then a.fecha else c.fecha_Hora_ini_Oper end ),
mes = right('0'+cast(month(case when c.fecha_Hora_ini_Oper is null then a.fecha else c.fecha_Hora_ini_Oper end) as varchar), 2),
dia = right('0'+cast(day(case when c.fecha_Hora_ini_Oper is null then a.fecha else c.fecha_Hora_ini_Oper end) as varchar), 2),
--c.tipoCliente as operacion,
--c.tipoCliente as operacion,
a.NOORDEN,
a.noplacas,
case when a.HoraRetiro is null then 0 else 1 end as entregado,
datediff(minute,
	(select MIN(b.fecha_hora_ini_oper) from Tb_CITAS b where b.id_hd = a.id_hd and b.tipoCliente in ('servicio','mantenimiento','reparacion') and b.id = c.id),
	(select MAX(b.fecha_hora_fin_oper) from Tb_CITAS b where b.id_hd = a.id_hd and b.tipoCliente in ('servicio','mantenimiento','reparacion') and b.id = c.id)
) as T_Taller,
c.status,
Encargado= case when c.idtecnico = '' then 'Incompleto' else isnull((select top 1 NOMBRE_EMPLEADO from tb_TECNICOS where  ID_EMPLEADO= idTecnico), 'No show') end 
from Tb_CITAS_HEADER_NW a, Tb_CITAS c
where datediff(year, a.fecha,getdate())<=2 and a.fecha<=getdate()
and a.id_hd = c.id_hd
and c.serviciocapturado<>'Lavado'
and Horallegada is not null 
and c.noOrden<>0
and c.Status='Terminado'
and c.fecha<=getdate()-1
--and a.NOORDEN not in (0)
--and c.fecha_Hora_ini_Oper is not null
GO

/****** Object:  View [dbo].[vv_reporte_uso_global]    Script Date: 2/11/2025 9:59:36 PM ******/
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
		dbo.fv_control_citas_unicas
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

	 aux_tb_ordenes_programadas AS (
	SELECT 
		noorden,
		fecha,
		ROW_NUMBER() OVER (PARTITION BY noorden ORDER BY fecha DESC) AS rn
	FROM 
		dbo.v_tiempo_operacion_real
	WHERE
		YEAR(fecha) >= 2024
        AND tecnico NOT IN ('Inválido')
),

	 tb_ordenes_programadas AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_programadas
	FROM 
		aux_tb_ordenes_programadas
	WHERE
		rn = 1
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

	 tb_ordenes_finalizadas_tecnico AS (
	SELECT 
		YEAR(fecha) AS anio,
		MONTH(fecha) AS mes,
		COUNT(*) AS ordenes_finalizadas_tecnico
	FROM 
		dbo.vfv_productividad_tecnico
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
		[capnet-apps-hyundai-satelite].dbo.informacion
	WHERE
		no_orden IN (SELECT a.no_orden FROM [capnet-apps-hyundai-satelite].dbo.actividades_tecnico a)
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
		[capnet-apps-hyundai-satelite].dbo.informacion
	WHERE
		no_orden IN (SELECT a.no_orden FROM [capnet-apps-hyundai-satelite].dbo.log_envios a)
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
		'Hyundai Pedregal' AS concesionario,
		'Alden' AS grupo,
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
		o.encuestas_realizadas,
		p.ordenes_finalizadas_tecnico
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
	LEFT JOIN
		tb_ordenes_finalizadas_tecnico p ON a.anio = p.anio AND a.mes = p.mes
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
		encuestas_realizadas = CASE WHEN encuestas_realizadas > 0 THEN ISNULL(encuestas_realizadas, 0) ELSE 0 END,
		ordenes_finalizadas_tecnico = CASE WHEN ordenes_finalizadas_tecnico > 0 THEN ISNULL(ordenes_finalizadas_tecnico, 0) ELSE 0 END
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
	asignacion_trabajo = CASE WHEN total_ordenes > 0 THEN ISNULL((ordenes_programadas * 100.0) / total_ordenes, 0) ELSE 0 END,
	ordenes_finalizadas_tecnico,
	manejo_estatus_ordenes_servicio_tecnico = CASE WHEN ordenes_programadas > 0 THEN ISNULL((ordenes_finalizadas_tecnico * 100.0) / ordenes_programadas, 0) ELSE 0 END,
	manejo_contenido_ordenes_servicio_tecnico = CASE WHEN ordenes_enlazadas > 0 THEN ISNULL((ordenes_finalizadas_tecnico * 100.0) / ordenes_enlazadas, 0) ELSE 0 END,
	ordenes_finalizadas_lavado,
	manejo_estatus_ordenes_servicio_lavado = CASE WHEN ordenes_programadas > 0 THEN ISNULL((ordenes_finalizadas_lavado * 100.0) / ordenes_programadas, 0) ELSE 0 END,
	manejo_contenido_ordenes_servicio_lavado = CASE WHEN ordenes_enlazadas > 0 THEN ISNULL((ordenes_finalizadas_lavado * 100.0) / ordenes_enlazadas, 0) ELSE 0 END,
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
