USE [TableroV4_Hyundai_LomasVerdes]
GO
/****** Object:  View [dbo].[vv_control_citas]    Script Date: 2/11/2025 10:43:01 PM ******/
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
