USE [TableroV4_Hyundai_LomasVerdes]
GO
/****** Object:  View [dbo].[v_control_citas]    Script Date: 2/11/2025 10:45:13 PM ******/
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
