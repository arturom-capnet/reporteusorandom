USE [TableroV4_Hyundai_LomasVerdes]
GO
/****** Object:  View [dbo].[vfv_productividad_tecnico]    Script Date: 2/11/2025 10:44:09 PM ******/
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
