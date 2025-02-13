USE [TableroV4_Hyundai_Pedregal]
GO
/****** Object:  StoredProcedure [dbo].[cargar_kpis]    Script Date: 2/11/2025 10:00:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[cargar_kpis] as -- exec cargar_kpis

		set datefirst 1;

		update tb_citas_header_nw
		set fechaHoraPromesa = fecha_hora_com
		where fechaHoraPromesa is null and Fecha_hora_com is not null;

		drop table fv_productividad_total
		select * into fv_productividad_total from vfv_productividad_total;

		drop table v_citas_show_noshow
		select * into v_citas_show_noshow from vv_citas_show_noshow;

		drop table v_hit_bateo
		select * into v_hit_bateo from vv_hit_bateo;

		drop table V_TIEMPO_OPERACION_REAL
		select * into V_TIEMPO_OPERACION_REAL from vV_TIEMPO_OPERACION_REAL;

		drop table V_TIEMPO_OPERACION_REAL_TECNICO
		select * into V_TIEMPO_OPERACION_REAL_TECNICO from vV_TIEMPO_OPERACION_REAL_TECNICO;

		drop table fv_lavado_kpi
		select * into fv_lavado_kpi from v_lavado_kpi;

		drop table fv_calidad_kpi
		select * into fv_calidad_kpi from v_calidad_kpi;

		drop table v_pull_sys
		select * into v_pull_sys from vv_pull_sys;

		drop table v_pull_sys_detalle
		select * into v_pull_sys_detalle from vv_pull_sys_detalle;

		drop table v_pull_sys_ws
		select * into v_pull_sys_ws from vv_pull_sys_ws;

		drop table v_dif_entrega
		select * into v_dif_entrega from vv_dif_entrega;

		drop table v_uso_tableros_asesor_anfitrion
		select * into v_uso_tableros_asesor_anfitrion from vv_uso_tableros_asesor_anfitrion;

		drop table fv_kpi_promedio_tiempos_express
		select * into fv_kpi_promedio_tiempos_express from v_kpi_promedio_tiempos_express;

		insert into Tabla_citas_operaciones_kpi
		select * from dbo.v_asesores_operacion_citas b with(nolock)
		where isnull(b.NUMCITA,0) not in(select ISNULL(z.numcita,0)
												from Tabla_citas_operaciones_kpi z with(nolock));

		drop table fv_control_citas
		select * into fv_control_citas from v_control_citas;

		drop table fv_control_citas_unicas
		select * into fv_control_citas_unicas from vv_control_citas;

		drop table fv_productividad_calidad
		select * into fv_productividad_calidad from vfv_productividad_calidad where T_Esp_Calidad >= '0';
		
		drop table fv_productividad_paperles
		select * into fv_productividad_paperles from vfv_productividad_paperles;
		
		drop table fv_productividad_paperles_1
		select * into fv_productividad_paperles_1 from vfv_productividad_paperles_1;

		drop table fv_reporte_uso_global
		select * into fv_reporte_uso_global from vv_reporte_uso_global;
GO
