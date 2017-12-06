/**
 *@name Consulta.java
 * 
 *@version 1.0 
 * 
 *@date 30-03-2017
 * 
 *@author EA7129
 * 
 *@copyright Cencosud. All rights reserved.
 */
package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
/**
 * @description 
 */
public class LoadDataVentaEnVerde {
	
	private static final int DIFF_HOY_FECHA_INI = 1;
	private static final int DIFF_HOY_FECHA_FIN = 0;
	//private static final int FORMATO_FECHA_0 = 0;
	//private static final int FORMATO_FECHA_1 = 1;
	//private static final int FORMATO_FECHA_3 = 3;
	//private static final String RUTA_ENVIO = "C:/Share/Inbound/TRAZABILIDAD";

	private static BufferedWriter bw;
	private static String path;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {
			
			

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}
	
	private static void crearTxt(Map<String, String> mapArguments) {
		// TODO Auto-generated method stub
		//Connection dbconnOracle = crearConexionOracle();
		Connection dbconnMysql = crearConexionMysql();

		//File file1              = null;
		BufferedWriter bw       = null;
		PreparedStatement pstmt = null;
		//StringBuffer sb         = null;
		String sFechaIni        = null;
		String sFechaFin        = null;
		String iFechaIni2           							= null;
		String iFechaFin2          								= null;
		String iFechaIni3           							= null;
		String iFechaFin3          								= null;
		
		Date now2 = new Date();
		SimpleDateFormat ft2 = new SimpleDateFormat ("dd/MM/YY hh:mm:ss");
		String currentDate2 = ft2.format(now2);
		info("Inicio Programa: " + currentDate2 + "\n");

		try {

			try {
				
				Date now3 = new Date();
				SimpleDateFormat ft3 = new SimpleDateFormat ("YYYYMMdd");
				String iFechaFin = ft3.format(now3);
				info("iFechaFin: " + iFechaFin + "\n");
				
				
				
				//sFechaIni = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_INI);
				//sFechaFin = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);
				sFechaIni = restarDias(iFechaFin, DIFF_HOY_FECHA_INI);
				sFechaFin = restarDias(iFechaFin, DIFF_HOY_FECHA_FIN);

				
				iFechaIni2 = restarDias2(iFechaFin, 1);
				iFechaFin2 = restarDias2(iFechaFin, 0);
				
				iFechaIni3 = restarDias2(iFechaFin, 1);
				iFechaFin3 = restarDias2(iFechaFin, 0);
				
				//sFechaIni = "29-03-2017";
				//sFechaFin = "29-03-2017";
				info("sFechaIni: " + sFechaIni + "\n");
				info("sFechaFin: " + sFechaFin + "\n");
				
				info("sFechaIni2: " + iFechaIni2 + "\n");
				info("sFechaFin2: " + iFechaFin2 + "\n");
				
				info("sFechaIni3: " + iFechaIni3 + "\n");
				info("sFechaFin3: " + iFechaFin3 + "\n");
			}
			catch (Exception e) {

				info("error: " + e);
			}
			//file1                   = new File(path + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			//file1                   = new File(RUTA_ENVIO + "/" + sFechaIni + "_" + sFechaFin + "11111.txt");
			
			Date nowPro = new Date();
		    SimpleDateFormat ftPro = new SimpleDateFormat ("YYYY-MM-dd HH:mm:ss");
		    String currentDatePro = ftPro.format(nowPro);
			
			
			Thread.sleep(60);
		    System.out.println("Pausa para Insertar  HORARIO_VTAV_ACT_STOCK sleep(60 seg)");            
		    agregarRegistroProcesoStock(dbconnMysql, currentDatePro, "1");
		  
			
			
			
			Thread.sleep(60);
			System.out.println("Pausa para Eliminar ecommerce_soporte_venta sleep(60 seg)");	
			info("Pausa para Eliminar ecommerce_soporte_venta sleep(60 seg)");
			//elimnarCuadratura(dbconnOracle2,"DELETE FROM  ECOMMERCE_SOPORTE_VENTA where 1 = 1 AND FECTRANTSL >= '"+iFechaIni+"'  AND FECTRANTSL <= '"+iFechaIni+"'");
			elimnarCuadratura(dbconnMysql,"DELETE FROM  cuadratura_venta_verde where 1 = 1 AND FECHA_CREACION_ORDEN >= '"+iFechaIni3+" 00:00:00'  AND FECHA_CREACION_ORDEN <= '"+iFechaFin3+" 23:59:59' "); // 1 = detalleOcEom
			
			//loadData(dbconnMysql,"LOAD DATA INFILE 'C:\\\\Share\\\\Inbound\\\\TRAZABILIDAD\\\\20170926_20170926.txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (ID,NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
			//loadData(dbconnMysql,"LOAD DATA INFILE 'C:\\\\Share\\\\Inbound\\\\TRAZABILIDAD\\\\"+"/" + sFechaIni + "_" + sFechaFin + ".txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (ID,NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
						
			Thread.sleep(60);
			//ejecutarPaso1(dbconnMysql, "LOAD DATA INFILE 'C:\\\\Share\\\\Inbound\\\\TRAZABILIDAD\\\\20170926_20170926.txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
			//ejecutarPaso1(dbconnMysql, "LOAD DATA INFILE 'C:\\\\Share\\\\Inbound\\\\TRAZABILIDAD\\\\"+"/" + sFechaIni + "_" + sFechaFin + ".txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
			//ejecutarPaso1(dbconnMysql, "LOAD DATA LOCAL 'C:\\\\Users\\\\Public\\\\carga_archivos\\\\Carga_TRAZABILIDAD\\\\trazabilidad.txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
			ejecutarPaso1(dbconnMysql, "LOAD DATA LOCAL INFILE 'C:\\\\Share\\\\Inbound\\\\VentaEnVerde\\\\Teradata\\\\ventaenverde.txt' INTO TABLE cuadratura_venta_verde  FIELDS TERMINATED BY ';'  ENCLOSED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES (SOLICITUDORIGINAL,N_ORDEN_DISTRIBU,FECHA_CREACION_ORDEN,PONUMB,EX14ERROR,DESPA,TIPO_ESTADO,TIPO_RELACION, RELNUM, INUMBR, RELBL5, LOGENCONTRADOENJDA) ");
			
			
			
			info("Archivos creados." + "\n");
		}
		catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnMysql, pstmt, bw);
		}
		info("Fin Programa: " + currentDate2 + "\n");
	}
	/**
	 * Metodo que agregar un registro para monitorear los procesos a cada hora 
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param String, fecha actual del sistema
	 * @return String, estado actual de la tabla
	 * 
	 */
	private static void agregarRegistroProcesoStock(Connection dbconnection, String fecha, String estado){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		try {
			info("[Ini Metodo eliminarTablaRev3Pojob]");
			
			sb = new StringBuffer();
			//sb.append("DELETE FROM BJPROYEC.Q_40REV3 WHERE POJOB = 'PO_CANCEL'");
			sb.append("insert into horario_vtav_act_stock  (FECHA, ESTADO) VALUES ('"+fecha+"', '"+estado+"')");
			info("[sb]:"+sb);
			pstmt        = dbconnection.prepareStatement(sb.toString());
			info("Registros agregADOS horario_vtav_act_stock: "+pstmt.executeUpdate());	

			info("[Fin Metodo eliminarTablaRev3Pojob]");
			
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[Metodo eliminarTablaRev3Pojob]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}
	
	/**
	 * Metodo que resta dias 
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias2(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyy-MM-dd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	/**
	 * Metodo que ejecuta la eliminacion de registros en una tabla
	 * 
	 * @param Connection,
	 *            conexion de las base de datos
	 * @param String,
	 *            query para la eliminacion
	 * @return
	 */

	private static void elimnarCuadratura(Connection dbconnection, String sql) {
		PreparedStatement pstmt = null;
		info("sql " + sql);
		try {
			pstmt = dbconnection.prepareStatement(sql);
			System.out.println("registros elimnados : " + pstmt.executeUpdate());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cerrarTodo(null, pstmt, null);
		}

	}
	
	
	/**
	 * Metodo que ejecuta la eliminacion de registros en una tabla
	 * 
	 * @param Connection,
	 *            conexion de las base de datos
	 * @param String,
	 *            query para la eliminacion
	 * @return
	 */
	/*
	private static void loadData(Connection dbconnection, String sql) {
		PreparedStatement pstmt = null;
		try {
			info("loadData sql : " + sql + "\n");
			pstmt = dbconnection.prepareStatement(sql);
			info("loadData: " + pstmt.executeUpdate() + "\n");
			System.out.println("loadData : " + pstmt.executeUpdate());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cerrarTodo(null, pstmt, null);
		}

	}*/
	
	/**
	 * Metodo que elimina la tabla temporal BJPROYEC.JPDNOVRET, BJPROYEC.JPDVTASDAD, BJPROYEC.JPDTOTDAD en JDA
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @return 
	 * 
	 */
	
	private static void ejecutarPaso1(Connection dbconnection, String sql) {
		info("[sql]"+ sql);
		StringBuffer sb = new StringBuffer();
		PreparedStatement pstmt = null;
		try {
			info("[Ini paso 1]");

			sb = new StringBuffer();
			//sb.append("LOAD DATA INFILE 'C:\\\\Share\\\\Inbound\\\\TRAZABILIDAD\\\\20170926_20170926.txt' INTO TABLE `ecommerce_soporte_venta` FIELDS TERMINATED BY ';' ENCLOSED BY '' LINES TERMINATED BY  "+"'\\n'"+" IGNORE 1 LINES (NUMORDEN,CODIGO_DESPACHO,FECTRANTSL,NUMCTLTSL,NUMTERTSL,NUMTRANTSL,PXQ,SKU,LOLOCA,ESTORDEN,TIPO_ESTADO_OC,TIPVTA,TIPOPAG,TIPO_ESTADO,TIPO_RELACION) ");
			sb.append(sql);
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			info("registros cargados de ecommerce_soporte_venta: " + pstmt.executeUpdate());

			info("[Fin paso 1]");

		} catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso1]Exception:" + e.getMessage());
		} finally {
			cerrarTodo(null, pstmt, null);
		}
	}

	/**
	 * Metodo de conexion para MEOMCLP 
	 * 
	 * @return void,  no tiene valor de retorno
	
	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			//Shareplex
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			//El servidor g500603sv0zt corresponde a ProducciÃ³n. por el momento
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@172.18.163.15:1521/XE", "kpiweb", "kpiweb");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	} */
	
	
	/**
	 * Metodo de conexion para MEOMCLP 
	 * 
	 * @return void,  no tiene valor de retorno
	 */
	private static Connection crearConexionMysql() {

		Connection dbconnection = null;

		try {

			Class.forName("com.mysql.jdbc.Driver");
			//dbconnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/trazabilidad", "root", "admin");
			dbconnection = DriverManager.getConnection("jdbc:mysql://txddb.cqjxibnc2qub.us-west-2.rds.amazonaws.com/trazabilidad", "adminuser", "s4cc2ss943");
			//dbconnection = DriverManager.getConnection("jdbc:mysql://localhost/trazabilidad", "jgarrido", "admin");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}



	/**
	 * Metodo que cierra la conexion, Procedimintos,  BufferedWriter
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param PreparedStatement, Objeto que representa una instrucción SQL precompilada. 
	 * @return retorna
	 * 
	 */
	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}


	/**
	 * Metodo que muestra informacion 
	 * 
	 * @param String, texto a mostra
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:" + e.getMessage());
		}
	}


	/**
	 * Metodo que resta dias 
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	/*
	private static String formatDate(Date fecha, int iOptFormat) {

		String sFormatedDate = null;
		String sFormat = null;

		try {

			SimpleDateFormat df = null;

			switch (iOptFormat) {

			case 0:
				sFormat = "dd/MM/yy HH:mm:ss,SSS";
				break;
			case 1:
				sFormat = "dd/MM/yy";
				break;
			case 2:
				sFormat = "dd/MM/yy HH:mm:ss";
				break;
			case 3:
				sFormat = "yyyy-MM-dd HH:mm:ss,SSS";
				break;
			}
			df = new SimpleDateFormat(sFormat);
			sFormatedDate = df.format(fecha != null ? fecha:new Date(0));

			if (iOptFormat == 0 && sFormatedDate != null) {

				sFormatedDate = sFormatedDate + "000000";
			}
		}
		catch (Exception e) {

			info("[formatDate]Exception:"+e.getMessage());
		}
		return sFormatedDate;
	}
	*/

}
