import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormatSymbols;
import java.util.Properties;
import java.util.Scanner;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Locale;
public class Main {
	

	private static final String DB_DRIVER = "org.postgresql.Driver";
	private static final String imp_msg="[Import from csv]";
	private static final String exp_msg="[Export to CSV]";
	private static final String mani_msg="[Manipulate Data]";
	
	public static boolean isNumeric(String str){
	    try
	    {
	      double d = Double.parseDouble(str);
	    }
	    catch(NumberFormatException nfe)
	    {
	      return false;
	    }
	    return true;
	}
	
	public static String get_date(String[] date_arr)
	{
		String[] convert= date_arr;
		String month = new DateFormatSymbols(new Locale("en", "US")).getShortMonths()[Integer.valueOf(convert[0].trim())-1].toString();
		String day = convert[1].trim();
		String year = convert[2].trim();
		String date_data="'"+day+" "+month+" "+year+"'";
		return date_data;
	}
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException, FileNotFoundException {
		
		String DB_CONNECTION_URL = "jdbc:postgresql"; //postgresql://127.0.0.1/exam1

		FileInputStream in = new FileInputStream("connection.txt");
		BufferedReader fin = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
		Scanner scan = new Scanner(fin);
		Scanner scan_sysin = new Scanner(System.in);
		int choice = 0;
		
		String ip= scan.nextLine().split(":")[1].trim();
		String db= scan.nextLine().split(":")[1].trim();
		String schema= scan.nextLine().split(":")[1].trim();
		String id= scan.nextLine().split(":")[1].trim();
		String pw= scan.nextLine().split(":")[1].trim();
		DB_CONNECTION_URL+="://"+ip;
		DB_CONNECTION_URL+="/"+db;
	
		Class.forName(DB_DRIVER);
		Properties connProps = new Properties();

		/* Setting Connection Info */
		connProps.setProperty("user", 		id);
		connProps.setProperty("password", 	pw);
		/* Connect! */
		Connection conn = DriverManager.getConnection(DB_CONNECTION_URL, connProps);
		Statement st = conn.createStatement();
		while(choice!=4)
		{
			System.out.print("Please input the instruction number (1: Import from CSV, 2: Export to CSV, 3: Manipulate Data, 4: Exit) : ");
			choice = scan_sysin.nextInt();
			switch(choice){
				case 1:
					System.out.println(imp_msg);
					System.out.print("Please specify the filename for table description :");
					String table = scan_sysin.next().trim();
					in = new FileInputStream(table);
					fin = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
					scan = new Scanner(fin);
					int col_size =0;
					String factor_ck;
					String fc_vl; //factor value
					ArrayList<String> CreateTableSQL = new ArrayList<String>();
					ArrayList<String> col_list = new ArrayList<String>();
					ArrayList<String> col_type_list = new ArrayList<String>();
					String table_name="";
					String sql="";
					
					while (scan.hasNextLine())
					{
						String line = scan.nextLine();
						
						factor_ck=line.split(":")[0].trim();
						fc_vl=line.split(":")[1].trim();
						
						if (factor_ck.equals("Name"))
						{
							CreateTableSQL.add("CREATE TABLE "+"\""+fc_vl+"\""+" (");
							table_name=fc_vl;
						}
						else if (factor_ck.substring(0,2).equals("Co") && factor_ck.length()<10)
						{
							CreateTableSQL.add("\""+fc_vl+"\"");
							col_list.add(fc_vl);
						}						
						else if (factor_ck.substring(0,2).equals("Co") && factor_ck.length()>10)
						{
							if (scan.hasNextLine()==false)
							{
								CreateTableSQL.add(fc_vl+")");
								col_type_list.add(fc_vl);
							}
							else
							{
								CreateTableSQL.add(fc_vl+",");
								col_type_list.add(fc_vl);
							}
						}
						else if (factor_ck.toUpperCase().equals("PK") )
						{
							String[] pk_arr = fc_vl.split(",");
							String data ="";
							for (int i=0; i<pk_arr.length;i++)
							{
								data+="\""+pk_arr[i].trim()+"\"";
								if (i<pk_arr.length-1)
								{
									data+=",";
								}
							}
							CreateTableSQL.add(" primary key ("+ data+")");
						}
						else if (factor_ck.toUpperCase().equals("NOT NULL"))
						{
							String[] fc_vl_arr = fc_vl.split(",");
							for (int i=0; i<fc_vl_arr.length; i++)
							{
								fc_vl_arr[i] = fc_vl_arr[i].trim();
								
								for (int j=0; j<CreateTableSQL.size();j++)
								{
									if (CreateTableSQL.get(j).equals("\""+fc_vl_arr[i]+"\""))
									{
										String added=" "+ CreateTableSQL.get(j+1).substring(0,CreateTableSQL.get(j+1).length()-1)
												+" not null";
										if (j+1<CreateTableSQL.size()-1)
										{
											added+=",";
										}
										CreateTableSQL.remove(j+1);
										CreateTableSQL.add(j+1, added);
									}
								}
							}
						}
					if (scan.hasNextLine()==false)
					{
						CreateTableSQL.add(")");
					}
					}
					
					for (String item: CreateTableSQL)
					{
						sql+=item;
					}
					//update create table statement to connected db
					st.executeUpdate(sql);
					
					System.out.println("Table is newly created as described in the file.");
					System.out.print("Please specify the CSV filename :");
					
					String csv = scan_sysin.next().trim();
					
					in = new FileInputStream(csv);
					fin = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
					scan = new Scanner(fin);
					String[] csv_col= scan.nextLine().split(",");
					
					if (csv_col.length != col_list.size())
					{
						System.out.println("Data import failure. (The number of columns does not match between the table description and the CSV file.)");
						break;
					}
					
					
					int[] csv_col_order= new int[csv_col.length];
					ArrayList<String> fail_data_list = new ArrayList<String>();
					String insert_sql="INSERT INTO "+"\""+table_name+"\""+" VALUES(";
					int count_line=1;
					int suc_num=0;
					int fail_num=0;
					for (int i=0; i<col_list.size();i++)
					{
						for (int j=0; j<col_list.size();j++)
						{
							if (csv_col[i].trim().equals(col_list.get(j)))
							{
								csv_col_order[i]=j;
							}
						}
					}
					while(scan.hasNextLine())
					{
						String row_data= scan.nextLine();
						String[] insert_data= new String[col_list.size()];
						String[] row_data_arr=row_data.split(",");
						String[] sql_data_order = new String[col_list.size()];
						for (int i=0; i<insert_data.length;i++)
						{
							if (i<row_data_arr.length)
							{
								insert_data[i]=row_data_arr[i];
							}
							else
							{
								insert_data[i]="null";
							}
						}
						
						for (int k=0; k<col_list.size();k++)
						{
							
							if (col_type_list.get(csv_col_order[k]).substring(0,3).equals("var") ||
									col_type_list.get(csv_col_order[k]).substring(0,3).equals("cha") ||
									col_type_list.get(csv_col_order[k]).substring(0,3).equals("tim"))
							{
								sql_data_order[csv_col_order[k]]="'"+insert_data[k].trim()+"'";
							}
							else if (col_type_list.get(csv_col_order[k]).substring(0,3).equals("dat"))
							{
								String date_data=get_date(insert_data[k].trim().split("/"));
								sql_data_order[csv_col_order[k]]=date_data;
							}
							else
							{
								sql_data_order[csv_col_order[k]]=insert_data[k].trim();
							}
						}
						for (int p=0; p<col_list.size();p++)
						{
							insert_sql+=sql_data_order[p];
							if (p<col_list.size()-1)
							{
								insert_sql+=",";
							}
						}
						insert_sql+=")";
						count_line++;
						System.out.println(insert_sql);
						try{
						 st.executeUpdate(insert_sql);
						 suc_num++;
						}
						catch (SQLException e)
						{
							String fail_data=Integer.toString(count_line)+":"+row_data;
							fail_data_list.add(fail_data);
							fail_num++;
						}
						insert_sql="INSERT INTO "+"\""+table_name+"\""+" VALUES(";
						
					}
					System.out.println("Data import completed (Insertion Success : "
					+Integer.toString(suc_num)+", Insertion Failure : "+Integer.toString(fail_num)+" )");
					if (fail_num >0)
					{
						for (int i=0; i<fail_num;i++)
						{
							String[] fail=fail_data_list.get(i).split(":");
							String fail_line = fail[0];
							String fail_st =  fail[1];
							System.out.println("Failed tuple : "+fail_line+" line in CSV - "+fail_st);
						}
					}
					
					break;
				case 2:
					System.out.println(exp_msg);
					System.out.print("Please specify the table name : ");
					String out_table = scan_sysin.next().trim();
					System.out.print("Please specify the CSV filename : ");
					String out_csv = scan_sysin.next().trim();
			    	File outF=new File(out_csv);
			    	try{
			    	outF.createNewFile();
			    	BufferedWriter fout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outF,true),StandardCharsets.UTF_8));
					ResultSet rs = st.executeQuery("SELECT * FROM "+"\""+out_table+"\"");
					int rs_col_num = rs.getMetaData().getColumnCount();
						while (rs.next()){
							String rs_tuple="";
							for (int k=1; k<=rs_col_num;k++)
							{
								rs_tuple+=rs.getString(k);
								if (k<rs_col_num)
								{
									rs_tuple+=",";
								}
							}
							System.out.println(rs_tuple);
							fout.write(rs_tuple);
							fout.write('\n');
						
						}
					fout.close();
					}
					catch (IOException e)
					{
						
					}
			    	System.out.println("Data export completed");
			    	
					break;
				  case 3: 
					  System.out.println(mani_msg);
					  int num=0;
					  while(true){
							System.out.print("Please input the instruction number (1: Show Tables, 2: Describe Table, 3: Select, 4: Insert, 5: Delete, 6: Update, 7: Drop Table, 8: Back to main) :");
							Scanner userinput = new Scanner(System.in);
							num = userinput.nextInt();
							ResultSet rs;
							
							if(num==8)break;
							else if(num==1){
								System.out.println("=================");
								System.out.println("Table List");
								System.out.println("=================");
								rs=st.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
								while (rs.next()) {
								    System.out.println(rs.getString(1));
								}
								System.out.println();
							}//end of num==1
							else if(num==2){
								Scanner input = new Scanner(System.in);
								System.out.println("Please specify the table name :");
								String tableName = input.nextLine();
								rs=st.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
								boolean exist = false;
								while (rs.next()) {
								    if(rs.getString(1).equals(tableName)){
								    	exist=true;
								    }
								}
								if(exist==true){
									rs=st.executeQuery("SELECT column_name,data_type,character_maximum_length,numeric_precision,numeric_scale FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + tableName+"'");
									System.out.println("====================================================================================");
									System.out.println("Column Name | Data Type | Character Maximum Length(or Numeric Precision and Scale)");
									System.out.println("====================================================================================");
									while(rs.next()){
										System.out.print(rs.getString(1) + ", ");
									    System.out.print(rs.getString(2));
									    if(rs.getString(2).equals("integer") || rs.getString(2).equals("numeric")){
									    	System.out.print(", ("+rs.getString(4)+","+rs.getString(5)+")");
									    }else if(rs.getString(2).equals("date") || rs.getString(2).equals("time")){
									    	System.out.print("");
									    }else{
									    	System.out.print(", "+rs.getString(3));
									    }
									    System.out.println();
									}	
								}else{
									System.out.println("No such Table");
								}
							}//end of num==2
							else if(num==3){
								Scanner input = new Scanner(System.in);
								String query="";
								String selectQuery="SELECT ";
								System.out.println("Please specify the table name :");
								String tableName = input.nextLine().trim();
								System.out.println("Please specify columns which you want to retrieve (ALL : *) :");
								String columnNames = input.nextLine().trim();
								String[] columns = null;
								if(columnNames.equals("*")){
									selectQuery+="*";
								}else{
									columns = columnNames.split(",");
									for(int i=0;i<columns.length;i++){
										if(i==columns.length-1){
											selectQuery+="\""+columns[i].trim()+"\"";
										}else{
											selectQuery+="\""+columns[i].trim()+"\",";
										}
									}
								}
								String whole="";
								boolean check = true;
								System.out.println("Please specify the column which you want to make condition (Press enter : skip) :");
								while(check){
									String newCon="";
									String[] conditions = {"=", ">", "<", ">=", "<=", "!=", "LIKE"};
									String columnName = input.nextLine().trim();
									if(!columnName.equals("")){
										System.out.println("Please specify the condition (1: =, 2: >, 3: < , 4: >=, 5: <=, 6: !=, 7: LIKE) :");
										int conditionNum =input.nextInt()-1;
										newCon="\""+columnName+"\""+conditions[conditionNum];
										System.out.println("Please specify the condition value ("+whole+newCon+" ?) :");
										rs=st.executeQuery("select * from "+"\""+tableName+"\"");
										String col_data_type=rs.getMetaData().getColumnTypeName(rs.findColumn(columnName));
										String conditionVal = input.next().trim();
										if(!col_data_type.equals("integer") && !col_data_type.equals("numeric")){
											conditionVal="'"+conditionVal+"'";
										}
										newCon += conditionVal;
										System.out.println("Please specify the condition (1: AND, 2: OR, 3: finish) :");
										int cho = input.nextInt();
										input.nextLine();
										if(cho==3){
											whole+=newCon;
											check=false;
										}
										else if(cho==1){
											newCon+=" and ";
											whole+=newCon;
											System.out.println("Please specify the column which you want to make condition :");
											check=true;
										}
										else if(cho==2){
											newCon+=" or ";
											whole+=newCon;
											System.out.println("Please specify the column which you want to make condition :");
											check=true;
										}
									}else{
										check=false;
									}
								}
								String orderBy = "";
								System.out.println("Please specify the column name for ordering (Press enter : skip) :");
								String columnOrder = input.nextLine().trim();
								String[] columnsOrderS = columnOrder.split(",");
								if(!columnsOrderS[0].equals("")){
									System.out.println("Please specify the sorting criteria (Press enter : skip) :");
									String columnSort = input.nextLine().trim();
									String[] columnSortS = columnSort.split(",");
									for(int i=0;i<columnSortS.length;i++){
										if(columnSortS[i].trim().equals("ASCEND")){
											columnSortS[i]="asc";
										}else if(columnSortS[i].trim().equals("DESCEND")){
											columnSortS[i]="desc";
										}
									}
									if(!columnSortS[0].equals("")){
										orderBy = "ORDER BY ";
										for(int i=0;i<columnsOrderS.length;i++){
											if(i==columnsOrderS.length-1){
												orderBy+="\""+columnsOrderS[i].trim()+"\""+" "+columnSortS[i];
											}else{
												orderBy+="\""+columnsOrderS[i].trim()+"\""+" "+columnSortS[i]+", ";
											}

										}
									}else{
										orderBy = "ORDER BY ";
										for(int i=0;i<columnsOrderS.length;i++){
											if(i==columnsOrderS.length-1){
												orderBy+="\""+columnsOrderS[i].trim()+"\"";
											}else{
												orderBy+="\""+columnsOrderS[i].trim()+"\""+", ";
											}

										}
									}
								}
								String whereQuery="";
								if(!whole.equals("")){
									whereQuery+="WHERE "+whole;
								}
								query += selectQuery + " FROM " + "\"" + tableName + "\" " + whereQuery +" "+ orderBy;
								System.out.println("SQL query you requested : "+query); //실제 실행되는 쿼리문 출
								try{
									rs=st.executeQuery(query);
									System.out.println("============================================================");
									int colNum;//the number of columns in a TABLE
									if(columns==null){//SELECT * 일
										rs=st.executeQuery("SELECT * FROM \""+tableName+"\"");
										ResultSetMetaData rsmd = rs.getMetaData();
										colNum = rsmd.getColumnCount();
										for (int i = 1; i <= colNum; i++){
											  String col_name = rsmd.getColumnName(i);
											  System.out.print(col_name+"|");
										}
									}else{//User가 특정한 select column을 지정했을
										colNum=columns.length;
										for(int i=0;i<columns.length;i++){
											System.out.print(columns[i].trim()+"|");
										}
									}
									System.out.println();
									System.out.println("============================================================");
									rs=st.executeQuery(query);
									int selectedRow=0;
									int cutIndex=10;
									int selIndex=0;
									while(rs.next()){
										if(selIndex==cutIndex){
											System.out.println("<Press enter>");
											input.nextLine();
											cutIndex+=10;
										}
										for(int i=1;i<=colNum;i++){
											System.out.print(rs.getString(i)+" ");
										}
										System.out.println();
										selIndex++;
										selectedRow++;
									}
									System.out.println("<"+selectedRow+" rows selected>");
								}catch (SQLException e){
									String err_msg="<error detected>";
									System.out.println(err_msg);
									System.out.println();
								}
							}//end of num==3
							else if (num==4)
							{
								Scanner insertInput = new Scanner(System.in);
								String result_msg="";
								int row_num=0;
								System.out.print("Please specify the table name : ");
								String inst_tbl_n= insertInput.nextLine().trim();
								System.out.print("Please specify all columns in order of which you want to insert : ");
								String inst_col= insertInput.nextLine().trim();
								System.out.print("Please specify values for each column : ");
								String inst_col_val= insertInput.nextLine().trim();
								String[] inst_col_list=inst_col.split(",");
								String[] inst_col_val_list=inst_col_val.split(",");
								rs=st.executeQuery("select * from "+"\""+inst_tbl_n+"\"");	 
								int col_num= rs.getMetaData().getColumnCount();
								String inst_sql="";
								boolean find = false;
								for (int i=1; i<=col_num;i++)
								{
									for (int j=0; j<inst_col_list.length;j++)
									{
										if (rs.getMetaData().getColumnName(i).equals(inst_col_list[j].trim()))
										{
											String data="";
											if (!isNumeric(inst_col_val_list[j].trim()))
											{
												data="'"+inst_col_val_list[j].trim()+"'";
											}
											else
											{
												data= inst_col_val_list[j].trim();
											}
											inst_sql+=data;
											find = true;
										}
										
									}
									if (find==false)
									{
										inst_sql+="null";
									}
									if (i<=col_num-1)
									{
										inst_sql+=",";
									}
									find = false;
								}
								
								try{
									row_num=st.executeUpdate("INSERT INTO "+"\""+inst_tbl_n+"\""+" VALUES("+inst_sql+")");
									if (row_num==1)
									{
										result_msg="<"+Integer.toString(row_num)+" row inserted>";
									}
									else
									{
										result_msg="<"+Integer.toString(row_num)+" rows inserted>";
									}
									System.out.println(result_msg);
								}
								catch (SQLException e)
								{
									result_msg="<"+Integer.toString(row_num)+" row inserted due to error>";
									System.out.println(result_msg);
								}
								
								System.out.println();
							}//end of num==4
							else if (num==5)
							{
								Scanner delInput = new Scanner(System.in);
								String result_msg="";
								String del_query_suf="";
								String del_query_suf_show="";
								String del_sign_str="";
								String del_col_con=""; //update column condition
								int row_num=0;
								int del_next_num=0;
								int del_next=0;
								int deleted_row_num=0;
								System.out.print("Please specify the table name : ");
								String del_tbl_n= delInput.nextLine().trim();
								try{
									rs=st.executeQuery("select * from "+"\""+del_tbl_n+"\"");
									while (del_next!=3)
									{
									if (del_next_num==0)
									{
										System.out.print("Please specify the column which you want to make condition (Press enter : skip) : ");
										del_col_con= delInput.nextLine();
										if (del_col_con.equals(""))
										{
											break;
										}
									}
									else
									{
										System.out.print("Please specify the column which you want to make condition : ");
										del_col_con= delInput.next().trim();
									}
									
									
									if (del_col_con!="")
									{
									if (del_next_num != 0)
									{
										while (del_col_con=="")
										{
											System.out.print("Please specify the column which you want to make condition : ");
											del_col_con= delInput.next().trim();
										}
									}
									rs.findColumn(del_col_con);
									System.out.print("Please specify the condition (1: =, 2: >, 3: < , 4: >=, 5: <=, 6: !=, 7: LIKE) : ");
									int del_sign= delInput.nextInt();
									switch(del_sign)
									{
										case 1:
											del_sign_str="=";
											break;
										case 2:
											del_sign_str=">";
											break;
										case 3:
											del_sign_str="<";
											break;
										case 4:
											del_sign_str=">=";
											break;
										case 5:
											del_sign_str="<=";
											break;
										case 6:
											del_sign_str="!=";
											break;
										case 7:
											del_sign_str="LIKE";
											break;
											
									}
									del_query_suf+="\""+del_col_con+"\""+" "+del_sign_str+" ";
									del_query_suf_show+=del_col_con+" "+del_sign_str+" ";
									System.out.print("Please specify the condition value ("+del_query_suf_show+"?) : ");
									String del_val= delInput.next().trim();
									String col_data_type=rs.getMetaData().getColumnTypeName(rs.findColumn(del_col_con));
									if (!isNumeric(del_val.trim()))
									{
										del_val="'"+del_val+"'";
									}
									else if (col_data_type.substring(0,3).equals("dat"))
									{
										String date_data=get_date(del_val.trim().split("/"));
										del_val=date_data;
									}
									del_query_suf+=del_val;
									del_query_suf_show+=del_val;
									System.out.print("Please specify the condition (1: AND, 2: OR, 3: finish) :");
									del_next= delInput.nextInt();
									if (del_next==1)
									{
										del_query_suf+=" and ";
										del_query_suf_show+=" and ";
										del_next_num++;
									}
									else if (del_next==2)
									{
										del_query_suf+=" or ";
										del_query_suf_show+=" or ";
										del_next_num++;
									}
									del_sign_str="";
									}
									}
									// while end
									if (del_query_suf!="")
									{
										System.out.println("DELETE FROM "+"\""+del_tbl_n+"\""+" WHERE "+del_query_suf);
										row_num=st.executeUpdate("DELETE FROM "+"\""+del_tbl_n+"\""+" WHERE "+del_query_suf);
									}
									else
									{
										row_num=st.executeUpdate("DELETE FROM "+"\""+del_tbl_n+"\"");
									}
									if (row_num==0 || row_num==1)
									{
										result_msg="<"+Integer.toString(row_num)+" row deleted>";
									}
									else
									{
										result_msg="<"+Integer.toString(row_num)+" rows deleted>";
									}
									System.out.println(result_msg);
									System.out.println();
								}
								catch (SQLException e)
								{
									result_msg="<error detected>";
									System.out.println(result_msg);
									System.out.println();
								}
							}//end of num==5
							else if (num==6)
							{
								Scanner updateInput = new Scanner(System.in);
								String result_msg="";
								String upd_query_pre="";
								String upd_query_suf="";
								String upd_query_suf_show="";
								String upd_sign_str="";
								String upd_col_con=""; //update column condition
								int row_num=0;
								int upd_next_num=0;
								int upd_next=0;
								int updated_row_num=0;
								System.out.print("Please specify the table name : ");
								String upd_tbl_n= updateInput.nextLine().trim();
								try{
									rs=st.executeQuery("select * from "+"\""+upd_tbl_n+"\"");
									while (upd_next!=3)
									{
									if (upd_next_num==0)
									{
										System.out.print("Please specify the column which you want to make condition (Press enter : skip) : ");
										upd_col_con= updateInput.nextLine();
										if (upd_col_con.equals(""))
										{
											break;
										}
									}
									else
									{
										System.out.print("Please specify the column which you want to make condition : ");
										upd_col_con= updateInput.next().trim();
									}
									
									
									if (upd_col_con!="")
									{
									if (upd_next_num != 0)
									{
										while (upd_col_con=="")
										{
											System.out.print("Please specify the column which you want to make condition : ");
											upd_col_con= updateInput.next().trim();
										}
									}
									rs.findColumn(upd_col_con);
									System.out.print("Please specify the condition (1: =, 2: >, 3: < , 4: >=, 5: <=, 6: !=, 7: LIKE) : ");
									int upd_sign= updateInput.nextInt();
									switch(upd_sign)
									{
										case 1:
											upd_sign_str="=";
											break;
										case 2:
											upd_sign_str=">";
											break;
										case 3:
											upd_sign_str="<";
											break;
										case 4:
											upd_sign_str=">=";
											break;
										case 5:
											upd_sign_str="<=";
											break;
										case 6:
											upd_sign_str="!=";
											break;
										case 7:
											upd_sign_str="LIKE";
											break;
											
									}
									upd_query_suf+="\""+upd_col_con+"\""+" "+upd_sign_str+" ";
									upd_query_suf_show+=upd_col_con+" "+upd_sign_str+" ";
									System.out.print("Please specify the condition value ("+upd_query_suf_show+"?) : ");
									String upd_val= updateInput.next().trim();
									String col_data_type=rs.getMetaData().getColumnTypeName(rs.findColumn(upd_col_con));
									if (!isNumeric(upd_val.trim()))
									{
										upd_val="'"+upd_val+"'";
										
									}
									else if (col_data_type.substring(0,3).equals("dat"))
									{
										String date_data=get_date(upd_val.trim().split("/"));
										upd_val=date_data;
									}
									upd_query_suf+=upd_val;
									upd_query_suf_show+=upd_val;
									
									System.out.print("Please specify the condition (1: AND, 2: OR, 3: finish) :");
									upd_next= updateInput.nextInt();
									if (upd_next==1)
									{
										upd_query_suf+=" and ";
										upd_query_suf_show+=" and ";
										upd_next_num++;
									}
									else if (upd_next==2)
									{
										upd_query_suf+=" or ";
										upd_query_suf_show+=" or ";
										upd_next_num++;
									}
									upd_sign_str="";
									}
									}
									// while end
									System.out.print("Please specify column names which you want to update : ");
									updateInput= new Scanner(System.in);
									String upd_col= updateInput.nextLine().trim();
									String[] upd_col_list =upd_col.split(",");
									System.out.print("Please specify the value which you want to put : ");
									String upd_col_val= updateInput.nextLine().trim();
									String[] upd_col_val_list = upd_col_val.split(",");
									
									for (int k=0; k<upd_col_list.length;k++)
									{
										String col_data_type=rs.getMetaData().getColumnTypeName(rs.findColumn(upd_col_list[k]));
										if (!isNumeric(upd_col_val_list[k].trim()))
										{
											upd_query_pre+="\""+upd_col_list[k].trim()+"\""+" = '"+upd_col_val_list[k].trim()+"'";
										}
										else if (col_data_type.substring(0,3).equals("dat"))
										{
											String date_data=get_date(upd_col_val_list[k].trim().split("/"));
											upd_query_pre+="\""+upd_col_list[k].trim()+"\""+"="+date_data;
										}
										else
										{
											upd_query_pre+="\""+upd_col_list[k].trim()+"\""+"="+upd_col_val_list[k].trim();
										}
										if (k<upd_col_list.length-1)
										{
											upd_query_pre+=",";
										}
										
									}
									
									System.out.println(upd_query_pre);
									System.out.println(upd_query_suf);
									
									if (upd_query_suf!="")
									{
										System.out.println("UPDATE "+"\""+upd_tbl_n+"\""+" SET "+ upd_query_pre+" WHERE "+upd_query_suf);
										row_num=st.executeUpdate("UPDATE "+"\""+upd_tbl_n+"\""+" SET "+ upd_query_pre+" WHERE "+upd_query_suf);
									}
									else
									{
										System.out.println("UPDATE "+"\""+upd_tbl_n+"\""+" SET "+ upd_query_pre);
										row_num=st.executeUpdate("UPDATE "+"\""+upd_tbl_n+"\""+" SET "+ upd_query_pre);
									}
									if (row_num==1 || row_num==0)
									{
										result_msg="<"+Integer.toString(row_num)+" row updated>";
									}
									else
									{
										result_msg="<"+Integer.toString(row_num)+" rows updated>";
									}
									System.out.println(result_msg);
									System.out.println();
								}
								catch (SQLException e)
								{
									result_msg="<error detected>";
									System.out.println(result_msg);
									System.out.println();
								}
							}//end of num==6
							else if (num==7)
							{
								Scanner dropInput = new Scanner(System.in);
								System.out.print("Please specify the table name : ");
								String drop_tbl_n= dropInput.next().trim();

								System.out.print("If you delete this table, it is not guaranteed to recover again. Are you sure you want to delete this table (Y: yes, N, no)? ");
								String drop_choi= dropInput.next().trim();

								while (!"Y".equals(drop_choi) && !"N".equals(drop_choi)){
									System.out.println("Wrong Input, please enter either Upper-case letter Y or N");
									System.out.print("If you delete this table, it is not guaranteed to recover again. Are you sure you want to delete this table (Y: yes, N, no)? ");
									drop_choi= dropInput.next().trim();
								}
								if (drop_choi.equals("Y"))
								{
									st.execute("drop table "+"\""+drop_tbl_n+"\"");
									System.out.println("<The table "+drop_tbl_n+" is deleted>");
								}
								else if (drop_choi.equals("N"))
								{
									System.out.println("<Deletion canceled>");
								}
								System.out.println();
							}//end of num==7
					 }//end of case 3
					break;
				case 4:
					System.out.println("Program Terminated!");
					System.exit(0);
					break;
			}
			System.out.println("");

		}
		
    }	
}

