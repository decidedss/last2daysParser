import config.Config;

import java.net.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Main {

    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException {

        //connect to db
        String JDBC_DRIVER = "org.postgresql.Driver";
        Class.forName("org.postgresql.Driver");

        Connection con = null;
        Statement stmt = null;
        try {
            con = DriverManager.getConnection(Config.dbURL, Config.uName, Config.uPass);
            con.setAutoCommit(false);
            stmt = con.createStatement();

            //truncate old data
            stmt.executeUpdate("TRUNCATE " + Config.tableName);
            con.commit();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        //data to be retrieved
        List<String> urls = new ArrayList<String>();
        urls.add("http://penteli.meteo.gr/stations/amyntaio/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/thessaloniki/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/veroia/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/serres/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/giannitsa/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/grevena/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/florina/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/ptolemaida/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/kastoria/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/drama/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/xanthi/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/orestiada/downld02.txt");
        urls.add("http://penteli.meteo.gr/stations/rizomata/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/mavropigi/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/ardassa/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/vlasti/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/variko/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/seli/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/veroia/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/vegoritida/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/kerasia/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/kaimaktsalan/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/3-5pigadia/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/dion/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/sindos/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/neamichaniona/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/polygyros/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/kassandreia/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/kerkini/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/lagadas/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/nevrokopi/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/mikrokampos/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/fotolivos/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/paranesti/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/neaperamos/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/thasos/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/alexandroupolis/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/didymoteicho/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/metaxades/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/notiopedio/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/vlasti/downld02.TXT");
        urls.add("http://penteli.meteo.gr/stations/eleftheroupoli/downld02.TXT");

        for(String url : urls){

            System.out.println(url);

            //get station
            String station = url.split("\\/+")[3];

            //get data
            URL oracle = new URL(url);
            URLConnection yc = oracle.openConnection();
            BufferedReader br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

            String prev = "";

            Map<Integer, String> indexStr = new HashMap<Integer, String>();

            for (String next = "", line = br.readLine(); line != null; line = next) {
                next = br.readLine();

                if (next != null) {
                    if(next.startsWith("-----------")){
                        int index = 0, start = 0;
                        String fstr = "";
                        for (char ch : prev.toCharArray()) {
                            String sstr = "";

                            if (Character.isLetter(ch)) {
                                start = 1;
                                fstr += ch;
                            }
                            if (Character.isSpaceChar(ch) & start == 1) {
                                start = 0;
                                sstr = "";

                                for (int i = index - 1; i > 0; i--) {
                                    if (Character.isSpaceChar(line.charAt(i)))
                                        break;
                                    sstr += line.charAt(i);

                                }

                                indexStr.put(line.substring(0, index).trim().split("\\s+").length, fstr + " " + reverse(sstr));
                                fstr = "";
                            }
                            index++;
                        }

                        String[] data = line.trim().split("\\s+");

                        for(int i = 1; i <= data.length; i++){
                            if(!indexStr.containsKey(i))
                                indexStr.put(i, data[i-1]);
                        }
                    }
                }

                prev = line;
            }

            int day = getKeyByValue(indexStr, "Date");
            int time = getKeyByValue(indexStr, "Time");
            int tempOut = getKeyByValue(indexStr, "Temp Out");

            int wind_speed = getKeyByValue(indexStr, "Wind Speed");
            int wind_dir = getKeyByValue(indexStr, "Wind Dir");
            int rain = getKeyByValue(indexStr, "Rain");

            //parse data
            yc = oracle.openConnection();
            br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

            List<String> tmp = new ArrayList<String>();
            String ch;

            do {
                ch = br.readLine();
                tmp.add(ch);
            } while (ch != null);

            int end = tmp.size() - 8;

            for(int i=tmp.size()-2;i>=end;i--) {
                DateFormat format = new SimpleDateFormat("dd/MM/yy");
                Date date = format.parse(tmp.get(i).trim().split("\\s+")[day - 1]);

                //insert to db
                try {
                    String tempout = tmp.get(i).trim().split("\\s+")[tempOut - 1];
                    String windspeed = tmp.get(i).trim().split("\\s+")[wind_speed - 1];
                    String rainvalue = tmp.get(i).trim().split("\\s+")[rain - 1];

                    if(tempout.contains("---")){ tempout = "-100"; }
                    if (windspeed.contains("----")){ windspeed = "-100";}
                    if(rainvalue.contains("----")){ rainvalue = "100";}

                    String sql = "INSERT INTO _meteo_last2days VALUES ('" + date + "', '" + tmp.get(i).trim().split("\\s+")[time - 1] + "', '" + tempout + "', '" + windspeed + "', '" + tmp.get(i).trim().split("\\s+")[wind_dir - 1] + "', '" + rainvalue + "', '" + station + "')";
                    stmt.execute(sql);
                    con.commit();

                }catch(Exception e){
                    System.err.println(e.getMessage());
                }
            }

            br.close();
        }

        try {
            stmt.close();
            con.close();

            System.out.println("\n--- data updated ---");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public static String reverse(String str){
        StringBuffer buffer = new StringBuffer(str);
        buffer.reverse();

        return buffer.toString();
    }

    public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Map.Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }
}